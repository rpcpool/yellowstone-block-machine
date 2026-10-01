# AGENTS.md

Guidance for agents and contributors changing this repo. Read this before you touch
`state_machine.rs` or `forks.rs`: most bugs fixed here came from code that looked like it could be
simplified but was protecting a rule this file lists.

## Layout

| Path | What it is |
|---|---|
| `crates/yellowstone-block-machine/src/state_machine.rs` | `BlocksStateMachine`, the sans-IO core. Nearly all logic and unit tests live here. |
| `crates/yellowstone-block-machine/src/forks.rs` | `Forks<T>`: a slot-level fork graph (parent edges, rooting, fork detection). |
| `crates/yellowstone-block-machine/src/wrapper.rs` | `BlocksStateMachineWrapper`: maps wire-agnostic `event.rs` views to state-machine events and runs `gc` every 10 completed slots. |
| `crates/yellowstone-block-machine/src/stream.rs` | `BlockStream` + the `BlockAccumulator` trait. Turns state-machine outputs into consumer outputs and prunes the accumulator. |
| `crates/yellowstone-block-machine/src/dragonsmouth/` | Yellowstone gRPC adapter (`dragonsmouth-thin` / `dragonsmouth` features) and a simulator. |
| `crates/scenario-tests/` | End-to-end scenarios that drive `BlockStream` the way an external consumer does. |
| `AUDIT.md` | Numbered findings from the 4.3 / bank_id audit. Code comments cite them as "AUDIT.md finding N". |
| `docs/` | Incident write-ups (e.g. `skipped-slot-bank-leak.md`). |

## Commands

```sh
cargo test --workspace --all-features
cargo clippy --workspace --all-features --all-targets
cargo fmt --all
```

`cargo fmt` prints warnings about `imports_granularity` / `group_imports` on the stable toolchain.
They are expected and harmless.

## Core model

- **Buffers are keyed by `bank_id`, not `Slot`.** One slot can have several bank instances
  (dump-and-repair replay, duplicate blocks). Never key new per-block state by `Slot`.
- **Each slot has at most one resolved (winning) bank.** Resolution comes from a
  Confirmed/Finalized status naming the bank, or from `try_infer_sole_candidate_winner` when the
  slot has exactly one known bank.
- **`Forks` tracks slots, not banks.** It records one parent per slot and is fed only through
  `set_resolved_bank` / `register_resolved_parent`.

## Invariants: do not break these

1. **Only the resolved bank's parent may enter `Forks`.** Competing banks for the same slot can
   claim different parents. Feeding an unresolved bank's `parent_slot` corrupts the graph.
2. **Never resolve at `CreatedBank` time**, even with a single bank. A second bank's
   `CreatedBank` may still be in flight. Resolve lazily: at freeze, or at a commitment update.
3. **Processed never supersedes or discards.** Several banks of one slot can be Processed as
   peers. Only Confirmed/Finalized names the canonical bank and discards the others, and once a
   slot is Confirmed/Finalized its resolution can never change.
4. **Use `reparent_with_rooted_trace` for resolved parents**, not `add_slot_with_parent*`. A
   Processed-only resolution can be superseded, and a plain add leaves a stale edge behind.
5. **`discarded_bank_ids` is permanent until `gc` purges the slot.** Every bank-scoped handler
   checks it first and returns `Err(UntrackedSlot)`. `stream.rs` uses that `Err` to skip storing
   the raw event, so returning `Ok` for a discarded bank makes the accumulator keep a buffer
   nothing will ever prune.
6. **Skipped-slot cleanup trusts only Confirmed/Finalized.** A Confirmed/Finalized slot with
   parent `p` proves every slot in `(p, slot)` is skipped (`mark_skipped_slots_below`). A
   Processed resolution must never drive this. Walk only slots already tracked; a gap over unseen
   slots must stay a no-op that allocates nothing.
7. **Dead and skipped are different outputs.** `DeadSlotDetected` is only for a slot the wire
   declared dead (replay failed). A skipped slot reports as `ForksDetected`. Each uses its own
   bank_id snapshot (`dead_slot_bank_ids_snapshot` / `skipped_slot_bank_ids_snapshot`), because the
   teardown wipes `slot_to_banks` before the end-of-tick flush reads it.
8. **Do not add orphan nodes to `Forks`.** `pop_oldest_rooted_slot` only reclaims nodes reachable
   from a root. Call `Forks::mark_slot_as_forked` only on a slot that is already a node
   (`Forks::contains`). Otherwise record it in `forks_history` alone so `gc` pass 1 can purge it.
9. **Optimistic freeze matches the parent bank by content.** It freezes a still-buffering parent
   bank only if that bank's last entry hash equals the child's `parent_blockhash`. Never freeze
   "every buffering bank" at the parent slot.
10. **`FrozenBlock::entries` is sorted by `entry_index`.** The buffer is a hash map, so sort
    explicitly.

## How state is released

Downstream consumers hold per-bank data and cannot tell on their own that a bank is finished.
Every way a bank can end must reach one of these prune signals. If you add a new way, wire it to
one of them.

| How a bank ends | State machine cleanup | Consumer is told via |
|---|---|---|
| Slot Finalized | `deregister_finalized_slot_schedule`, after the output is popped | its commitment updates |
| Loses its slot to a sibling bank | `discard_losing_banks` | `BankDiscarded` + DLQ `Discarded` |
| Slot dead (`SlotDead` / `dead_error`) | `mark_slot_as_dead` | `DeadSlotDetected { bank_ids }` |
| Slot skipped by a Confirmed/Finalized descendant | `mark_slot_as_skipped` | `ForksDetected { bank_ids }` + DLQ `Discarded` |
| Optimistic freeze impossible | `execute_optimistic_freeze_for_needed_banks` | DLQ `Incomplete` |
| Slot forked in the graph | `gc` pass 1, once below the oldest rooted slot | `ForksDetected { bank_ids }`, then the `gc` trace |
| Anything else stuck | `gc` pass 2, after `MAX_UNRESOLVED_SLOT_AGE` (300s) | the `gc` trace |

Pass 2 is a last-resort memory bound, not a cleanup path. When a bank waits 300s for it, look for
the wire signal that should have ended it sooner.

The geyser interface has no "fork abandoned" or "bank pruned" notification. Do not treat any
other status as Dead to fake one.

## Writing tests

- Unit tests go in `mod tests` at the bottom of `state_machine.rs`. Reuse its helpers (`buffer`,
  `seal`, `seal_at`, `seal_bank`, `commitment`, `created_bank`, `dead`, `drain`, `fork_reports`,
  `sorted_fork_reports`, `dlq_discarded`).
- Simulate the passage of time with `gc_with_now(None, Instant::now() + d)`. Never sleep.
- Give each test a `///` doc comment that names the finding or doc it covers and includes an ASCII
  diagram of the slot lineage in a ```` ```text ```` block (see the skipped-slot tests).
- A regression test must fail without its fix. Check this by temporarily disabling the fix.
- Flush order comes from hash sets and is not deterministic. Sort outputs before comparing them.
- Use `crates/scenario-tests` for behaviour that only shows up through `BlockStream`.

## Style

- Doc comments use the `///` / text / `///` framing already used in `state_machine.rs`. Explain
  *why* a guard exists and cite the AUDIT.md finding or doc that motivated it.
- Errors use `thiserror`, not `anyhow`.
- Log events the cluster should never produce with `tracing::error!("UNEXPECTED: ...")`, and
  refuse to act on them rather than panicking.
- Changing a public output enum (`BlockStateMachineOutput`, `BlockMachineOutput`,
  `BlockStreamEvent`) breaks consumers. Prefer reusing an existing variant whose documented
  meaning fits.
