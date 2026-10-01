# Skipped slots leak banks for 300s

**Status:** fixed (unreleased, after 0.10.1) -- see `mark_skipped_slots_below` / `mark_slot_as_skipped` in `state_machine.rs`. Only a `Confirmed`/`Finalized` resolution marks a gap skipped; a `Processed` sole-candidate inference never does. Not covered: block data for a skipped slot that first arrives *after* the descendant confirmed still falls back to the 300s age sweep.

**Affected version:** 0.10.1. All line numbers below are from that published version's `src/`.

## The defect in one paragraph

When the cluster skips a slot, this state machine is never told. It holds that slot's bank — and
every consumer keyed on `bank_id` holds whatever it buffered for that bank — until `gc`'s
age-based sweep ages it out after `MAX_UNRESOLVED_SLOT_AGE` (300s, `state_machine.rs:142`). Every
prompt cleanup path misses it, because each one keys off a signal a skipped slot never produces.
The information needed to clean it up *is* on the wire and arrives within a slot or two: the
`parent_slot` of the next slot that gets Confirmed/Finalized.

## Reproducer

Observed in production on mainnet, 2026-10-01:

- The cluster confirms up to slot 452222015.
- Slots 452222016–452222021 are **skipped** on-chain. The chain continues at 452222022, whose
  commitment status carries `parent_slot = 452222015`.
- One node's validator had received block data for 452222016 and streamed it through geyser:
  entries only, no `BlockMeta`, so the bank never froze and no commitment status was ever emitted
  for the slot.
- That bank was still held 300s later, when the age sweep finally logged
  `Slot 452222016 aged out after 301.9s without ever reaching Finalized -- evicting stale index state`
  (`state_machine.rs:1374`).

Note the asymmetry that makes this easy to miss in the field: only a node that actually *received
block data* for the skipped slot is affected. Peers that saw nothing but slot statuses never
created a bank and leak nothing, so the same incident looks completely clean on most of a fleet.

## Why this hurts consumers, not just this crate

A consumer keyed on `bank_id` cannot learn on its own that a bank will never finish — it is told
when a bank freezes, when a slot dies, and when a bank loses its slot, and a skipped slot
triggers none of those. So any per-bank resource it holds is held for the full 300s:

- buffered account/transaction/entry payloads for a block that will never be read;
- in a consumer that spawns a worker per bank and ends it when the bank's stream ends, that worker
  runs for 300s against a stream that will never produce another event. If such workers are pooled,
  enough skipped slots in one window silently consume the pool.

300s is also an awkward number to design around: it is an order of magnitude longer than the
timeouts a consumer is likely to have for its own liveness (tens of seconds), so "wait for the
state machine to tell me" is not a usable strategy, and consumers end up inventing their own
guesses instead.

## Why every prompt cleanup path misses

### `Dead` — wrong signal, not a bug

`SlotLifecycle::Dead` → `mark_slot_as_dead` (`state_machine.rs:762`, `:903`) does discard every
bank for the slot promptly, and it is reached from `SlotStatusKind::Dead` or from a commitment
status carrying `dead_error` (`wrapper.rs:109-117`). But Solana emits `SlotStatus::SlotDead` when
**replay fails** — the validator pulled the block in and rejected it. A fork that merely loses the
vote race was never rejected; the validator just stopped extending it. The geyser plugin interface
has no notification for "bank pruned" / "fork abandoned": `notify_slot_status` covers
FirstShredReceived, Completed, CreatedBank, Processed, Confirmed, Rooted and Dead, and nothing
else.

**Do not try to fix this by treating some other status as Dead.** There is no such wire event.

### `ForksDetected` / `BankDiscarded` — the slot is not in the fork graph

A slot enters `Forks` only through `set_resolved_bank` → `register_resolved_parent`
(`state_machine.rs:641`, `:649`). A slot only *resolves* via:

- a Confirmed/Finalized commitment status naming a bank, or
- `try_infer_sole_candidate_winner` (`state_machine.rs:626`) off a **Processed** status, when the
  slot has exactly one candidate bank.

A skipped slot whose block never froze gets neither — no commitment status of any level is ever
emitted for it. So it never resolves, never enters the graph, and when 452222022 is rooted the
pruning walk in `make_slot_rooted_with_rooted_trace` has no node for 452222016 to reach.
`discard_losing_banks` (`state_machine.rs:682`) is also inapplicable: it handles *same-slot* bank
competition, and here there is exactly one bank for the slot — it just isn't on the winning chain.

### `gc` pass 1 — iterates the fork graph, same blindness

Pass 1 (`state_machine.rs:1315-1346`) iterates `forks_history`, so a slot that never entered the
graph is invisible to it no matter how old. Pass 2 (`:1352-1380`) is the age sweep, and its own
doc comment already names this class of hole — it was added for a neighbouring one (a *resolved*
slot that never receives its `Finalized`). A skipped slot is a second instance of the same
problem, and 300s is the wrong latency for it: it is knowable in well under a second.

## The signal that exists and is unused

When a slot resolves with a known parent, **every slot strictly between parent and slot is skipped
on-chain**, definitively and permanently. `SlotUpdateEvInfo.parent` (`event.rs:31`) carries it,
and `bank_parent_slot` already records it per bank.

In the reproducer: 452222022 resolving with `parent_slot = 452222015` is an authoritative
statement that 452222016–452222021 will never finish — available roughly 400ms after the fork
resolves instead of 300s.

## Suggested fix

In `register_resolved_parent` (`state_machine.rs:649`), or immediately after it in
`set_resolved_bank`, walk `(parent+1)..slot` and mark each slot skipped:

- For each skipped slot the machine knows anything about (`slot_to_banks`, `block_buffer_map`,
  `slot_first_seen_at`), discard its banks on the paths that already exist. Reusing
  `Forks::mark_slot_as_forked` (`forks.rs:510`) with the `LongShortForksMutationTracer` feeds
  `forks_detected_in_current_tick`, so the existing `flush_forks_detected_in_current_tick`
  (`state_machine.rs:1020`) emits one `ForksDetected { slot, bank_ids }` per skipped slot with no
  new output variant and no new consumer work. Adding a dedicated output variant instead is a
  breaking change for consumers — weigh that against how much clearer "skipped" is than "forked"
  at the API boundary, since a skipped slot is not in fact a fork.
- The DLQ side must stay consistent: `ForksDetected` alone does not push
  `DeadletterEvent::Discarded`. Check whether consumers keyed purely on the DLQ (rather than on
  the output stream) need it, and mirror whatever `discard_losing_banks` does.
- A skipped slot the machine has never heard of must be cheap: do not auto-vivify state for it.
  Gaps are routine — most leader rotations skip slots — so this walk runs constantly and must be
  a no-op in the common case.

### Constraints and traps

- **Bound the walk.** `slot - parent` is normally small but is attacker-/bug-influenced and can be
  large after a long outage. Cap it, and prefer iterating known slots in the range over iterating
  the range itself.
- **Only resolve-time parents are trustworthy.** The doc comment on `register_resolved_parent`
  explains why only the *resolved* bank's parent claim may be fed to `Forks`; an unresolved
  candidate's `parent_slot` must not drive this. A resolution can also later be superseded
  (`reparent_with_rooted_trace`), so a slot marked skipped under an earlier parent claim must not
  come back wrong — decide explicitly whether skipped-marking is reversible, and test the
  dump-and-repair replay case that `state_machine.rs:806-811` describes.
- **`discarded_bank_ids` is permanent.** Once a bank lands there, straggler events for it are
  rejected (`:728-740`, `:769-776`). Marking a slot skipped is a strong claim; make sure a late
  Confirmed for it cannot arrive. (By construction of Solana's fork choice it cannot — but assert
  it rather than assume it, and decide what to do if it does.)
- **Don't disturb the dead-slot path.** `dead_slot_bank_ids_snapshot` (`:537`) relies on being
  populated only by `mark_slot_as_dead` for the slot the wire itself declared dead; a skipped slot
  flowing through the same flush must take the generic `ForksDetected` branch, not
  `DeadSlotDetected`.
- **`retroactively_rooted_slots`** interacts with rooting traces; make sure marking skipped slots
  does not strand entries there.

### Tests worth writing

In `state_machine.rs`'s test module, alongside the existing `gc` tests (which already simulate the
age sweep with `gc_with_now` and a far-future `Instant` — see `:2474-2477`):

1. The reproducer: feed block data for slot N (entries only, never a `BlockMeta`), then resolve
   slot N+6 with `parent_slot = N-1`; assert N's bank is discarded **without** advancing the clock
   by `MAX_UNRESOLVED_SLOT_AGE`, and that an output names it.
2. A gap over slots the machine has never seen produces no output and allocates no state.
3. A skipped slot that *did* have multiple candidate banks discards all of them.
4. The existing age-sweep test still passes — pass 2 must remain for the unresolved-but-not-skipped
   case it was built for.
5. Supersession: a resolution that later reparents does not leave a wrongly-skipped slot behind.
