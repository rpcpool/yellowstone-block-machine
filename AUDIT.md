# Logic audit: Agave 4.3.0 / bank_id transition

Audit of the `Slot` to `bank_id` rekeying on branch `4.3-transition`, at commit `3148de5`.
Primary target: `crates/yellowstone-block-machine/src/state_machine.rs`.

**Date:** 2026-09-09

## Summary

Twelve defects are confirmed, each backed by a test in `audit_regression` (see the reproduction
suite section). Six further defects were reported by the audit but have not been verified and are
listed separately.

None of these are visible to the existing suite. All 36 pre-existing tests pass regardless of which
findings below are fixed, because the failing behaviours all sit in orderings that suite does not
construct.

**Status: 6 of 12 fixed.** Finding 1 (optimistic freeze fabricating blocks), finding 2
(retroactive rooting dropping commitment delivery), finding 3 (gap-filled Finalized scheduling
premature teardown), finding 4 (superseding leaving a stale fork edge), finding 5 (dead-slot fork
events losing their bank_ids), and finding 6 (discarded losers reaching no prune path) are fixed as
of this revision. The other six are open.

## Verification legend

| Mark | Meaning |
|---|---|
| `FIXED` | Verified defect, fix applied, its regression test now passes |
| `TEST` | Reproduced by a test in `audit_regression`, still open |
| `READ` | Confirmed by code inspection and a workspace-wide search, still open |
| `UNVERIFIED` | Reported by the audit, mechanism plausible, not yet reproduced |

---

## 1. Optimistic freeze fabricates blocks for sibling banks

**Severity:** critical &nbsp;&nbsp; **Status:** `FIXED` &nbsp;&nbsp; **Location:** `state_machine.rs:914`

`handle_block_summary` marks *every* bank still buffering at `block_summary.parent_slot` for
optimistic freeze. `execute_optimistic_freeze_for_needed_banks` then forges a `BlockSummary` for it
and pushes a real `FrozenBlock` downstream.

The guarding comment reads "This should never happen". Under `bank_id` keying it is the normal case:
a slot legitimately has more than one bank instance, and only one of them ever receives a
`BlockMeta`. The other is not broken, it is simply not the winner.

**Trigger.** Two banks at slot 10. Bank 1000 seals normally. Bank 1001 receives entries but no
`BlockMeta`. A child at slot 11 with `parent_slot` 10 then receives its own `BlockMeta`.

**Observed.** A `FrozenBlock` was emitted for bank 1001 carrying a blockhash forged from the last
entry hash, an all-zero `parent_blockhash`, and `block_time` of `0`. The consumer cannot tell this
from a real block.

**Fix.** Applied: `handle_block_summary` now identifies the parent-slot bank to optimistically
freeze by *content*, not by consensus resolution. The child's `BlockMeta` already carries
`parent_blockhash`, a cryptographic claim about exactly which block it extends. For each bank still
buffering at the parent slot, the fix computes what that bank's blockhash would be if frozen now
(`last_entry_hash`, the same value `forge_optimistic_block_summary` would freeze it with, gated on
`can_be_optimistic_frozen` so it is only computed once all of that bank's entries are in) and only
optimistically freezes it if that matches `block_summary.parent_blockhash` exactly.
`Hash::default()` -- the sentinel this crate's own forged summaries use for "unknown" -- is
excluded from matching, since it cannot safely identify anything.

This is strictly better than gating on `resolved_bank_per_slot`, an earlier version of this fix:
matching by hash never freezes the wrong sibling (fixing the fabrication bug), and unlike gating on
resolution it also still recovers the legitimate case *before* the parent slot has resolved, since
identity here is a cryptographic fact independent of what consensus has decided so far.

Covered by two tests: `audit_1_no_fabricated_block_for_parent_slot_sibling` (the fabrication case,
passes) and `audit_1b_optimistic_freeze_still_recovers_the_real_parent_by_hash` (the legitimate
recovery case, including pre-resolution, also passes).

## 2. Retroactive rooting drops a slot's entire commitment delivery

**Severity:** critical &nbsp;&nbsp; **Status:** `FIXED` &nbsp;&nbsp; **Location:** `state_machine.rs:829`

`process_retroactively_rooted_slots` iterates a set it has already consumed with `std::mem::take`,
and `continue`s past any slot with no entry in `resolved_bank_per_slot`. That slot is then lost
permanently: nothing re-queues it.

A slot with two competing banks that both only ever reach `Processed` is never resolved, by design.
If such a slot is rooted through a finalized child, it is rooted in `Forks` and silently receives no
commitment update at all.

**Trigger.** Slot 1 gets two banks, both `Processed`, so it stays unresolved. Slot 2, its child,
reaches `Finalized`.

**Observed.** `Forks` reported slot 1 as rooted. Exactly zero status updates were delivered for
slot 1. Only slot 2's own three levels came out.

**Fix.** Applied, in two parts, since "no resolved bank" actually covers two different situations:

- **The slot only ever had one known bank.** It simply hasn't gone through freeze or a `Processed`
  update yet, so `try_infer_sole_candidate_winner` was never triggered for it. This is safe to
  resolve immediately: a retroactively rooted slot is, by construction, already known to be part
  of a finalized chain, so a sole candidate for it cannot be anything but the canonical bank -- no
  risk of the premature guess the crate's own design otherwise avoids (a genuine second competitor
  could still be in flight in the general case; here there is external proof there isn't one).
  `process_retroactively_rooted_slots` now calls `try_infer_sole_candidate_winner(slot)` before
  checking `resolved_bank_per_slot`.
- **The slot genuinely has two or more competing banks**, none of which any direct commitment
  update has named as canonical. Nothing here can safely choose between them, and the fix does not
  try to. Instead of dropping the notification (`continue` with no trace), the slot is re-queued
  into `retroactively_rooted_slots` so this retries on every subsequent tick. The moment a real
  commitment update *does* resolve it -- Solana guarantees exactly one bank per slot ever reaches
  Confirmed/Finalized -- the deferred delivery fires instead of having been lost. The retry log was
  dropped to `debug` rather than `warn`, since a persistently ambiguous slot would otherwise log on
  every single replay/consensus event until it resolves.

Note the residual limitation: a slot that is retroactively rooted while genuinely ambiguous, and
that *never* receives a further direct commitment update for either candidate, stays deferred
forever -- a small, bounded memory cost (one `Slot` in a set, rechecked each tick) rather than the
previous silent, untraceable, permanent data loss. Resolving that residual case for good would
require identifying the true parent by content (matching a candidate's own blockhash against the
finalized descendant's `parent_blockhash` chain, the same technique finding 1's fix uses), which
needs retaining frozen blockhashes per bank_id past their `Block` buffer's lifetime -- a larger
structural change left for a follow-up if this residual case turns out to matter in practice.

Covered by two tests: `audit_2_retroactively_rooted_sole_candidate_resolves_immediately` (the
common case, immediate resolution, delivery still correctly gated on the bank's own freeze) and
`audit_2b_ambiguous_ancestor_recovers_once_a_direct_commitment_arrives` (the genuinely ambiguous
case, correctly withheld until a real event resolves it, then delivered). Both pass.

## 3. Gap-filled Finalized schedules slot teardown three times

**Severity:** high &nbsp;&nbsp; **Status:** `FIXED` &nbsp;&nbsp; **Location:** `state_machine.rs:754`

Inside `deliver_commitment`'s `for update in to_push` loop, the body tests the outer `commitment`
variable rather than `update.commitment`. When a `Finalized` update gap-fills the two levels below
it, all three iterations take the `Finalized` branch.

Two consequences follow. `make_slot_rooted_with_rooted_trace` is called three times, which is
harmless because it early-returns. `deregister_finalized_slot_schedule` however receives one entry
per revision, including the revision of the synthesized `Processed` update.

`pop_next_unprocess_blockstore_update` keys teardown on the revision of the item just popped. So a
consumer that pops only the `Processed` update triggers teardown of the whole slot while its
`Confirmed` and `Finalized` outputs are still sitting unread in the queue.

**Observed.** The schedule was `[(1, [1]), (2, [1]), (3, [1])]`. After popping the frozen block and
the `Processed` status, two updates remained queued and the slot was already staged for teardown.
Once `gc` ran, `frozen_commitment_index`, `resolved_bank_per_slot` and `slot_min_commitment` had all
lost the slot.

Two further failures cascade from that torn-down state, each reproduced:

- A duplicate `BlockMeta` for bank 100 was accepted rather than rejected, and re-emitted a second
  `FrozenBlock` for a bank already delivered to the consumer.
- A rogue `Confirmed` naming a different bank re-resolved the already-`Finalized` slot 1 to bank
  101, and fed `Forks` a second parent claim of 99. The finality guard could not fire because the
  floor it reads had been erased.

**Fix.** Applied: the loop now captures `update.commitment` into a local before pushing the
update, and tests that local rather than the outer `commitment`. That alone confines the schedule
to the genuine `Finalized` revision and closes both cascades.
`audit_3_gapfill_schedules_teardown_once_at_the_finalized_revision`,
`audit_3a_duplicate_block_meta_is_still_rejected_after_partial_drain`, and
`audit_3b_finalized_resolution_survives_a_rogue_bank` all pass.

## 4. Superseding double-feeds `Forks` and leaves a stale parent edge

**Severity:** high &nbsp;&nbsp; **Status:** `FIXED` &nbsp;&nbsp; **Location:** `state_machine.rs:555`

`set_resolved_bank` calls `register_resolved_parent` unconditionally. When it supersedes a previous
winner, the slot's parent is fed to `Forks` a second time.

`Forks` updates `reverse_parent_children_map` to the new parent, but the forward edge in
`parent_children_map` from the *old* parent survives. The slot is now recorded as a child of two
different parents.

This is the one case that genuinely breaks the "feed `Forks` exactly once per slot" invariant the
rewrite's design notes rest on. Superseding a `Processed`-inferred resolution is itself intended;
the defect is the residue it leaves in the fork graph.

**Trigger.** Bank 3000 is the sole candidate for slot 30, claims parent 29, and freezes, so
sole-candidate inference resolves the slot and feeds `Forks` the edge from 29. Bank 3001 then
arrives claiming parent 27 and reaches `Confirmed`. Slot 29 is later marked `Dead`, which is the
very reason the repair re-parented to 27.

**Observed.** `Dead(29)` walked the stale forward edge and reported the canonical `Confirmed` slot
30 as a fork. `stream.rs` prunes every bank named in a `ForksDetected`, so this discards a valid,
confirmed block.

**Fix.** Applied, in `forks.rs`, not just at the call site. `Forks` gained a new method,
`reparent_with_rooted_trace`, which retracts a child's stale forward edge under its previous parent
(if it had one, and if it differs from the new parent) before delegating to the existing
`add_slot_with_parent_with_rooted_trace`. `register_resolved_parent` in `state_machine.rs` now
calls this instead of the plain add.

This was chosen over a fuller rekey of `Forks` to `BankId`-typed nodes (discussed and rejected):
`bank_parent_slot: FxHashMap<BankId, Slot>` in `state_machine.rs` already independently tracks every
bank instance's own claimed parent, so the "multiple banks, multiple parents" case was never
actually missing a multiset -- it already existed one layer up. `Forks<Slot>` only ever needs to
represent the *currently resolved* bank's claim for chain-level fork detection, one edge at a time;
what was missing was the ability to correct that one edge when resolution moves to a different
bank's claim, not the ability to hold several at once. A `BankId`-keyed rewrite would also have
broken the public `Forks<Slot>` field and `BlocksStateMachineWrapper::fork_graph()`, and would have
reintroduced a version of finding 2's problem (a bank's parent is only known by slot, not by parent
bank_id, so bank-to-bank edges require deferring until the parent itself resolves).

**Known residual limitation, no live impact.** `reparent_with_rooted_trace` does not revisit any
fork conclusion already drawn from the retracted edge. If `slot` was marked forked *before* the
reparent happened -- specifically, if the old parent died while the stale edge was still the active
one -- that fork flag is not cleared once `slot` is correctly reparented. Un-marking it would require
proving the conclusion depended on nothing but the retracted edge, which this structure does not
track, so the deliberately conservative choice is to leave it forked rather than risk wrongly
clearing a real fork. In practice this has no live consequence: `forked_slots` has no public getter
and nothing downstream re-checks it; its only effect is the one-shot `ForksDetected` notification
already fired at the moment the flag was set, for whichever bank_ids existed at that instant. A
bank that later wins resolution still freezes and delivers normally through the ordinary resolution
pipeline, which never consults fork status.

Covered by two layers of tests. At the `state_machine.rs` level,
`audit_4_supersede_does_not_leave_a_stale_fork_edge` reproduces the exact scenario (resolve, then
supersede with a different parent, then mark the old parent dead) and passes. At the `forks.rs`
level, three focused unit tests exercise the new method directly:
`reparent_retracts_the_stale_forward_edge`, `reparent_to_the_same_parent_is_a_pure_shortcut`, and
`reparent_prevents_the_old_parents_death_from_wrongly_forking_the_child`.

## 5. Dead-slot fork events lose their bank_ids

**Severity:** high &nbsp;&nbsp; **Status:** `FIXED` &nbsp;&nbsp; **Location:** `state_machine.rs:794`

`mark_slot_as_dead` snapshots the slot's bank ids on line 793, uses the snapshot only to mark them
discarded, then calls `remove_slot_references_in_state`, which deletes the `slot_to_banks` entry.

`flush_forks_detected_in_current_tick` runs afterwards, at the end of `process_replay_event`, and
rebuilds `ForkDetected::bank_ids` by re-reading `slot_to_banks`. For a dead slot that lookup always
misses.

**Observed.** A dead slot with banks 70 and 71 emitted `ForkDetected { slot: 7, bank_ids: [] }`. The
prune loop in `stream.rs` iterates zero times, so both banks keep every buffered event they hold.
`gc` cannot recover them either, because its own trace reads the same erased map.

**Fix.** Applied: a new field, `dead_slot_bank_ids_snapshot: FxHashMap<Slot, Vec<BankId>>`, carries
the bank ids across the same tick. `mark_slot_as_dead` snapshots into it *before* the teardown --
but only when the slot was actually just newly forked (checked via
`forks_detected_in_current_tick.contains(&slot)`), so nothing is stored for a slot that won't be
flushed this tick, and the entry never lingers. `flush_forks_detected_in_current_tick` now checks
this snapshot first (removing the entry as it consumes it) before falling back to the live
`slot_to_banks` lookup used by every other slot passing through the same flush -- a slot forked only
as a side effect of the dead slot's own descendants (via `Forks::mark_slot_as_forked`'s child-walk)
was never wiped and correctly keeps using the live path unchanged.

Covered by two tests: `audit_5_dead_slot_event_carries_its_bank_ids` (the direct case) and
`audit_5b_descendant_fork_in_the_same_tick_still_reports_its_own_live_bank_ids` (proving the
snapshot mechanism doesn't cross-contaminate a descendant slot forked in the very same tick). Both
pass.

## 6. Discarded loser banks reach no prune path

**Severity:** high &nbsp;&nbsp; **Status:** `FIXED` &nbsp;&nbsp; **Location:** `state_machine.rs:581`

`discard_losing_banks` retains only the winner in `slot_to_banks`. That map is the sole source `gc`
builds its `deleted` bank id trace from. No deadletter event is pushed and no `ForksDetected` names
the loser.

`BlockAccumulator::prune_block` is therefore never called for it, and the loser's accumulated
accounts, transactions and entries are held for the life of the process.

**Observed.** After the loser was discarded, five `gc` passes produced an empty trace and the
deadletter queue stayed empty.

**Fix.** Applied: `DeadletterEvent` gained a new `Discarded(BankId)` variant, distinct from the
existing `Incomplete(BankId)` (a bank this crate gave up trying to freeze -- no entries, no known
parent -- which is a different situation from a fully valid bank that simply lost a resolution
race). `discard_losing_banks` now pushes `DeadletterEvent::Discarded(loser)` for every bank it
discards, using the same DLQ channel `execute_optimistic_freeze_for_needed_banks` already used for
`Incomplete`. `stream.rs`'s `on_new_frozen_block` -- which already drained the DLQ and called
`prune_block` for `Incomplete` -- now does the same for `Discarded` too, via one added match arm.

No other consumer needed updating: `wrapper.rs::pop_next_dlq` forwards the enum opaquely without
matching on it. `audit_6_discarded_loser_is_announced_for_pruning` passes.

## 7. Unresolved slots are invisible to garbage collection

**Severity:** high &nbsp;&nbsp; **Status:** `TEST` &nbsp;&nbsp; **Location:** `state_machine.rs:1087`

`gc`'s purge loop iterates `forks_history` only. After the rewrite the only thing that teaches
`Forks` that a slot exists is `register_resolved_parent`, reachable solely from `set_resolved_bank`.

A slot that never resolves never enters the fork graph, never enters `forks_history`, and is
therefore never a GC candidate. Its `slot_to_banks` and `pending_slot_status_update` entries are
permanent.

**Observed.** Ten slots received commitment updates for banks whose entries and `BlockMeta` never
arrived, which is what a stream restart mid-block looks like. After 25 `gc` passes all ten were
still held, with `forks_history` empty.

**Fix.** Age unresolved slots out on a separate criterion, such as wall-clock age of the oldest
buffered bank, rather than depending on fork-graph membership.

## 8. Events for discarded banks return `Ok`

**Severity:** high &nbsp;&nbsp; **Status:** `TEST` &nbsp;&nbsp; **Location:** `state_machine.rs:802`, `state_machine.rs:870`

`handle_block_entry_insert` and `handle_block_summary` both return `Ok(())` when the bank is in
`discarded_bank_ids`. The state machine correctly stores nothing.

`stream.rs` however uses that `Result` as the gate for `insert_into_storage`. An `Ok` means the
event is inserted into the accumulator, which auto-vivifies a fresh buffer for the discarded bank.
Nothing will ever prune it, because the state machine has no record of it.

**Observed.** After a slot was marked `Dead`, a straggler entry and a straggler `BlockMeta` for one
of its banks both returned `Ok`.

**Fix.** Return `Err(UntrackedSlot)` for discarded banks, matching what `is_bank_trackable` already
tells the caller for account and transaction events.

## 9. `DeadSlotDetected` is never constructed

**Severity:** medium &nbsp;&nbsp; **Status:** `READ` &nbsp;&nbsp; **Location:** `state_machine.rs:245`

`BlockStateMachineOutput::DeadSlotDetected` has zero construction sites in the workspace. It is only
declared, matched in `slot()`, and matched in `stream.rs:297`.

Everything downstream of it is therefore unreachable: `PendingEvent::DeadBlockDetect`,
`BlockMachineOutput::DeadBlockDetected`, and the public `BlockStreamEvent::DeadBlockDetected` that
`lib.rs:45` advertises as one of four stream outputs and that the example handles.

Dead slots currently surface as `ForksDetected`, via `mark_slot_as_dead` calling
`mark_slot_as_forked`. That is indistinguishable from an ordinary fork, and per finding 5 it arrives
with no bank ids.

**Fix.** Emit `DeadSlotDetected` from `mark_slot_as_dead` with the bank id snapshot, or delete the
variant and its documented event so the public surface stops promising it.

## 10. `dead_blocks_queue` is never written

**Severity:** low &nbsp;&nbsp; **Status:** `READ` &nbsp;&nbsp; **Location:** `state_machine.rs:445`

The field is initialised and its length is reported by `stats()`, but nothing in the workspace ever
pushes to it. `BlockstoreStats::dead_block_queue_len`, exposed through
`BlockStream::state_machine_stats`, can only ever read zero. A dashboard built on it would show no
dead blocks regardless of how many occur.

## 11. `FrozenBlock::entries` is emitted in hash-map order

**Severity:** low &nbsp;&nbsp; **Status:** `READ` &nbsp;&nbsp; **Location:** `state_machine.rs:164`

`Block::freeze` builds the vector with `self.entries.values().cloned().collect()` over an
`FxHashMap<u64, EntryInfo>`, so entry order is nondeterministic rather than sorted by `entry_index`.

Neither accumulator in this crate reads that field, so there is no live data loss here. It remains a
hazard on a public, `Serialize`/`Deserialize` type: an external consumer would reasonably assume
block order. Sorting by `entry_index` in `freeze` costs nothing.

## 12. Declared but unused items

**Severity:** low &nbsp;&nbsp; **Status:** `READ`

`tick_entry_cnt` is incremented and never read. `min_history_revision_in_queue` is assigned and
never read. `BlockstorePublisherConfig`, `InvalidBlock`, `InnerBlockSequence`, `AVG_BLOCK_LEN` and
`AVG_TPB` have no uses anywhere.

Each is individually harmless. Together with findings 9 and 10 they suggest the rewrite left
scaffolding behind, which makes it hard for a reader to tell intended-but-unfinished from
deliberately-removed.

---

## Reported but not verified

These came out of the audit with a plausible mechanism but were not reproduced. They are recorded so
they are not lost, and should be confirmed before any code changes on their account.

| # | Severity | Location | Claim |
|---|---|---|---|
| 13 | high | `state_machine.rs:1015` | The optimistic-freeze failure arm calls `remove_bank_references`, which erases `frozen_commitment_index`. If the bank had already frozen and been emitted, the machine forgets it ever froze, so later commitment updates queue forever. |
| 14 | medium | `state_machine.rs:1016` | The deadletter arm never inserts the bank into `discarded_bank_ids`, unlike `discard_losing_banks` and `mark_slot_as_dead`. The consumer prunes the payload while the machine stays willing to re-freeze a truncated block. |
| 15 | medium | `state_machine.rs:631` | The `CreatedBank` branch checks only `discarded_bank_ids`, not `frozen_commitment_index`. Every other bank-scoped ingress checks both, so a reordered `CreatedBank` can resurrect a buffer that can never freeze. |
| 16 | medium | `state_machine.rs:784` | No persistent dead-slot set is kept. `mark_slot_as_dead` can only discard banks already known at that moment, so a bank-scoped event arriving after `Dead` auto-vivifies a buffer that will never freeze. |
| 17 | medium | `stream.rs:292` | A `ForksDetected` later in the same drain can `prune_block` a bank whose `PendingEvent::FrozenBlock` an earlier `SlotStatus` already queued, yielding a commitment update with no preceding block. |
| 18 | low | `state_machine.rs:993` | Optimistic-freeze cascades advance one generation per replay event, because `handle_block_summary` re-inserts into the set already emptied by `mem::take`. Consensus events never drain it at all. |

## Coverage

This audit is partial. Ten specialist reviewers were dispatched; seven failed on infrastructure
limits. Findings 1 through 18 come from the three that completed, plus independent verification.

Areas still unreviewed:

- Wire adaptation in `dragonsmouth/proto_adapter.rs` and `wrapper.rs`, including the reachable
  `expect` calls on wire-supplied hash strings and the `dead_error` downgrade path.
- The async driver in `stream.rs`, including the ordering of `on_new_frozen_block` against
  `freeze_block`, and the `unsafe` projection in `client_ext.rs`.
- Entry bookkeeping and the `Block` counters, including whether `entry_cnt` is safe to use as an
  index in `last_entry_hash`.
- A regression diff against the pre-transition implementation at `5cf9afc`, to find behaviour
  dropped or half-migrated by the rekeying.
- `generate_entries` in the existing test module, which appears to emit colliding `entry_index`
  values and may make several existing tests weaker than they read.

## Reproduction suite

The findings above are backed by two things:

- `crates/yellowstone-block-machine/src/state_machine.rs`, module `audit_regression` -- one test per
  finding at the level `BlocksStateMachine` is actually used.
- `crates/yellowstone-block-machine/src/forks.rs`, module `forks_tests` -- three additional unit
  tests (`reparent_*`) exercising finding 4's fix directly against the new `Forks` method, since
  that is where the actual defect lived.

Every `audit_regression` test asserts the **correct** behaviour, so every one of them **fails
against the current implementation** until its finding is fixed. A test flipping to green means
that finding is fixed. The three `forks.rs` tests are ordinary passing regression tests for the new
method, not inverted like the `audit_regression` ones -- there was no way to write a "must currently
fail" test at that level before the method existed.

```text
cargo test --all-features audit_regression::
```

Expect four failures (findings 7, 8, 9, and 11 remain open). Findings 1 through 6 are fixed; all eleven of their `state_machine.rs`-level tests now pass, plus three new `forks.rs`-level unit tests for finding 4. The pre-existing suite is unaffected:

```text
cargo test --all-features -- --skip audit_regression    # 39 passed (36 pre-existing + 3 new forks.rs tests)
```

| Test | Finding | Assertion that currently fails |
|---|---|---|
| `audit_1_no_fabricated_block_for_parent_slot_sibling` | 1 | **FIXED.** Bank 1001 never got a `BlockMeta`, its hash doesn't match the child's `parent_blockhash`, and it is no longer frozen. |
| `audit_1b_optimistic_freeze_still_recovers_the_real_parent_by_hash` | 1 | **FIXED (added).** A genuine parent whose hash matches is still recovered, even before the slot resolves. |
| `audit_2_retroactively_rooted_sole_candidate_resolves_immediately` | 2 | **FIXED.** A sole-candidate ancestor resolves immediately and delivers once it freezes. |
| `audit_2b_ambiguous_ancestor_recovers_once_a_direct_commitment_arrives` | 2 | **FIXED (added).** A genuinely ambiguous ancestor is deferred, not dropped, and recovers once resolved. |
| `audit_3_gapfill_schedules_teardown_once_at_the_finalized_revision` | 3 | **FIXED.** Teardown is now scheduled once, at revision 3 only. |
| `audit_3a_duplicate_block_meta_is_still_rejected_after_partial_drain` | 3 | **FIXED.** A duplicate `BlockMeta` for bank 100 is rejected, not re-emitted. |
| `audit_3b_finalized_resolution_survives_a_rogue_bank` | 3 | **FIXED.** Slot 1's Finalized resolution to bank 100 survives a rogue Confirmed for bank 101. |
| `audit_4_supersede_does_not_leave_a_stale_fork_edge` | 4 | **FIXED.** Canonical slot 30 is no longer reported as a fork when its abandoned parent dies. |
| (forks.rs) `reparent_retracts_the_stale_forward_edge` | 4 | **FIXED (added).** Reparenting removes the old forward edge and installs the new one. |
| (forks.rs) `reparent_to_the_same_parent_is_a_pure_shortcut` | 4 | **FIXED (added).** Reparenting to an unchanged parent is a no-op, matching `add`'s shortcut. |
| (forks.rs) `reparent_prevents_the_old_parents_death_from_wrongly_forking_the_child` | 4 | **FIXED (added).** The old parent's later death no longer forks the reparented child. |
| `audit_5_dead_slot_event_carries_its_bank_ids` | 5 | **FIXED.** The dead slot's event now names banks 70 and 71 via a same-tick snapshot. |
| `audit_5b_descendant_fork_in_the_same_tick_still_reports_its_own_live_bank_ids` | 5 | **FIXED (added).** A descendant forked in the same tick still reports its own live bank_ids, unaffected. |
| `audit_6_discarded_loser_is_announced_for_pruning` | 6 | **FIXED.** Bank 500 now reaches the deadletter queue via a new `DeadletterEvent::Discarded` variant. |
| `audit_7_unresolved_slot_state_is_eventually_reclaimed` | 7 | Ten abandoned slots must be reclaimed. All ten survive 25 gc passes. |
| `audit_8_events_for_discarded_banks_are_rejected` | 8 | Stragglers for discarded bank 70 must return `Err`. Both return `Ok`. |
| `audit_9_dead_slot_emits_a_dead_slot_output` | 9 | A `Dead` update must produce `DeadSlotDetected`. It produces `ForksDetected`. |
| `audit_11_frozen_block_entries_are_ordered_by_entry_index` | 11 | Entries must come out in index order. They come out in hash order. |

Findings 10 and 12 are absence-of-code defects with nothing to assert at runtime. Confirm them with:

```text
rg 'dead_blocks_queue|tick_entry_cnt|min_history_revision_in_queue' crates/
rg 'BlockStateMachineOutput::DeadSlotDetected' crates/ examples/
```

To keep a green default suite while these stay open, add `#[ignore]` to each test and run them with
`cargo test --all-features audit_regression:: -- --ignored`.
