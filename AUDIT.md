# Logic audit: Agave 4.3.0 / bank_id transition

Audit of the `Slot` to `bank_id` rekeying on branch `4.3-transition`, at commit `3148de5`.
Primary target: `crates/yellowstone-block-machine/src/state_machine.rs`.

**Date:** 2026-09-09

## Summary

Thirteen defects are confirmed, each backed by a test (see the reproduction suite section). Six
further defects were reported by the audit but have not been verified and are listed separately.

None of these are visible to the existing suite. All 36 pre-existing tests pass regardless of which
findings below are fixed, because the failing behaviours all sit in orderings that suite does not
construct.

**Status: 11 of 13 fixed.** All findings except 10 and 12 are fixed as of this revision: finding 1
(optimistic freeze fabricating blocks), finding 2 (retroactive rooting dropping commitment
delivery), finding 3 (gap-filled Finalized scheduling premature teardown), finding 4 (superseding
leaving a stale fork edge), finding 5 (dead-slot fork events losing their bank_ids), finding 6
(discarded losers reaching no prune path), finding 7 (unresolved/never-finalized slots invisible to
garbage collection), finding 8 (events for discarded banks returning `Ok`, including a third call
site found while fixing it), finding 9 (`DeadSlotDetected` never constructed), finding 11
(`FrozenBlock::entries` in hash-map order), and finding 19 (every `Account` update misclassified as
a sysvar, found after this audit's own Coverage section flagged `proto_adapter.rs` as unreviewed).

Findings 10 and 12 were never assigned a `TEST`/`READ` status meant to be flipped to `FIXED` -- they
are dead-code observations (an unused queue, a handful of unused declarations) rather than logic
bugs, and remain exactly as documented in their own sections below unless the crate owner wants
them cleaned up separately.

The regression tests for findings 1 through 9 and 11 originally lived in a standalone
`audit_regression` module, one test per finding, each asserting the *correct* behaviour so it would
fail until its finding was fixed. Now that every one of them is fixed, they have been merged
directly into the crate's own pre-existing `mod tests` alongside the tests that were already there
-- there is no longer a reason to keep them separate, since a passing `audit_*` test is no longer
distinguishable in spirit from any other regression test in the suite. Their doc comments (each
still naming the `AUDIT.md` finding it covers) were kept intact through the merge.

## Verification legend

| Mark | Meaning |
|---|---|
| `FIXED` | Verified defect, fix applied, its regression test now passes |
| `TEST` | Reproduced by a test, not yet fixed (no finding currently carries this mark) |
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

**Severity:** high &nbsp;&nbsp; **Status:** `FIXED` &nbsp;&nbsp; **Location:** `state_machine.rs:1087`

`gc`'s purge loop iterates `forks_history` only. After the rewrite the only thing that teaches
`Forks` that a slot exists is `register_resolved_parent`, reachable solely from `set_resolved_bank`.

A slot that never resolves never enters the fork graph, never enters `forks_history`, and is
therefore never a GC candidate. Its `slot_to_banks` and `pending_slot_status_update` entries are
permanent.

**Observed.** Ten slots received commitment updates for banks whose entries and `BlockMeta` never
arrived, which is what a stream restart mid-block looks like. After 25 `gc` passes all ten were
still held, with `forks_history` empty.

Investigating further while implementing the fix turned up that the finding's own framing
("unresolved slots") was narrower than the actual bug. A single bank with no competitor resolves
immediately, by sole-candidate inference, on its very first `Processed` update -- `resolved_bank_per_slot`
gets set and `Forks` does learn its parent edge. But `forks_history` (the set `gc` actually
iterates) is only ever populated when something is *detected as forked*, never merely by being
added to the fork graph's topology. So the real defect is broader: **any slot that is never
detected as a fork and never reaches `Finalized`** is invisible to `gc`, resolved or not.

**Fix.** Applied: a new field, `slot_first_seen_at: FxHashMap<Slot, Instant>`, records when a slot
was first registered (set in `register_bank_for_slot`, the common choke point already used for
`slot_to_banks`; cleared alongside it in `remove_slot_references_in_state`). `gc` is now split into
a public `gc()` (unchanged signature, calls `Instant::now()`) and a private `gc_with_now(deleted,
now)` that does the real work, parameterized on the clock so tests can simulate the passage of time
without an actual sleep -- `Instant + Duration` arithmetic needs no waiting, mirroring the existing
`Block::new_with_clock` pattern in this same file.

`gc_with_now` runs two passes. Pass 1 is the original `forks_history`-based sweep, unchanged. Pass
2 is new: any slot in `slot_first_seen_at`, not already purged by pass 1, older than a new constant
`MAX_UNRESOLVED_SLOT_AGE` (300 seconds, generously above the tens of seconds a healthy slot normally
takes to reach `Finalized`), is evicted -- deliberately *without* either of pass 1's two safety
gates. The `oldest_rooted_slot` slot-number comparison is meaningless for a slot that may not be in
the fork graph at all. The pending-Processed gate is actively wrong here: it exists to protect a
slot that is likely about to resolve normally very soon, but a slot only reaches pass 2 after
sitting for the *entire* age threshold with zero progress, at which point a still-queued Processed
status is not "about to be delivered" -- it is exactly the leaked state this sweep exists to
reclaim. Reusing that gate unmodified, which was my first attempt, made pass 2 permanently unable
to evict the very slots `audit_7`'s scenario constructs, since every one of them has exactly such a
queued Processed status.

A late straggler event for a bank_id evicted this way restarts a fresh, small buffer rather than
being rejected outright, since it was never marked `discarded`, only aged out -- the same accepted
bounded edge case pass 1's ordinary sweep already has.

`audit_7_unresolved_slot_state_is_eventually_reclaimed` now drives this precisely: it first confirms
25 ordinary `gc()` passes, with no time elapsed, reclaim nothing (since `forks_history` stays
empty), then calls `gc_with_now` with a synthetic far-future `Instant` and confirms everything is
reclaimed. It passes.

## 8. Events for discarded banks return `Ok`

**Severity:** high &nbsp;&nbsp; **Status:** `FIXED` &nbsp;&nbsp; **Location:** `state_machine.rs:802`, `state_machine.rs:870`, and `state_machine.rs:664` (found during the fix, see below)

`handle_block_entry_insert` and `handle_block_summary` both return `Ok(())` when the bank is in
`discarded_bank_ids`. The state machine correctly stores nothing.

`stream.rs` however uses that `Result` as the gate for `insert_into_storage`. An `Ok` means the
event is inserted into the accumulator, which auto-vivifies a fresh buffer for the discarded bank.
Nothing will ever prune it, because the state machine has no record of it.

**Observed.** After a slot was marked `Dead`, a straggler entry and a straggler `BlockMeta` for one
of its banks both returned `Ok`.

**Fix.** Applied: both sites now return `Err(UntrackedSlot)` for a discarded bank, matching what
`is_bank_trackable` already tells the caller for Account/Transaction events.

While implementing this, a third call site with the identical defect turned up:
`handle_slot_lifecyle_status`'s `CreatedBank` branch also returned `Ok(())` for an already-discarded
bank, which is exactly the same problem -- a straggler `CreatedBank` for a discarded bank auto-
vivifies an unprunable buffer the same way a straggler entry or `BlockMeta` would. This wasn't in
the original finding, since the regression test only exercised Entry and BlockMeta, but it's the
same bug, so it's fixed here too rather than left as a known gap.

`audit_8_events_for_discarded_banks_are_rejected` now also asserts a straggler `CreatedBank` is
rejected, alongside the entry and `BlockMeta` cases it already covered. All three pass.

## 9. `DeadSlotDetected` is never constructed

**Severity:** medium &nbsp;&nbsp; **Status:** `FIXED` &nbsp;&nbsp; **Location:** `state_machine.rs:245`

`BlockStateMachineOutput::DeadSlotDetected` had zero construction sites in the workspace. It was
only declared, matched in `slot()`, and matched in `stream.rs:297`.

Everything downstream of it was therefore unreachable: `PendingEvent::DeadBlockDetect`,
`BlockMachineOutput::DeadBlockDetected`, and the public `BlockStreamEvent::DeadBlockDetected` that
`lib.rs:45` advertises as one of four stream outputs and that the example handles.

Dead slots surfaced as `ForksDetected`, via `mark_slot_as_dead` calling `mark_slot_as_forked`. That
was indistinguishable from an ordinary fork, and (before finding 5's fix) arrived with no bank ids.

**Fix.** Applied, by extending finding 5's own snapshot mechanism rather than adding a new one:
`dead_slot_bank_ids_snapshot` is populated by `mark_slot_as_dead`, and only by it, for the exact
slot the wire declared `Dead` this tick -- so its presence at flush time is already a precise
signal that this specific slot (not a descendant forked only as a side effect of walking its
children) was the direct cause. `flush_forks_detected_in_current_tick` now branches on that: the
directly-dead slot gets the dedicated `DeadSlotDetected` output, using the already-snapshotted bank
ids; every other slot in the same flush (an ordinary fork, or a live descendant forked alongside
the dead one) still falls back to a fresh `slot_to_banks` lookup and gets `ForksDetected` exactly as
before. No changes were needed in `stream.rs`, `client_ext.rs`, or the example -- that plumbing
already existed correctly, it was simply never reachable.

This changes what a directly-dead slot emits, from `ForksDetected` to `DeadSlotDetected`, so three
tests that asserted the old (incorrect) output needed updating to match: the pre-existing
`dead_slot_discards_every_bank_registered_for_it`, and `audit_5`/`audit_5b` (a new `dead_reports`
helper mirrors the existing `fork_reports` one). All three now pass, along with the new
`audit_9_dead_slot_emits_a_dead_slot_output`.

## 10. `dead_blocks_queue` is never written

**Severity:** low &nbsp;&nbsp; **Status:** `READ` &nbsp;&nbsp; **Location:** `state_machine.rs:445`

The field is initialised and its length is reported by `stats()`, but nothing in the workspace ever
pushes to it. `BlockstoreStats::dead_block_queue_len`, exposed through
`BlockStream::state_machine_stats`, can only ever read zero. A dashboard built on it would show no
dead blocks regardless of how many occur.

## 11. `FrozenBlock::entries` is emitted in hash-map order

**Severity:** low &nbsp;&nbsp; **Status:** `FIXED` &nbsp;&nbsp; **Location:** `state_machine.rs:164`

`Block::freeze` built the vector with `self.entries.values().cloned().collect()` over an
`FxHashMap<u64, EntryInfo>`, so entry order was nondeterministic rather than sorted by
`entry_index`.

Neither accumulator in this crate read that field, so there was no live data loss. It remained a
hazard on a public, `Serialize`/`Deserialize` type: an external consumer would reasonably assume
block order.

**Fix.** Applied: `freeze` now collects `self.entries.into_values()` into a `Vec` and sorts it with
`sort_unstable_by_key(|entry| entry.entry_index)` before building `FrozenBlock`.
`audit_11_frozen_block_entries_are_ordered_by_entry_index`, which feeds entries in a deliberately
scrambled order (`[5, 0, 3, 1, 4, 2]`) and checks the frozen output comes back as `[0, 1, 2, 3, 4,
5]`, passes.

## 12. Declared but unused items

**Severity:** low &nbsp;&nbsp; **Status:** `READ`

`tick_entry_cnt` is incremented and never read. `min_history_revision_in_queue` is assigned and
never read. `BlockstorePublisherConfig`, `InvalidBlock`, `InnerBlockSequence`, `AVG_BLOCK_LEN` and
`AVG_TPB` have no uses anywhere.

Each is individually harmless. Together with findings 9 and 10 they suggest the rewrite left
scaffolding behind, which makes it hard for a reader to tell intended-but-unfinished from
deliberately-removed.

## 19. Every `Account` update was classified as a sysvar, regardless of its owner

**Severity:** high &nbsp;&nbsp; **Status:** `FIXED` &nbsp;&nbsp; **Location:** `proto_adapter.rs:70`
(`extract_geyser_ev_info`)

`GeyserEventAdapter::extract_geyser_ev_info` mapped `UpdateOneof::Account(_)` to
`GeyserEventInfo::SysvarAccount` unconditionally, regardless of the account's actual `owner`.
`subscribe_block` force-subscribes only sysvar-owned accounts via the crate's own reserved filter,
but a caller's own `SubscribeRequest.accounts` can legitimately ask for arbitrary non-sysvar
accounts too -- every one of those was misrouted through the sysvar path instead of `BankData`.

Two independent effects followed from the misrouting, both silent: (1) `add_event`'s must-have
bitmask check (`MUST_HAVE_SYSVAR_ACCOUNTS.iter().position(...)`) ran needlessly on every non-sysvar
account, which happened to be harmless since none of them could ever match one of the four
sysvar pubkeys; (2) the mismatch was purely representational, not yet correctness-affecting for a
client that only asked for sysvars -- but any client whose own `accounts` filter named non-sysvar
program-owned accounts would have every one of them tagged as `GeyserEventInfo::SysvarAccount`,
which is the wrong event kind for downstream consumers of `GeyserEventInfo` to reason about (e.g.
anything matching on `SysvarAccount` vs `BankData` by variant, expecting the variant itself to mean
"this is a sysvar").

**Fix.** Applied: `extract_geyser_ev_info` now checks `account.account.owner` against
`SYSVAR_PROGRAM_ID` and only produces `GeyserEventInfo::SysvarAccount` when it matches; otherwise it
produces `GeyserEventInfo::BankData`, same as `Transaction`/`TransactionStatus`. The three
pre-existing `block_accumulator.rs` tests were relying on their `sysvar_account_update` test helper
hardcoding `owner: vec![0; 32]` instead of the real sysvar program ID -- fixed to use
`SYSVAR_PROGRAM_ID` so they still exercise what they claim to. A new test,
`non_sysvar_account_does_not_count_toward_the_must_have_mask`, feeds a non-sysvar account (random
owner) alongside a complete set of real sysvars and asserts the block still seals and the extra
account is still delivered via the ordinary `BankData`/`account_idx_map` path.

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

- Wire adaptation in `dragonsmouth/proto_adapter.rs` and `wrapper.rs` -- partially reviewed since
  the above: finding 19 (every `Account` update misclassified as a sysvar) was found and fixed here.
  Still open: the reachable `expect` calls on wire-supplied hash strings and the `dead_error`
  downgrade path.
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

- `crates/yellowstone-block-machine/src/state_machine.rs`, `mod tests` -- fifteen tests named
  `audit_*`, one per finding (findings 1 through 9 and 11), living alongside the crate's own
  pre-existing tests. They started out in a standalone `audit_regression` module, each asserting
  the *correct* behaviour so it would fail until its finding was fixed; now that every one of them
  passes, they were merged into `mod tests` since a fixed `audit_*` test is no longer meaningfully
  different from any other regression test in the suite. Their doc comments, each naming the
  `AUDIT.md` finding it covers, were kept intact through the merge. Two duplicate helpers
  (`created_bank`, `dead`) and one renamed one (`cmt` -> `commitment`, reusing the pre-existing
  helper of the same shape) were dropped in the merge; nothing else about the tests changed.
- `crates/yellowstone-block-machine/src/forks.rs`, module `forks_tests` -- three additional unit
  tests (`reparent_*`) exercising finding 4's fix directly against the new `Forks` method, since
  that is where the actual defect lived. These were always ordinary passing tests for the new
  method, not inverted -- there was no way to write a "must currently fail" test at that level
  before the method existed.
- `crates/yellowstone-block-machine/src/dragonsmouth/block_accumulator.rs`, `mod tests` -- one
  additional test, `non_sysvar_account_does_not_count_toward_the_must_have_mask`, for finding 19.
  Also an ordinary passing test for the same reason as the `forks.rs` ones above.

```text
cargo test --all-features audit_          # the 15 audit_* tests, by name prefix
```

Expect zero failures. Every finding with a regression test (1 through 9, 11, and 19) is fixed; all
fifteen `audit_*` tests pass, alongside the crate's other pre-existing tests in the same module,
plus three `forks.rs`-level unit tests for finding 4 (one of the pre-existing tests,
`dead_slot_discards_every_bank_registered_for_it`, was itself updated to expect the now-correct
`DeadSlotDetected` output instead of `ForksDetected`, since finding 9 changed what a directly-dead
slot emits) and one `block_accumulator.rs`-level unit test for finding 19:

```text
cargo test --all-features    # 55 passed in the lib target (state_machine.rs's mod tests holds
                              # 23 -- 8 pre-existing plus the 15 audit_* -- forks.rs holds 22, and
                              # dragonsmouth::block_accumulator::tests holds 4)
```

| Test | Finding | Assertion (originally failing, now passing unless marked otherwise) |
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
| `audit_5_dead_slot_event_carries_its_bank_ids` | 5 | **FIXED.** The dead slot's event now names banks 70 and 71 via a same-tick snapshot, delivered as `DeadSlotDetected` (finding 9). |
| `audit_5b_descendant_fork_in_the_same_tick_still_reports_its_own_live_bank_ids` | 5 | **FIXED (added).** A descendant forked in the same tick still reports its own live bank_ids via `ForksDetected`, unaffected. |
| `audit_6_discarded_loser_is_announced_for_pruning` | 6 | **FIXED.** Bank 500 now reaches the deadletter queue via a new `DeadletterEvent::Discarded` variant. |
| `audit_7_unresolved_slot_state_is_eventually_reclaimed` | 7 | **FIXED.** Ten abandoned slots survive 25 ordinary `gc()` passes, then are reclaimed by `gc_with_now` once `MAX_UNRESOLVED_SLOT_AGE` has passed. |
| `audit_8_events_for_discarded_banks_are_rejected` | 8 | **FIXED.** Entry, BlockMeta, and CreatedBank stragglers for discarded bank 70 all now return `Err`. |
| `audit_9_dead_slot_emits_a_dead_slot_output` | 9 | **FIXED.** A Dead lifecycle update now produces `DeadSlotDetected`, not `ForksDetected`. |
| `audit_11_frozen_block_entries_are_ordered_by_entry_index` | 11 | **FIXED.** Entries now come out sorted by `entry_index`. |
| (block_accumulator.rs) `non_sysvar_account_does_not_count_toward_the_must_have_mask` | 19 | **FIXED.** A non-sysvar account is classified `BankData`, not `SysvarAccount`, and is still delivered normally. Not named `audit_*` -- it lives in `block_accumulator.rs`'s own test module, not `state_machine.rs`'s, so it isn't selected by the `audit_` prefix filter above. |

Findings 10 and 12 are absence-of-code observations with nothing to assert at runtime. Confirm
they are still just dead code, not fixed into something new, with:

```text
rg 'dead_blocks_queue|tick_entry_cnt|min_history_revision_in_queue' crates/
```
