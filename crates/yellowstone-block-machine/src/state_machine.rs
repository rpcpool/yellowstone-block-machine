use {
    crate::forks::{Forks, ForksMutationTracer},
    derive_more::From,
    rustc_hash::{FxHashMap, FxHashSet},
    serde::{Deserialize, Serialize},
    solana_clock::{BankId, Slot},
    solana_commitment_config::CommitmentLevel,
    solana_hash::Hash,
    std::{
        collections::VecDeque,
        time::{Duration, Instant},
    },
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SlotLifecycle {
    FirstShredReceived,
    CreatedBank,
    Completed,
    Dead,
}

#[derive(Debug, Clone)]
pub struct SlotCommitmentStatusUpdate {
    pub parent_slot: Option<Slot>,
    pub slot: Slot,
    pub commitment: CommitmentLevel,
    pub bank_id: BankId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SlotLifecycleUpdate {
    pub parent_slot: Option<Slot>,
    pub slot: Slot,
    pub stage: SlotLifecycle,
    ///
    /// The bank instance this update applies to. Only ever `Some` for `CreatedBank` — the other
    /// three lifecycle stages are slot-scoped, not bank-scoped (see `event::SlotUpdateEvInfo`).
    ///
    pub bank_id: Option<BankId>,
}

pub struct BlockstorePublisherConfig {
    pub linger: u64,
    pub max_batch_size: bytesize::ByteSize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BlockSummary {
    pub slot: Slot,
    pub entry_count: u64,
    pub parent_slot: Slot,
    pub executed_transaction_count: u64,
    pub blockhash: Hash,
    pub parent_blockhash: Hash,
    ///
    /// Unix timestamp the block was produced at. `0` if the wire didn't report one (this is
    /// also what the optimistic-freeze path forges, since it has no wire `BlockMeta` to draw
    /// this from).
    ///
    pub block_time: u64,
    pub bank_id: BankId,
}

#[derive(Debug, Clone, From)]
pub enum BlockReplayEvent {
    SlotLifecycleStatus(SlotLifecycleUpdate),
    Entry(EntryInfo),
    BlockSummary(BlockSummary),
}

#[derive(Debug, Clone, From)]
pub enum ConsensusUpdate {
    SlotCommitmentStatus(SlotCommitmentStatusUpdate),
}

pub type InnerBlockSequence = i64;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct EntryInfo {
    pub slot: Slot,
    pub entry_index: u64,
    pub starting_txn_index: u64,
    pub entry_hash: Hash,
    pub executed_txn_count: u64,
    pub bank_id: BankId,
}

///
/// Bank instance under construction. Keyed by `bank_id`, not `slot` — see the module doc
/// comment on [`BlocksStateMachine`] for why.
///
#[derive(Debug)]
pub struct Block {
    pub slot: Slot,
    pub bank_id: BankId,
    entries: FxHashMap<u64, EntryInfo>,
    entry_cnt: u64,
    tick_entry_cnt: u64,
    created_at: std::time::Instant,
    created_bank_seen: bool,
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq)]
pub struct FrozenBlock {
    pub slot: Slot,
    pub bank_id: BankId,
    pub parent_slot: Slot,
    pub entries: Vec<EntryInfo>,
    pub blockhash: Hash,
    ///
    /// The entry count reported by the wire's BlockMeta itself -- independent of however many
    /// `Entry` events this crate's own sans-io core happened to buffer, so a downstream
    /// `BlockAccumulator` can verify its own observed entry count against the wire's expectation
    /// rather than trusting this crate's internal bookkeeping.
    ///
    pub entries_count: u64,
    pub executed_transaction_count: u64,
    pub parent_blockhash: Hash,
    pub block_time: u64,
}

// Avg 2k tx + 2k account update
pub const AVG_BLOCK_LEN: usize = 4000;
// Avg transaction per block ~ 2000;
pub const AVG_TPB: usize = 2000;

///
///
/// State machine for buffering blockstore events based on linger and other buffering limits.
///
/// Implements a sans-IO API for better easier testability and better separation of concerns.
///
/// If you want to use this in a real-world scenario, you need to implement your own sync/async driver
/// to progress in the same machine.
///
impl Block {
    pub fn new_with_clock(slot: Slot, bank_id: BankId, clock: Instant) -> Self {
        Self {
            slot,
            bank_id,
            entries: Default::default(),
            created_at: clock,
            entry_cnt: 0,
            tick_entry_cnt: 0,
            created_bank_seen: false,
        }
    }

    pub fn new(slot: Slot, bank_id: BankId) -> Self {
        Self::new_with_clock(slot, bank_id, Instant::now())
    }

    fn last_entry_hash(&self) -> Option<Hash> {
        self.entries
            .get(&(self.entry_cnt - 1))
            .map(|entry| entry.entry_hash)
    }

    fn freeze(self, summary: &BlockSummary) -> FrozenBlock {
        FrozenBlock {
            slot: self.slot,
            bank_id: self.bank_id,
            entries: self.entries.values().cloned().collect(),
            blockhash: summary.blockhash,
            parent_slot: summary.parent_slot,
            entries_count: summary.entry_count,
            executed_transaction_count: summary.executed_transaction_count,
            parent_blockhash: summary.parent_blockhash,
            block_time: summary.block_time,
        }
    }

    fn can_be_optimistic_frozen(&self) -> bool {
        if self.entry_cnt == 0 {
            return false;
        }

        (0..self.entry_cnt).all(|idx| self.entries.contains_key(&idx))
    }

    fn forge_optimistic_block_summary(&self, parent_slot: Slot) -> BlockSummary {
        BlockSummary {
            slot: self.slot,
            bank_id: self.bank_id,
            parent_slot,
            entry_count: self.entry_cnt,
            executed_transaction_count: self.entries.values().map(|e| e.executed_txn_count).sum(),
            blockhash: self.last_entry_hash().expect("last entry hash"),
            // Genuinely unknown in a forged summary -- the real BlockMeta never arrived, which
            // is exactly why this recovery path exists.
            parent_blockhash: Hash::default(),
            block_time: 0,
        }
    }

    fn insert_entry(&mut self, block_entry: EntryInfo) {
        let entry_idx = block_entry.entry_index;
        let tx_count = block_entry.executed_txn_count;
        if self.entries.insert(entry_idx, block_entry).is_none() {
            self.entry_cnt += 1;

            if tx_count == 0 {
                self.tick_entry_cnt += 1;
            }
        }
    }
}

type Revision = usize;

#[derive(Debug)]
pub enum DeadletterEvent {
    Incomplete(BankId),
}

///
/// Dead blocks will never be frozen
///
pub struct InvalidBlock {
    pub slot: Slot,
}

#[derive(Debug, Clone)]
pub struct ForkDetected {
    pub slot: Slot,
    ///
    /// Every bank instance known for this slot at the moment the fork was detected. May be
    /// empty if no bank ever showed up for this slot in this pipeline.
    ///
    pub bank_ids: Vec<BankId>,
}

#[derive(Debug, Clone)]
pub struct DeadBlockDetected {
    pub slot: Slot,
    pub bank_ids: Vec<BankId>,
}

#[derive(Debug)]
pub enum BlockStateMachineOutput {
    FrozenBlock(FrozenBlock),
    SlotStatus(SlotCommitmentStatusUpdate),
    ForksDetected(ForkDetected),
    DeadSlotDetected(DeadBlockDetected),
}

impl BlockStateMachineOutput {
    pub const fn slot(&self) -> Slot {
        match self {
            Self::DeadSlotDetected(blk) => blk.slot,
            Self::FrozenBlock(blk) => blk.slot,
            Self::SlotStatus(update) => update.slot,
            Self::ForksDetected(info) => info.slot,
        }
    }
}

///
/// Stats produce during [`BlocksStateMachine::gc`] operation.
///
#[derive(Debug, Default)]
pub struct BlockstoreGCStats {
    // Number of slot that were purged
    pub slot_purge_count: usize,
    // Number of slots could potentially be purge but that were blocked by some condition.
    pub slot_blocked_count: usize,
}

#[derive(Debug, Default, Clone)]
pub struct BlockstoreStats {
    pub block_buffer_len: usize,
    pub forks_map_len: usize,
    pub dead_block_queue_len: usize,
    pub blockstore_update_queue_len: usize,
}

const fn cmp_commitment_level(a: CommitmentLevel, b: CommitmentLevel) -> std::cmp::Ordering {
    match (a, b) {
        (CommitmentLevel::Processed, CommitmentLevel::Processed) => std::cmp::Ordering::Equal,
        (CommitmentLevel::Finalized, CommitmentLevel::Finalized) => std::cmp::Ordering::Equal,
        (CommitmentLevel::Confirmed, CommitmentLevel::Confirmed) => std::cmp::Ordering::Equal,
        (CommitmentLevel::Processed, _) => std::cmp::Ordering::Less,
        (CommitmentLevel::Finalized, _) => std::cmp::Ordering::Greater,
        (CommitmentLevel::Confirmed, CommitmentLevel::Finalized) => std::cmp::Ordering::Less,
        (CommitmentLevel::Confirmed, CommitmentLevel::Processed) => std::cmp::Ordering::Greater,
    }
}

const fn commitment_rank(level: CommitmentLevel) -> u8 {
    match level {
        CommitmentLevel::Processed => 0,
        CommitmentLevel::Confirmed => 1,
        CommitmentLevel::Finalized => 2,
    }
}

fn iter_to_commitment(cl: &CommitmentLevel) -> impl Iterator<Item = CommitmentLevel> {
    match cl {
        CommitmentLevel::Processed => vec![CommitmentLevel::Processed].into_iter(),
        CommitmentLevel::Confirmed => {
            vec![CommitmentLevel::Processed, CommitmentLevel::Confirmed].into_iter()
        }
        CommitmentLevel::Finalized => vec![
            CommitmentLevel::Processed,
            CommitmentLevel::Confirmed,
            CommitmentLevel::Finalized,
        ]
        .into_iter(),
    }
}

#[derive(Debug)]
pub struct OldestBufferedBlockInfo {
    pub slot: Slot,
    pub bank_id: BankId,
    pub age: Duration,
    pub parent_slot: Option<Slot>,
    pub pending_slot_status: usize,
}

impl Default for BlocksStateMachine {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug)]
pub struct LongShortForksMutationTracer<'a> {
    long: &'a mut FxHashSet<Slot>,
    short: &'a mut FxHashSet<Slot>,
}

impl ForksMutationTracer<Slot> for LongShortForksMutationTracer<'_> {
    fn insert(&mut self, slot: Slot) {
        // We only insert into short if slot not already present in long.
        if self.long.insert(slot) {
            self.short.insert(slot);
        }
    }
}

///
/// Occurred when a replay event is rejected by the state machine because it relates to a bank
/// instance that cannot be tracked by the state machine (already discarded as a loser, or
/// already frozen and receiving anomalous late data).
///
#[derive(Debug, thiserror::Error)]
#[error("replay event rejected")]
pub struct UntrackedSlot;

///
/// Bank-instance-oriented block reconstruction state machine.
///
/// Buffers block content (`Block`) keyed by `bank_id` rather than `Slot`. Under Alpenglow, a
/// single slot can legitimately have more than one bank instance (dump-and-repair replay,
/// duplicate-unconfirmed-slot) — a slot-keyed buffer can't tell a genuine second bank apart from
/// a spurious re-announcement of the first, and either reading loses data. Keying by `bank_id`
/// sidesteps that entirely: each bank instance gets its own independent buffer, so there is
/// nothing for one to clobber in another.
///
/// A slot only ever has more than one live buffer while competing bank instances are still
/// unresolved. `Processed` never resolves, supersedes, or discards anything — multiple banks for
/// the same slot can be simultaneously `Processed` with no precedence between them. Only
/// `Confirmed`/`Finalized` ever names a slot's canonical bank (Solana guarantees at most one bank
/// per slot ever reaches either), at which point every other bank registered for that slot is
/// discarded as a loser.
///
/// [`Forks`] (slot-level chain-fork tracking) is a separate, orthogonal concern — it solves
/// competing *slot numbers* extending the same parent, not competing *bank instances* for one
/// slot number. Since `Forks` can only ever record one parent claim per slot, it is fed exactly
/// once per slot, from the resolved bank's own parent — never eagerly from an individual bank's
/// unresolved lifecycle/commitment updates, which could otherwise disagree with each other (see
/// `set_resolved_bank`/`register_resolved_parent`).
///
#[derive(Debug)]
pub struct BlocksStateMachine {
    /// Bank instances still accumulating content, keyed by bank_id.
    block_buffer_map: FxHashMap<BankId, Block>,

    /// Commitment levels already delivered for a bank instance, once it has frozen. Existence of
    /// an entry (even an empty set) means the bank has frozen.
    frozen_commitment_index: FxHashMap<BankId, FxHashSet<CommitmentLevel>>,

    /// Every bank_id ever seen for a slot, so the losers can be found once a winner is known.
    slot_to_banks: FxHashMap<Slot, Vec<BankId>>,

    /// The bank_id currently believed to be a slot's canonical bank. Only ever set/changed by a
    /// Confirmed/Finalized commitment update, or by single-candidate inference when a slot has
    /// only ever had one bank_id at all (`try_infer_sole_candidate_winner`). Once a slot reaches
    /// Confirmed or Finalized this is treated as final and can no longer change.
    resolved_bank_per_slot: FxHashMap<Slot, BankId>,

    /// The highest commitment level ever assigned to a slot's resolved bank, independent of
    /// whether that bank has frozen yet. Used to guard against a resolved Confirmed/Finalized
    /// slot ever being superseded.
    slot_min_commitment: FxHashMap<Slot, CommitmentLevel>,

    /// bank_ids discarded as losers once a slot resolved a different winner, paired with the
    /// slot they belonged to so `gc` can age them out. A straggler event arriving afterward for
    /// one of these is ignored outright.
    discarded_bank_ids: FxHashMap<BankId, Slot>,

    /// Every bank_id's known parent slot, learned from whichever source reports it first
    /// (`CreatedBank`, a commitment update, or `BlockMeta`). Kept independent of `Block` so a
    /// bank's parent stays queryable even after it has frozen (its `Block` buffer removed).
    bank_parent_slot: FxHashMap<BankId, Slot>,

    /// We queue slot status update if the bank has not yet frozen. This is to provide nice
    /// guarantee such that you will read the entire block before any slot status update for it.
    pending_slot_status_update: FxHashMap<BankId, VecDeque<SlotCommitmentStatusUpdate>>,

    /// Represents the passage of time when blockstore update occurs.
    /// this can be useful to track how things are old or schedule things in future revision.
    revision: Revision,

    /// Keep track of the minimum revision [`blockstore_update_queue`].
    min_history_revision_in_queue: Option<usize>,

    /// Update queue for blockstore events.
    blockstore_update_queue: VecDeque<(usize, BlockStateMachineOutput)>,

    /// Maps when it is safe to "deregister" slot.
    /// Deregistering a slot is removing all index data about this slot as it cannot be reference by downstream consumer.
    /// We handle finalized block separately since they are the easiest to deregister.
    deregister_finalized_slot_schedule: FxHashMap<Revision, Vec<Slot>>,

    /// Data from deregister_slot_schedule goes into a queue since we may want to process it in the future.
    pending_finalized_slot_deregister: VecDeque<Slot>,

    /// Maintain forks history of the blockchain.
    pub forks: Forks<Slot>,
    forks_history: FxHashSet<Slot>,

    ///
    /// Keep track of the slots that were detected as forks in the current tick.
    ///
    forks_detected_in_current_tick: FxHashSet<Slot>,

    /// Holds deadletter queue message for blockstore events that cannot be processed.
    dlq: VecDeque<DeadletterEvent>,

    /// Dead blocks are blocks who will never be frozen.
    /// This can happen when we boot fumarole initially : some block we receive slot status update before the block data.
    dead_blocks_queue: VecDeque<Slot>,

    ///
    /// Buffers slots that were retroactively rooted by a slot status update.
    ///
    retroactively_rooted_slots: FxHashSet<Slot>,

    ///
    /// Keep track of bank instances that need optimistic freeze because a descendant slot was
    /// frozen before them.
    ///
    need_optimistic_freeze: FxHashSet<BankId>,
}

impl BlocksStateMachine {
    ///
    /// Creates a new blockstore instance
    ///
    pub fn new() -> Self {
        Self {
            block_buffer_map: Default::default(),
            frozen_commitment_index: Default::default(),
            slot_to_banks: Default::default(),
            resolved_bank_per_slot: Default::default(),
            slot_min_commitment: Default::default(),
            discarded_bank_ids: Default::default(),
            bank_parent_slot: Default::default(),
            pending_slot_status_update: Default::default(),
            blockstore_update_queue: VecDeque::with_capacity(1000),
            revision: 0,
            min_history_revision_in_queue: Default::default(),
            dlq: Default::default(),
            deregister_finalized_slot_schedule: Default::default(),
            pending_finalized_slot_deregister: Default::default(),
            forks: Default::default(),
            forks_history: Default::default(),
            dead_blocks_queue: Default::default(),
            retroactively_rooted_slots: Default::default(),
            forks_detected_in_current_tick: Default::default(),
            need_optimistic_freeze: Default::default(),
        }
    }

    pub fn stats(&self) -> BlockstoreStats {
        BlockstoreStats {
            block_buffer_len: self.block_buffer_map.len(),
            forks_map_len: self.forks.len(),
            dead_block_queue_len: self.dead_blocks_queue.len(),
            blockstore_update_queue_len: self.blockstore_update_queue.len(),
        }
    }

    const fn next_history_revision(&mut self) -> usize {
        let temp = self.revision;
        self.revision += 1;
        temp
    }

    fn push_new_update(&mut self, update: BlockStateMachineOutput) -> Revision {
        let new_revision = self.next_history_revision();
        self.blockstore_update_queue
            .push_back((new_revision, update));
        new_revision
    }

    fn register_bank_for_slot(&mut self, slot: Slot, bank_id: BankId) {
        let ids = self.slot_to_banks.entry(slot).or_default();
        if !ids.contains(&bank_id) {
            ids.push(bank_id);
        }
    }

    fn get_or_create_bank(&mut self, slot: Slot, bank_id: BankId) -> &mut Block {
        self.register_bank_for_slot(slot, bank_id);
        self.block_buffer_map
            .entry(bank_id)
            .or_insert_with(|| Block::new(slot, bank_id))
    }

    ///
    /// A bank instance is trackable as long as it hasn't already been discarded as a loser.
    /// Used to gate content (Account/Transaction) events that this crate's sans-io core never
    /// buffers itself, but still needs to answer "should this be stored at all" for.
    ///
    pub fn is_bank_trackable(&self, bank_id: BankId) -> bool {
        !self.discarded_bank_ids.contains_key(&bank_id)
    }

    ///
    /// Infers a slot's resolved bank_id when it hasn't been named by a direct
    /// Confirmed/Finalized status update yet but there's exactly one known candidate for it —
    /// geyser doesn't guarantee a direct commitment status update arrives for every slot. With
    /// more than one still-unresolved candidate, which one is canonical genuinely can't be
    /// determined here, so it's left unresolved (both remain available at Processed regardless).
    ///
    fn try_infer_sole_candidate_winner(&mut self, slot: Slot) {
        if self.resolved_bank_per_slot.contains_key(&slot) {
            return;
        }
        if let Some([single]) = self.slot_to_banks.get(&slot).map(Vec::as_slice) {
            let single = *single;
            self.set_resolved_bank(slot, single);
        }
    }

    ///
    /// Marks `bank_id` as `slot`'s canonical bank: feeds its parent into [`Forks`] (exactly once
    /// per slot — see the struct doc comment on why only the resolved bank's parent is ever safe
    /// to use), and discards every other bank_id registered for the slot.
    ///
    fn set_resolved_bank(&mut self, slot: Slot, bank_id: BankId) {
        self.resolved_bank_per_slot.insert(slot, bank_id);
        if let Some(&parent) = self.bank_parent_slot.get(&bank_id) {
            self.register_resolved_parent(slot, parent);
        }
        self.discard_losing_banks(slot, bank_id);
    }

    fn register_resolved_parent(&mut self, slot: Slot, parent: Slot) {
        let mut multiset = LongShortForksMutationTracer {
            long: &mut self.forks_history,
            short: &mut self.forks_detected_in_current_tick,
        };
        self.forks.add_slot_with_parent_with_rooted_trace(
            slot,
            parent,
            &mut multiset,
            &mut self.retroactively_rooted_slots,
        );
    }

    fn discard_losing_banks(&mut self, slot: Slot, winner: BankId) {
        let Some(ids) = self.slot_to_banks.get_mut(&slot) else {
            return;
        };
        let losers: Vec<BankId> = ids.iter().copied().filter(|&id| id != winner).collect();
        ids.retain(|&id| id == winner);
        for loser in losers {
            self.remove_bank_references(loser);
            self.discarded_bank_ids.insert(loser, slot);
        }
    }

    ///
    /// Removes a single bank instance's own buffered state. Unlike
    /// [`Self::remove_slot_references_in_state`], this deliberately leaves `slot_to_banks`,
    /// `resolved_bank_per_slot`, and `discarded_bank_ids` untouched — the bank_id may still be a
    /// live, known candidate (just with no buffered content), not a slot-wide teardown.
    ///
    fn remove_bank_references(&mut self, bank_id: BankId) {
        self.block_buffer_map.remove(&bank_id);
        self.frozen_commitment_index.remove(&bank_id);
        self.pending_slot_status_update.remove(&bank_id);
        self.bank_parent_slot.remove(&bank_id);
        self.need_optimistic_freeze.remove(&bank_id);
    }
}

impl BlocksStateMachine {
    fn handle_slot_lifecyle_status(
        &mut self,
        slot_lifecycle_status: SlotLifecycleUpdate,
    ) -> Result<(), UntrackedSlot> {
        let slot = slot_lifecycle_status.slot;

        match slot_lifecycle_status.stage {
            SlotLifecycle::FirstShredReceived => {
                tracing::trace!("First shred received for slot {}", slot);
                // Neither carries a bank_id, and neither is needed: buffering is keyed by
                // bank_id and starts lazily on first sight of any bank-scoped event, not on
                // FirstShredReceived.
            }
            SlotLifecycle::CreatedBank => {
                let Some(bank_id) = slot_lifecycle_status.bank_id else {
                    tracing::warn!(
                        "CreatedBank status for slot {slot} carries no bank_id; ignoring"
                    );
                    return Ok(());
                };
                tracing::trace!("Bank {bank_id} created for slot {slot}");
                if self.discarded_bank_ids.contains_key(&bank_id) {
                    return Ok(());
                }
                if let Some(parent) = slot_lifecycle_status.parent_slot {
                    self.bank_parent_slot.entry(bank_id).or_insert(parent);
                }
                let bank = self.get_or_create_bank(slot, bank_id);
                bank.created_bank_seen = true;
                // Deliberately NOT resolving here, even if this is the only bank_id seen for
                // the slot so far: a genuine second bank's own CreatedBank could still be about
                // to arrive, and resolving early would feed Forks a premature parent claim (see
                // the struct doc comment). Resolution is attempted lazily instead, once there's
                // an actual reason to need it (freeze or a commitment update).
            }
            SlotLifecycle::Completed => {
                // Purely informational under bank_id keying: freezing is driven by BlockMeta
                // arrival for a specific bank_id (see `handle_block_summary`), not by this
                // slot-scoped signal, which per the wire format "does not belong to any bank".
            }
            SlotLifecycle::Dead => {
                self.mark_slot_as_dead(slot);
            }
        }
        Ok(())
    }

    fn handle_slot_commitment_status_update(&mut self, slot_status: SlotCommitmentStatusUpdate) {
        let slot = slot_status.slot;
        let bank_id = slot_status.bank_id;
        let commitment = slot_status.commitment;

        if self.discarded_bank_ids.contains_key(&bank_id) {
            tracing::warn!(
                "commitment update for slot {slot} targets bank_id {bank_id}, which was already discarded as a loser -- dropping"
            );
            return;
        }

        self.register_bank_for_slot(slot, bank_id);
        if let Some(parent) = slot_status.parent_slot {
            self.bank_parent_slot.entry(bank_id).or_insert(parent);
        }

        // Processed is optimistic, and multiple banks for the same slot can legitimately be
        // simultaneously Processed with no precedence between any of them -- so a Processed
        // sighting must never resolve, supersede, or discard anything. Only Confirmed/Finalized
        // is authoritative: Solana guarantees at most one bank per slot ever reaches it, and
        // once it does, that resolution is final.
        if commitment == CommitmentLevel::Processed {
            self.try_infer_sole_candidate_winner(slot);
        } else {
            match self.resolved_bank_per_slot.get(&slot).copied() {
                Some(existing) if existing == bank_id => {}
                Some(existing) => {
                    let existing_floor = self.slot_min_commitment.get(&slot).copied();
                    if matches!(
                        existing_floor,
                        Some(CommitmentLevel::Confirmed) | Some(CommitmentLevel::Finalized)
                    ) {
                        tracing::warn!(
                            "commitment update for slot {slot} targets bank_id {bank_id}, but bank_id {existing} was already {existing_floor:?} for this slot -- dropping"
                        );
                        return;
                    }
                    tracing::warn!(
                        "slot {slot}'s resolved bank changed from {existing} to {bank_id} (previous commitment: {existing_floor:?}) -- superseding, likely a dump-and-repair replay correcting an optimistic Processed bank"
                    );
                    self.set_resolved_bank(slot, bank_id);
                }
                None => self.set_resolved_bank(slot, bank_id),
            }
            let floor = self
                .slot_min_commitment
                .entry(slot)
                .or_insert(CommitmentLevel::Processed);
            if commitment_rank(commitment) > commitment_rank(*floor) {
                *floor = commitment;
            }
        }

        self.deliver_or_queue(bank_id, slot_status);
    }

    fn deliver_or_queue(&mut self, bank_id: BankId, slot_status: SlotCommitmentStatusUpdate) {
        if self.frozen_commitment_index.contains_key(&bank_id) {
            self.deliver_commitment(bank_id, slot_status);
        } else {
            self.pending_slot_status_update
                .entry(bank_id)
                .or_default()
                .push_back(slot_status);
        }
    }

    ///
    /// Delivers a commitment update for a bank that has already frozen, gap-filling any lower
    /// commitment levels that were never individually reported for this specific bank (e.g. a
    /// bank that jumps straight to Confirmed without its own Processed update first).
    ///
    fn deliver_commitment(&mut self, bank_id: BankId, slot_status: SlotCommitmentStatusUpdate) {
        let slot = slot_status.slot;
        let commitment = slot_status.commitment;
        let parent_slot = slot_status
            .parent_slot
            .or_else(|| self.bank_parent_slot.get(&bank_id).copied());
        let Some(visited) = self.frozen_commitment_index.get_mut(&bank_id) else {
            return;
        };

        let mut to_push = Vec::new();
        for level in iter_to_commitment(&commitment) {
            if visited.insert(level) {
                to_push.push(SlotCommitmentStatusUpdate {
                    bank_id,
                    slot,
                    parent_slot,
                    commitment: level,
                });
            }
        }

        // This code handles a tricky part where slot status update of higher level commitment
        // level may arrive before lower level commitment. By iterating from lower to higher
        // level commitment, we can ensure that we don't miss any slot status update.
        for update in to_push {
            let revision = self.push_new_update(BlockStateMachineOutput::SlotStatus(update));
            tracing::debug!(
                "Slot status update for slot {slot} (bank {bank_id}) at revision {revision}"
            );
            if commitment == CommitmentLevel::Finalized {
                let mut multiset = LongShortForksMutationTracer {
                    long: &mut self.forks_history,
                    short: &mut self.forks_detected_in_current_tick,
                };
                self.forks.make_slot_rooted_with_rooted_trace(
                    slot,
                    &mut multiset,
                    &mut self.retroactively_rooted_slots,
                );
                self.deregister_finalized_slot_schedule
                    .entry(revision)
                    .or_default()
                    .push(slot);
            }
        }
    }

    /// Pending slot updates are pending because the bank hasn't frozen yet.
    fn flush_pending_slot_status_update(&mut self, bank_id: BankId) {
        if let Some(updates) = self.pending_slot_status_update.remove(&bank_id) {
            for update in updates {
                self.deliver_commitment(bank_id, update);
            }
        }
    }

    ///
    /// Mark a slot as dead: every bank instance ever registered for it will never be frozen.
    ///
    fn mark_slot_as_dead(&mut self, slot: Slot) {
        let mut multitrace = LongShortForksMutationTracer {
            long: &mut self.forks_history,
            short: &mut self.forks_detected_in_current_tick,
        };
        self.forks.mark_slot_as_forked(slot, &mut multitrace);
        // Dead is authoritative and permanent -- unlike gc()'s aging-out sweep, every bank_id
        // this slot ever had must be marked discarded so a late straggler event can never
        // resurrect a fresh buffer for it (see `discard_losing_banks` for the same pattern).
        let bank_ids = self.slot_to_banks.get(&slot).cloned().unwrap_or_default();
        self.remove_slot_references_in_state(slot);
        for bank_id in bank_ids {
            self.discarded_bank_ids.insert(bank_id, slot);
        }
    }

    fn handle_block_entry_insert(&mut self, data: EntryInfo) -> Result<(), UntrackedSlot> {
        let bank_id = data.bank_id;
        if self.discarded_bank_ids.contains_key(&bank_id) {
            return Ok(());
        }
        if self.frozen_commitment_index.contains_key(&bank_id) {
            tracing::error!(
                "UNEXPECTED: entry for bank {bank_id} (slot {}) that is already frozen. Dropping.",
                data.slot
            );
            return Err(UntrackedSlot);
        }
        let slot = data.slot;
        let bank = self.get_or_create_bank(slot, bank_id);
        bank.insert_entry(data);
        Ok(())
    }

    #[inline]
    fn push_to_dlq(&mut self, msg: DeadletterEvent) {
        self.dlq.push_back(msg);
    }

    fn process_retroactively_rooted_slots(&mut self) {
        if self.retroactively_rooted_slots.is_empty() {
            return;
        }
        let retroactively_rooted_slots = std::mem::take(&mut self.retroactively_rooted_slots);
        for slot in retroactively_rooted_slots {
            let Some(&bank_id) = self.resolved_bank_per_slot.get(&slot) else {
                tracing::warn!(
                    "slot {slot} retroactively rooted but has no resolved bank_id yet -- cannot propagate Finalized for it"
                );
                continue;
            };
            tracing::trace!("Retroactively rooting slot {slot} (bank {bank_id})");
            self.handle_slot_commitment_status_update(SlotCommitmentStatusUpdate {
                slot,
                parent_slot: self.bank_parent_slot.get(&bank_id).copied(),
                commitment: CommitmentLevel::Finalized,
                bank_id,
            });
        }
    }

    fn flush_forks_detected_in_current_tick(&mut self) {
        if self.forks_detected_in_current_tick.is_empty() {
            return;
        }
        let forks_detected = std::mem::take(&mut self.forks_detected_in_current_tick);
        for slot in forks_detected {
            tracing::warn!("Forks detected for slot {}", slot);
            let bank_ids = self.slot_to_banks.get(&slot).cloned().unwrap_or_default();
            self.push_new_update(BlockStateMachineOutput::ForksDetected(ForkDetected {
                slot,
                bank_ids,
            }));
        }
    }

    ///
    /// Freeze a bank instance when we receive its block summary (BlockMeta). Auto-vivifies the
    /// bank's buffer if this is the very first event ever seen for it -- BlockMeta racing ahead
    /// of entries is possible, and this crate freezes on the BlockMeta signal directly rather
    /// than by counting entries/transactions against it.
    ///
    fn handle_block_summary(&mut self, block_summary: BlockSummary) -> Result<(), UntrackedSlot> {
        let slot = block_summary.slot;
        let bank_id = block_summary.bank_id;

        if self.discarded_bank_ids.contains_key(&bank_id) {
            tracing::debug!(
                "block summary for slot {slot} targets already-discarded bank {bank_id}; dropping"
            );
            return Ok(());
        }
        if self.frozen_commitment_index.contains_key(&bank_id) {
            tracing::error!(
                "UNEXPECTED: duplicate block summary for bank {bank_id} (slot {slot}) that is already frozen. Dropping."
            );
            return Err(UntrackedSlot);
        }

        self.register_bank_for_slot(slot, bank_id);
        self.bank_parent_slot
            .entry(bank_id)
            .or_insert(block_summary.parent_slot);

        let block = self
            .block_buffer_map
            .remove(&bank_id)
            .unwrap_or_else(|| Block::new(slot, bank_id));
        if !block.created_bank_seen {
            tracing::warn!(
                "Freezing bank {bank_id} (slot {slot}) without ever having observed CreatedBank for it"
            );
        }

        let frozen_block = block.freeze(&block_summary);
        assert_eq!(slot, frozen_block.slot);
        assert_eq!(bank_id, frozen_block.bank_id);

        self.frozen_commitment_index.entry(bank_id).or_default();
        self.try_infer_sole_candidate_winner(slot);

        // If this bank is (or becomes) the slot's resolved winner, make sure Forks has learned
        // its parent now that we definitely know it -- a no-op if `set_resolved_bank` already
        // registered it earlier via a commitment update.
        if self.resolved_bank_per_slot.get(&slot) == Some(&bank_id) {
            self.register_resolved_parent(slot, block_summary.parent_slot);
        }

        // This should never happen, but in case it does we will try to optimistically freeze
        // whichever bank at the parent slot this child's own BlockMeta names as its parent --
        // identified by content, not by consensus resolution: a still-buffering candidate's
        // would-be blockhash (`last_entry_hash`, the same value `forge_optimistic_block_summary`
        // would freeze it with) must match `block_summary.parent_blockhash` byte-for-byte. Under
        // bank_id keying the parent slot can legitimately have more than one bank instance, and
        // only the one this child's hash cryptographically names is ever the real parent; any
        // other still-buffering bank there is a competing instance awaiting discard, not a late
        // winner, and matching by hash (rather than by resolution, which may not have happened
        // yet, or by "every buffering bank", which fabricates blocks for losers) is both more
        // precise and available earlier. `Hash::default()` is the sentinel this crate's own
        // forged summaries use for "unknown" (see `forge_optimistic_block_summary`), so a child
        // reporting it can never be matched against here -- there is nothing to safely identify.
        if block_summary.parent_blockhash != Hash::default() {
            if let Some(parent_ids) = self.slot_to_banks.get(&block_summary.parent_slot).cloned() {
                for parent_bank_id in parent_ids {
                    if let Some(block) = self.block_buffer_map.get(&parent_bank_id) {
                        if block.can_be_optimistic_frozen()
                            && block.last_entry_hash() == Some(block_summary.parent_blockhash)
                        {
                            tracing::warn!(
                                "Freezing bank {bank_id} (slot {slot}) whose parent slot {} still has bank {parent_bank_id} in the buffer, identified as the parent by matching blockhash",
                                block_summary.parent_slot
                            );
                            self.need_optimistic_freeze.insert(parent_bank_id);
                        }
                    }
                }
            }
        }

        tracing::debug!("Block frozen for bank {bank_id} (slot {slot})");
        self.push_new_update(BlockStateMachineOutput::FrozenBlock(frozen_block));

        if let Some(max_pending_commitment_level) = self
            .pending_slot_status_update
            .get(&bank_id)
            .iter()
            .flat_map(|update| update.iter())
            .filter(|update| {
                cmp_commitment_level(update.commitment, CommitmentLevel::Processed).is_gt()
            })
            .max_by(|x, y| cmp_commitment_level(x.commitment, y.commitment))
        {
            // Sometime we may have a "processed" commitment level happens before slot is frozen,
            // this is because "frozen" state depends on block meta which may be sent after "Processed" slot status.
            // Technically, it should be impossible to have a commitment level higher than Processed and the slot not frozen already.
            // We should probably panic here, but we will just log an error.

            // Why we may received "Processed" slot status before the block meta?
            // In agave code base, as slot commitment level are updated, they directly notify loaded geyser plugins, blocking the replay stage
            // until they all acknowledge the slot status update.
            // On the other hand, block meta message are sent of a crossbeam channel that is eventually process by a background thread, which add a bit of latency
            // During replay, block meta is sent than right after it sends the slot status update.
            // Most of the time, we receive the block meta before the slot status update, but in some cases, we may receive the slot status update first.

            tracing::warn!(
                "Bank {bank_id} (slot {slot}) froze after receiving slot status update higher than Processed: {}",
                max_pending_commitment_level.commitment
            );
        }
        self.flush_pending_slot_status_update(bank_id);
        Ok(())
    }

    ///
    /// Main entry point to process blockstore event and progress the state-machine.
    ///
    /// Registers the event into the state machine and process any side effect that may arise from it.
    ///
    pub fn process_replay_event(&mut self, event: BlockReplayEvent) -> Result<(), UntrackedSlot> {
        match event {
            BlockReplayEvent::BlockSummary(bs) => {
                tracing::trace!("Inserting block summary for slot {}", bs.slot);
                self.handle_block_summary(bs)?;
            }
            BlockReplayEvent::Entry(data) => {
                self.handle_block_entry_insert(data)?;
            }
            BlockReplayEvent::SlotLifecycleStatus(slot_lifecycle_status) => {
                tracing::trace!(
                    "Inserting slot lifecycle status for slot {}",
                    slot_lifecycle_status.slot
                );
                self.handle_slot_lifecyle_status(slot_lifecycle_status)?;
            }
        }

        self.process_retroactively_rooted_slots();
        self.flush_forks_detected_in_current_tick();
        self.execute_optimistic_freeze_for_needed_banks();
        Ok(())
    }

    fn execute_optimistic_freeze_for_needed_banks(&mut self) {
        if self.need_optimistic_freeze.is_empty() {
            return;
        }
        let bank_ids = std::mem::take(&mut self.need_optimistic_freeze);
        for bank_id in bank_ids {
            // Check if we can freeze the block: we must have some entry to compute the block
            // hash, and a known parent slot to attribute it to.
            if let Some(block) = self.block_buffer_map.get(&bank_id) {
                let slot = block.slot;
                let parent_slot = self.bank_parent_slot.get(&bank_id).copied();

                match (block.can_be_optimistic_frozen(), parent_slot) {
                    (true, Some(parent_slot)) => {
                        let forged_block_summary =
                            block.forge_optimistic_block_summary(parent_slot);
                        tracing::warn!(
                            "Recovered block summary for bank {bank_id} (slot {slot}): {forged_block_summary:?}"
                        );
                        self.handle_block_summary(forged_block_summary)
                            .expect("untracked");
                    }
                    _ => {
                        tracing::error!(
                            "Cannot optimistically freeze bank {bank_id} (slot {slot}) because it has no entries or no known parent"
                        );
                        self.remove_bank_references(bank_id);
                        self.push_to_dlq(DeadletterEvent::Incomplete(bank_id));
                    }
                }
            }
        }
    }

    pub fn process_consensus_event(&mut self, event: ConsensusUpdate) {
        match event {
            ConsensusUpdate::SlotCommitmentStatus(slot_status) => {
                self.handle_slot_commitment_status_update(slot_status);
            }
        }
        self.process_retroactively_rooted_slots();
        self.flush_forks_detected_in_current_tick();
    }

    ///
    /// Removes any reference to `slot` -- and every bank_id ever registered for it -- from all
    /// of the state machine's indexes.
    ///
    fn remove_slot_references_in_state(&mut self, slot: Slot) {
        if let Some(bank_ids) = self.slot_to_banks.remove(&slot) {
            for bank_id in bank_ids {
                self.remove_bank_references(bank_id);
            }
        }
        self.resolved_bank_per_slot.remove(&slot);
        self.slot_min_commitment.remove(&slot);
    }

    ///
    /// Returns the oldest bank instance in the buffer.
    ///
    pub fn oldest_block_in_buffer(&self) -> Option<OldestBufferedBlockInfo> {
        self.block_buffer_map
            .values()
            .max_by_key(|block| block.created_at.elapsed())
            .map(|block| OldestBufferedBlockInfo {
                slot: block.slot,
                bank_id: block.bank_id,
                age: block.created_at.elapsed(),
                parent_slot: self.bank_parent_slot.get(&block.bank_id).copied(),
                pending_slot_status: self
                    .pending_slot_status_update
                    .get(&block.bank_id)
                    .map(|queue| queue.len())
                    .unwrap_or_default(),
            })
    }

    ///
    /// This function will clean out deprecated slot index information from the state machine making sure it does not grow forever.
    ///
    /// Stuff to "deindex" are :
    /// metadata about slot that are finalized -- Since they are finalized we don't need to keep them around anymore.
    /// Forked Slot -- Slot that are forked and we know we will never reach finalized status for them.
    ///
    ///
    /// `deleted`, if provided, is extended with every `bank_id` purged by this pass (not the
    /// slots themselves) -- this is what a downstream payload accumulator (keyed by `bank_id`,
    /// not `Slot`) needs in order to prune content for banks that were never delivered.
    ///
    pub fn gc(&mut self, mut deleted: Option<&mut Vec<BankId>>) -> BlockstoreGCStats {
        self.process_deregister_finalized_block_queue();
        let mut stats = BlockstoreGCStats::default();
        let mut elligible_for_deletion = Vec::with_capacity(self.forks_history.len());
        let mut forks_to_remove = FxHashSet::default();
        self.forks
            .truncate_excess_rooted_slots(&mut forks_to_remove);
        let oldest_rooted_slot = self.forks.oldest_rooted_slot().unwrap_or(0);
        for slot in self.forks_history.iter() {
            // If the oldest rooted slot that we have is bigger than current slot that we are processing than we should
            // have received Finalized status for this slot by now.
            // In other word this slot will never finish.
            if *slot >= oldest_rooted_slot {
                tracing::debug!(
                    "Slot {} cannot be safely evicted from index because it is still part of the fork index memory",
                    slot
                );
                stats.slot_blocked_count += 1;
                continue;
            }

            let has_pending_processed = self.slot_to_banks.get(slot).is_some_and(|ids| {
                ids.iter().any(|bank_id| {
                    self.pending_slot_status_update
                        .get(bank_id)
                        .is_some_and(|q| {
                            q.iter().any(|s| s.commitment == CommitmentLevel::Processed)
                        })
                })
            });
            if has_pending_processed && !forks_to_remove.contains(slot) {
                tracing::debug!(
                    "Slot {} cannot be safely evicted from index because pending Processed slot status",
                    slot
                );
                stats.slot_blocked_count += 1;
                continue;
            }
            elligible_for_deletion.push(*slot);
        }
        stats.slot_purge_count = elligible_for_deletion.len();
        let purged: FxHashSet<Slot> = elligible_for_deletion.iter().copied().collect();
        for slot in elligible_for_deletion {
            self.forks_history.remove(&slot);
            if let Some(trace) = deleted.as_mut() {
                if let Some(bank_ids) = self.slot_to_banks.get(&slot) {
                    for &bank_id in bank_ids {
                        trace.push(bank_id);
                    }
                }
            }
            self.remove_slot_references_in_state(slot);
        }
        if !purged.is_empty() {
            self.discarded_bank_ids
                .retain(|_, slot| !purged.contains(slot));
        }
        stats
    }

    ///
    /// Process the deregistered slot from the queue.
    ///
    pub fn process_deregister_finalized_block_queue(&mut self) {
        while let Some(slot) = self.pending_finalized_slot_deregister.pop_front() {
            self.remove_slot_references_in_state(slot);
        }
    }

    ///
    /// Pops the next unprocessed blockstore update from the queue.
    ///
    pub fn pop_next_unprocess_blockstore_update(&mut self) -> Option<BlockStateMachineOutput> {
        let (revision, data) = self.blockstore_update_queue.pop_front()?;
        self.min_history_revision_in_queue = Some(revision + 1);
        // Check if we need to schedule deregister process.
        if let Some(slots) = self.deregister_finalized_slot_schedule.remove(&revision) {
            self.pending_finalized_slot_deregister.extend(slots);
        }
        Some(data)
    }

    pub fn unprocess_blockstore_update_queue_len(&self) -> usize {
        self.blockstore_update_queue.len()
    }

    pub fn pop_next_dlq(&mut self) -> Option<DeadletterEvent> {
        self.dlq.pop_front()
    }
}

pub const fn module_path_for_test() -> &'static str {
    module_path!()
}

#[cfg(test)]
mod tests {
    use {
        crate::state_machine::{
            BlockSummary, BlocksStateMachine, EntryInfo, SlotCommitmentStatusUpdate, SlotLifecycle,
            SlotLifecycleUpdate,
        },
        solana_clock::{BankId, DEFAULT_TICKS_PER_SLOT, Slot},
        solana_commitment_config::CommitmentLevel,
        solana_hash::Hash,
    };

    fn generate_entries(
        slot: Slot,
        bank_id: BankId,
        num_data_entries: u64,
        tx_per_entry: u64,
    ) -> Vec<EntryInfo> {
        assert!(num_data_entries >= DEFAULT_TICKS_PER_SLOT);
        let mut entries = Vec::with_capacity((num_data_entries + DEFAULT_TICKS_PER_SLOT) as usize);
        let tick_entry_module = num_data_entries / DEFAULT_TICKS_PER_SLOT;
        let mut tick_entry_remain = DEFAULT_TICKS_PER_SLOT as usize;
        for i in 0..num_data_entries {
            let start_txn_index = i * tx_per_entry;
            entries.push(EntryInfo {
                slot,
                bank_id,
                entry_index: i,
                starting_txn_index: start_txn_index,
                entry_hash: Hash::new_unique(),
                executed_txn_count: tx_per_entry,
            });

            if i % tick_entry_module == 0 {
                entries.push(EntryInfo {
                    slot,
                    bank_id,
                    entry_index: i + DEFAULT_TICKS_PER_SLOT,
                    starting_txn_index: start_txn_index + tx_per_entry,
                    entry_hash: Hash::new_unique(),
                    executed_txn_count: 0,
                });
                tick_entry_remain -= 1;
            }
        }
        for _ in 0..tick_entry_remain {
            entries.push(EntryInfo {
                slot,
                bank_id,
                entry_index: num_data_entries + DEFAULT_TICKS_PER_SLOT,
                starting_txn_index: num_data_entries * tx_per_entry,
                entry_hash: Hash::new_unique(),
                executed_txn_count: 0,
            });
        }
        entries
    }

    fn created_bank(slot: Slot, parent: Option<Slot>, bank_id: BankId) -> SlotLifecycleUpdate {
        SlotLifecycleUpdate {
            slot,
            parent_slot: parent,
            stage: SlotLifecycle::CreatedBank,
            bank_id: Some(bank_id),
        }
    }

    fn dead(slot: Slot) -> SlotLifecycleUpdate {
        SlotLifecycleUpdate {
            slot,
            parent_slot: None,
            stage: SlotLifecycle::Dead,
            bank_id: None,
        }
    }

    fn commitment(
        slot: Slot,
        parent: Option<Slot>,
        level: CommitmentLevel,
        bank_id: BankId,
    ) -> SlotCommitmentStatusUpdate {
        SlotCommitmentStatusUpdate {
            slot,
            parent_slot: parent,
            commitment: level,
            bank_id,
        }
    }

    /// Drives one bank through CreatedBank + 64 entries + BlockMeta + the given commitment
    /// level, returning the block's blockhash. Doesn't pop any output -- callers do that.
    fn seal_bank(
        sm: &mut BlocksStateMachine,
        slot: Slot,
        parent: Option<Slot>,
        bank_id: BankId,
        commitment_level: CommitmentLevel,
    ) -> Hash {
        sm.process_replay_event(created_bank(slot, parent, bank_id).into())
            .unwrap();
        let entries = generate_entries(slot, bank_id, 64, 10);
        let blockhash = entries.last().unwrap().entry_hash;
        for e in entries {
            sm.process_replay_event(e.into()).unwrap();
        }
        let summary = BlockSummary {
            slot,
            bank_id,
            parent_slot: parent.unwrap_or(0),
            entry_count: 64 + DEFAULT_TICKS_PER_SLOT,
            executed_transaction_count: 640,
            blockhash,
            parent_blockhash: Hash::default(),
            block_time: 0,
        };
        sm.process_replay_event(summary.into()).unwrap();
        sm.process_consensus_event(commitment(slot, parent, commitment_level, bank_id).into());
        blockhash
    }

    #[test]
    fn it_should_handle_all_lifecycle_transition_and_produce_frozen_block() {
        let mut sm = BlocksStateMachine::default();
        let bank_id = 1;

        sm.process_replay_event(created_bank(1, None, bank_id).into())
            .unwrap();
        let entries = generate_entries(1, bank_id, 64, 10);
        let blockhash = entries.last().unwrap().entry_hash;
        for e in entries {
            sm.process_replay_event(e.into()).unwrap();
        }
        let summary = BlockSummary {
            slot: 1,
            bank_id,
            parent_slot: 0,
            entry_count: 64 + DEFAULT_TICKS_PER_SLOT,
            executed_transaction_count: 640,
            blockhash,
            parent_blockhash: Hash::default(),
            block_time: 0,
        };
        sm.process_replay_event(summary.into()).unwrap();
        sm.process_consensus_event(commitment(1, None, CommitmentLevel::Processed, bank_id).into());

        let actual = sm.pop_next_unprocess_blockstore_update();
        assert!(matches!(
            actual,
            Some(super::BlockStateMachineOutput::FrozenBlock(ref blk)) if blk.bank_id == bank_id
        ));
        let actual = sm.pop_next_unprocess_blockstore_update();
        let Some(super::BlockStateMachineOutput::SlotStatus(status)) = actual else {
            panic!("expected slot status update");
        };
        assert_eq!(status.bank_id, bank_id);
        assert_eq!(status.commitment, CommitmentLevel::Processed);
        assert!(sm.pop_next_unprocess_blockstore_update().is_none());
    }

    #[test]
    fn blockstore_gc_should_work_even_when_empty() {
        let mut sm = BlocksStateMachine::default();
        let mut gc_trace = Vec::new();
        let actual = sm.gc(Some(&mut gc_trace));
        assert_eq!(actual.slot_purge_count, 0);
        assert_eq!(actual.slot_blocked_count, 0);
        assert!(gc_trace.is_empty());
    }

    #[test]
    fn blockstore_should_correct_missing_processed_slot_status() {
        let mut sm = BlocksStateMachine::default();
        let bank_id = 1;
        seal_bank(&mut sm, 1, None, bank_id, CommitmentLevel::Confirmed);

        let actual = sm.pop_next_unprocess_blockstore_update().unwrap();
        assert!(matches!(
            actual,
            super::BlockStateMachineOutput::FrozenBlock(_)
        ));

        let super::BlockStateMachineOutput::SlotStatus(status) =
            sm.pop_next_unprocess_blockstore_update().unwrap()
        else {
            panic!("expected slot status update");
        };
        assert_eq!(status.commitment, CommitmentLevel::Processed);

        let super::BlockStateMachineOutput::SlotStatus(status) =
            sm.pop_next_unprocess_blockstore_update().unwrap()
        else {
            panic!("expected slot status update");
        };
        assert_eq!(status.commitment, CommitmentLevel::Confirmed);

        assert!(sm.pop_next_unprocess_blockstore_update().is_none());
    }

    #[test]
    fn it_should_detect_retroactively_rooted_slots() {
        let mut sm = BlocksStateMachine::default();

        seal_bank(&mut sm, 1, None, 100, CommitmentLevel::Processed);
        let actual = sm.pop_next_unprocess_blockstore_update().unwrap();
        assert!(matches!(
            actual,
            super::BlockStateMachineOutput::FrozenBlock(_)
        ));
        let actual = sm.pop_next_unprocess_blockstore_update().unwrap();
        assert!(matches!(
            actual,
            super::BlockStateMachineOutput::SlotStatus(_)
        ));

        // Slot 2 jumps straight to Finalized -- slot 1 (its parent) never got its own
        // Confirmed/Finalized update and must be retroactively rooted through it.
        seal_bank(&mut sm, 2, Some(1), 200, CommitmentLevel::Finalized);

        let mut seen = Vec::new();
        while let Some(update) = sm.pop_next_unprocess_blockstore_update() {
            if let super::BlockStateMachineOutput::SlotStatus(s) = update {
                seen.push((s.slot, s.commitment));
            }
        }
        for level in [
            CommitmentLevel::Processed,
            CommitmentLevel::Confirmed,
            CommitmentLevel::Finalized,
        ] {
            assert!(
                seen.contains(&(2, level)),
                "slot 2 must reach {level:?}: {seen:?}"
            );
        }
        assert!(
            seen.contains(&(1, CommitmentLevel::Confirmed)),
            "slot 1 must be retroactively confirmed: {seen:?}"
        );
        assert!(
            seen.contains(&(1, CommitmentLevel::Finalized)),
            "slot 1 must be retroactively finalized: {seen:?}"
        );
    }

    #[test]
    fn two_banks_for_same_slot_stay_peers_until_one_is_confirmed() {
        let mut sm = BlocksStateMachine::default();
        let slot = 40;
        let bank_a = 4000;
        let bank_b = 4001;

        seal_bank(&mut sm, slot, None, bank_a, CommitmentLevel::Processed);
        seal_bank(&mut sm, slot, None, bank_b, CommitmentLevel::Processed);

        // Both banks are independently frozen and Processed -- true peers, no precedence.
        let mut frozen_banks = Vec::new();
        let mut processed_banks = Vec::new();
        while let Some(update) = sm.pop_next_unprocess_blockstore_update() {
            match update {
                super::BlockStateMachineOutput::FrozenBlock(b) => frozen_banks.push(b.bank_id),
                super::BlockStateMachineOutput::SlotStatus(s) => processed_banks.push(s.bank_id),
                _ => {}
            }
        }
        assert_eq!(frozen_banks, vec![bank_a, bank_b]);
        assert_eq!(processed_banks, vec![bank_a, bank_b]);
        // bank_a tentatively became the slot's resolved bank via single-candidate inference
        // when it froze (before bank_b ever showed up) -- but that's provisional, not a real
        // discard: bank_b is still free to seal and deliver its own Processed update
        // independently (verified above), and the tentative resolution can still be
        // superseded by a later Confirmed/Finalized naming a different bank (verified below).
        assert_eq!(sm.resolved_bank_per_slot.get(&slot), Some(&bank_a));
        assert!(!sm.block_buffer_map.contains_key(&bank_a));
        assert!(!sm.discarded_bank_ids.contains_key(&bank_a));

        // bank_b reaches Confirmed -- it becomes the sole winner, bank_a is discarded.
        sm.process_consensus_event(
            commitment(slot, None, CommitmentLevel::Confirmed, bank_b).into(),
        );

        assert_eq!(sm.resolved_bank_per_slot.get(&slot), Some(&bank_b));
        assert_eq!(sm.discarded_bank_ids.get(&bank_a), Some(&slot));
        assert!(!sm.is_bank_trackable(bank_a));

        let mut winner_updates = Vec::new();
        while let Some(update) = sm.pop_next_unprocess_blockstore_update() {
            if let super::BlockStateMachineOutput::SlotStatus(s) = update {
                winner_updates.push((s.bank_id, s.commitment));
            }
        }
        assert!(winner_updates.contains(&(bank_b, CommitmentLevel::Confirmed)));
        assert!(
            winner_updates.iter().all(|(id, _)| *id == bank_b),
            "no further update should ever name the discarded loser: {winner_updates:?}"
        );

        // A straggler event for the discarded loser must be dropped, not resurrect it.
        sm.process_consensus_event(
            commitment(slot, None, CommitmentLevel::Finalized, bank_a).into(),
        );
        assert!(sm.pop_next_unprocess_blockstore_update().is_none());
    }

    #[test]
    fn two_banks_for_same_slot_can_disagree_on_parent_and_only_the_resolved_bank_feeds_forks() {
        let mut sm = BlocksStateMachine::default();
        let slot = 30;
        let bank_close_parent = 3000;
        let bank_distant_parent = 3001;

        // Two competing banks for the same slot, claiming two different parents. Neither is
        // resolved yet, so Forks must not have registered either claim.
        sm.process_replay_event(created_bank(slot, Some(29), bank_close_parent).into())
            .unwrap();
        sm.process_replay_event(created_bank(slot, Some(27), bank_distant_parent).into())
            .unwrap();
        sm.process_consensus_event(
            commitment(
                slot,
                Some(29),
                CommitmentLevel::Processed,
                bank_close_parent,
            )
            .into(),
        );
        sm.process_consensus_event(
            commitment(
                slot,
                Some(27),
                CommitmentLevel::Processed,
                bank_distant_parent,
            )
            .into(),
        );
        assert!(
            sm.forks.get_parent(&slot).is_none(),
            "an unresolved slot must not have fed any parent claim into Forks yet"
        );

        // The distant-parent bank is the one that actually gets confirmed (e.g. a
        // dump-and-repair replay correcting history) -- only *its* parent must reach Forks.
        sm.process_consensus_event(
            commitment(
                slot,
                Some(27),
                CommitmentLevel::Confirmed,
                bank_distant_parent,
            )
            .into(),
        );
        assert_eq!(
            sm.forks.get_parent(&slot),
            Some(27),
            "Forks must learn the resolved bank's parent, not the discarded sibling's"
        );
        assert_eq!(
            sm.resolved_bank_per_slot.get(&slot),
            Some(&bank_distant_parent)
        );
        assert_eq!(sm.discarded_bank_ids.get(&bank_close_parent), Some(&slot));
    }

    #[test]
    fn confirmed_resolution_cannot_be_superseded() {
        let mut sm = BlocksStateMachine::default();
        let slot = 20;
        let confirmed_bank = 2000;
        let rogue_bank = 2001;

        seal_bank(
            &mut sm,
            slot,
            None,
            confirmed_bank,
            CommitmentLevel::Confirmed,
        );
        while sm.pop_next_unprocess_blockstore_update().is_some() {}

        // A later, different bank_id reporting commitment for the same slot after it has
        // already reached Confirmed must be rejected outright, not allowed to supersede it.
        sm.process_replay_event(created_bank(slot, None, rogue_bank).into())
            .unwrap();
        sm.process_consensus_event(
            commitment(slot, None, CommitmentLevel::Processed, rogue_bank).into(),
        );

        assert_eq!(sm.resolved_bank_per_slot.get(&slot), Some(&confirmed_bank));
        assert!(!sm.discarded_bank_ids.contains_key(&rogue_bank));
        assert!(sm.is_bank_trackable(rogue_bank));
    }

    #[test]
    fn dead_slot_discards_every_bank_registered_for_it() {
        let mut sm = BlocksStateMachine::default();
        let slot = 7;
        let bank_a = 70;
        let bank_b = 71;

        sm.process_replay_event(created_bank(slot, None, bank_a).into())
            .unwrap();
        sm.process_replay_event(created_bank(slot, None, bank_b).into())
            .unwrap();
        assert!(sm.block_buffer_map.contains_key(&bank_a));
        assert!(sm.block_buffer_map.contains_key(&bank_b));

        sm.process_replay_event(dead(slot).into()).unwrap();

        assert!(!sm.block_buffer_map.contains_key(&bank_a));
        assert!(!sm.block_buffer_map.contains_key(&bank_b));
        assert!(!sm.slot_to_banks.contains_key(&slot));
        assert_eq!(sm.discarded_bank_ids.get(&bank_a), Some(&slot));
        assert_eq!(sm.discarded_bank_ids.get(&bank_b), Some(&slot));

        // Marking the slot dead legitimately emits a ForksDetected for it -- drain that before
        // checking that nothing further is produced.
        let update = sm.pop_next_unprocess_blockstore_update().unwrap();
        assert!(matches!(
            update,
            super::BlockStateMachineOutput::ForksDetected(ref f) if f.slot == slot
        ));
        assert!(sm.pop_next_unprocess_blockstore_update().is_none());

        // Late data for either bank after Dead must not resurrect anything.
        sm.process_consensus_event(
            commitment(slot, None, CommitmentLevel::Processed, bank_a).into(),
        );
        assert!(sm.pop_next_unprocess_blockstore_update().is_none());
        assert!(!sm.block_buffer_map.contains_key(&bank_a));
        assert!(!sm.slot_to_banks.contains_key(&slot));
    }
}

///
/// Regression suite for the defects catalogued in `AUDIT.md` at the repository root.
///
/// Every test here asserts the **correct** behaviour, so every one of them **fails against the
/// current implementation**. They are the reproduction cases for the audit: a test flipping to
/// green means that finding is fixed. Each test names the `AUDIT.md` finding it covers.
///
/// Run just this module with:
///
/// ```text
/// cargo test --all-features audit_regression::
/// ```
///
/// To keep a green default suite while these are open, add `#[ignore]` to each test and run them
/// with `cargo test --all-features audit_regression:: -- --ignored`.
///
#[cfg(test)]
mod audit_regression {
    use super::*;

    fn created_bank(slot: Slot, parent: Option<Slot>, bank_id: BankId) -> SlotLifecycleUpdate {
        SlotLifecycleUpdate {
            slot,
            parent_slot: parent,
            stage: SlotLifecycle::CreatedBank,
            bank_id: Some(bank_id),
        }
    }

    fn dead(slot: Slot) -> SlotLifecycleUpdate {
        SlotLifecycleUpdate {
            slot,
            parent_slot: None,
            stage: SlotLifecycle::Dead,
            bank_id: None,
        }
    }

    fn cmt(
        slot: Slot,
        parent: Option<Slot>,
        level: CommitmentLevel,
        bank_id: BankId,
    ) -> SlotCommitmentStatusUpdate {
        SlotCommitmentStatusUpdate {
            slot,
            parent_slot: parent,
            commitment: level,
            bank_id,
        }
    }

    /// `CreatedBank` plus `n` entries and no `BlockMeta`, so the bank is left buffering.
    fn buffer(
        sm: &mut BlocksStateMachine,
        slot: Slot,
        parent: Option<Slot>,
        bank_id: BankId,
        n: u64,
    ) {
        sm.process_replay_event(created_bank(slot, parent, bank_id).into())
            .unwrap();
        for i in 0..n {
            sm.process_replay_event(
                EntryInfo {
                    slot,
                    bank_id,
                    entry_index: i,
                    starting_txn_index: i * 10,
                    entry_hash: Hash::new_unique(),
                    executed_txn_count: 10,
                }
                .into(),
            )
            .unwrap();
        }
    }

    /// `buffer` plus the `BlockMeta` that freezes the bank.
    fn seal(sm: &mut BlocksStateMachine, slot: Slot, parent: Option<Slot>, bank_id: BankId) {
        buffer(sm, slot, parent, bank_id, 4);
        sm.process_replay_event(
            BlockSummary {
                slot,
                bank_id,
                parent_slot: parent.unwrap_or(0),
                entry_count: 4,
                executed_transaction_count: 40,
                blockhash: Hash::new_unique(),
                parent_blockhash: Hash::new_unique(),
                block_time: 1_700_000_000,
            }
            .into(),
        )
        .unwrap();
    }

    fn drain(sm: &mut BlocksStateMachine) -> Vec<BlockStateMachineOutput> {
        let mut v = Vec::new();
        while let Some(o) = sm.pop_next_unprocess_blockstore_update() {
            v.push(o);
        }
        v
    }

    fn frozen_bank_ids(outputs: &[BlockStateMachineOutput]) -> Vec<BankId> {
        outputs
            .iter()
            .filter_map(|o| match o {
                BlockStateMachineOutput::FrozenBlock(b) => Some(b.bank_id),
                _ => None,
            })
            .collect()
    }

    fn statuses(outputs: &[BlockStateMachineOutput]) -> Vec<(Slot, CommitmentLevel)> {
        outputs
            .iter()
            .filter_map(|o| match o {
                BlockStateMachineOutput::SlotStatus(s) => Some((s.slot, s.commitment)),
                _ => None,
            })
            .collect()
    }

    fn fork_reports(outputs: &[BlockStateMachineOutput]) -> Vec<(Slot, Vec<BankId>)> {
        outputs
            .iter()
            .filter_map(|o| match o {
                BlockStateMachineOutput::ForksDetected(f) => Some((f.slot, f.bank_ids.clone())),
                _ => None,
            })
            .collect()
    }

    ///
    /// AUDIT.md finding 1: optimistic freeze fabricates blocks for sibling banks.
    ///
    /// A bank at the parent slot that never received a `BlockMeta` is not a late winner, it is a
    /// competing instance awaiting discard. Freezing it forges a blockhash from the last entry
    /// hash and invents a zero `parent_blockhash` and a zero `block_time`.
    ///
    #[test]
    fn audit_1_no_fabricated_block_for_parent_slot_sibling() {
        let mut sm = BlocksStateMachine::default();
        // Parent slot 10 has two banks. 1000 seals for real, 1001 only ever buffers.
        sm.process_replay_event(created_bank(10, Some(9), 1000).into())
            .unwrap();
        sm.process_replay_event(created_bank(10, Some(9), 1001).into())
            .unwrap();
        seal(&mut sm, 10, Some(9), 1000);
        buffer(&mut sm, 10, Some(9), 1001, 3);
        assert!(
            sm.block_buffer_map.contains_key(&1001),
            "precondition: bank 1001 is still buffering"
        );
        drain(&mut sm);

        // A child at slot 11 whose parent is slot 10 receives its own BlockMeta.
        seal(&mut sm, 11, Some(10), 1100);
        let outputs = drain(&mut sm);

        let ids = frozen_bank_ids(&outputs);
        assert!(
            !ids.contains(&1001),
            "bank 1001 never received a BlockMeta and must not be frozen, got frozen banks {ids:?}"
        );
    }

    ///
    /// AUDIT.md finding 1, positive case: legitimate optimistic-freeze recovery still works, and
    /// now identifies the true parent by content -- its own would-be blockhash matching the
    /// child's declared `parent_blockhash` -- rather than by consensus resolution. That means it
    /// recovers even *before* the parent slot has resolved, which a resolution-gated fix could
    /// not do.
    ///
    #[test]
    fn audit_1b_optimistic_freeze_still_recovers_the_real_parent_by_hash() {
        let mut sm = BlocksStateMachine::default();
        let parent_bank = 2000;
        let last_hash = Hash::new_unique();

        // Parent slot 20 has a single bank that received all its entries but never got a
        // BlockMeta -- the classic recovery scenario -- and slot 20 is NOT yet resolved (no
        // commitment update at all was ever seen for it).
        sm.process_replay_event(created_bank(20, Some(19), parent_bank).into())
            .unwrap();
        for i in 0..3u64 {
            let hash = if i == 2 {
                last_hash
            } else {
                Hash::new_unique()
            };
            sm.process_replay_event(
                EntryInfo {
                    slot: 20,
                    bank_id: parent_bank,
                    entry_index: i,
                    starting_txn_index: i * 10,
                    entry_hash: hash,
                    executed_txn_count: 10,
                }
                .into(),
            )
            .unwrap();
        }
        assert!(
            !sm.resolved_bank_per_slot.contains_key(&20),
            "precondition: slot 20 is not yet resolved by any consensus signal"
        );

        // A child at slot 21 arrives whose BlockMeta names `last_hash` as its parent_blockhash --
        // exactly what the parent bank's own last entry hash would freeze to.
        sm.process_replay_event(created_bank(21, Some(20), 2100).into())
            .unwrap();
        sm.process_replay_event(
            EntryInfo {
                slot: 21,
                bank_id: 2100,
                entry_index: 0,
                starting_txn_index: 0,
                entry_hash: Hash::new_unique(),
                executed_txn_count: 1,
            }
            .into(),
        )
        .unwrap();
        sm.process_replay_event(
            BlockSummary {
                slot: 21,
                bank_id: 2100,
                parent_slot: 20,
                entry_count: 1,
                executed_transaction_count: 1,
                blockhash: Hash::new_unique(),
                parent_blockhash: last_hash,
                block_time: 1,
            }
            .into(),
        )
        .unwrap();

        let ids = frozen_bank_ids(&drain(&mut sm));
        assert!(
            ids.contains(&parent_bank),
            "the parent bank's own last entry hash matches the child's declared              parent_blockhash, so it must be recovered by optimistic freeze even though slot 20              never resolved, got frozen banks {ids:?}"
        );
    }

    ///
    /// AUDIT.md finding 2: retroactive rooting drops a slot's entire commitment delivery.
    ///
    /// A slot with two competing banks stays unresolved by design. Rooting it through a finalized
    /// child must still deliver its commitment levels rather than discard the slot.
    ///
    #[test]
    fn audit_2_retroactively_rooted_slot_still_gets_its_commitment() {
        let mut sm = BlocksStateMachine::default();
        // Both banks registered before either freezes, so slot 1 never resolves.
        sm.process_replay_event(created_bank(1, Some(0), 100).into())
            .unwrap();
        sm.process_replay_event(created_bank(1, Some(0), 101).into())
            .unwrap();
        seal(&mut sm, 1, Some(0), 100);
        seal(&mut sm, 1, Some(0), 101);
        sm.process_consensus_event(cmt(1, Some(0), CommitmentLevel::Processed, 100).into());
        sm.process_consensus_event(cmt(1, Some(0), CommitmentLevel::Processed, 101).into());
        assert!(
            !sm.resolved_bank_per_slot.contains_key(&1),
            "precondition: slot 1 is unresolved"
        );
        drain(&mut sm);

        // The child finalizes, which retroactively roots slot 1.
        seal(&mut sm, 2, Some(1), 200);
        sm.process_consensus_event(cmt(2, Some(1), CommitmentLevel::Finalized, 200).into());
        let outputs = drain(&mut sm);

        let seen = statuses(&outputs);
        assert!(
            sm.forks.is_rooted_slot(&1),
            "precondition: Forks rooted slot 1"
        );
        assert!(
            seen.contains(&(1, CommitmentLevel::Finalized)),
            "slot 1 was rooted so it must receive Finalized, got {seen:?}"
        );
    }

    ///
    /// AUDIT.md finding 3: a gap-filled Finalized schedules slot teardown three times.
    ///
    /// `deliver_commitment` tests the outer `commitment` rather than `update.commitment`, so every
    /// synthesized level takes the Finalized branch. Teardown must be scheduled once, on the
    /// revision of the genuine Finalized update.
    ///
    #[test]
    fn audit_3_gapfill_schedules_teardown_once_at_the_finalized_revision() {
        let mut sm = BlocksStateMachine::default();
        seal(&mut sm, 1, Some(0), 100);
        sm.process_consensus_event(cmt(1, Some(0), CommitmentLevel::Finalized, 100).into());

        // Revisions: 0 = FrozenBlock, 1 = Processed, 2 = Confirmed, 3 = Finalized.
        let mut sched: Vec<(Revision, Vec<Slot>)> = sm
            .deregister_finalized_slot_schedule
            .iter()
            .map(|(k, v)| (*k, v.clone()))
            .collect();
        sched.sort();
        assert_eq!(
            sched,
            vec![(3, vec![1])],
            "teardown must be scheduled once, on the Finalized revision only"
        );
    }

    ///
    /// AUDIT.md finding 3, first cascade: premature teardown lets an already-delivered bank
    /// re-freeze, emitting a second `FrozenBlock` for the same bank instance.
    ///
    #[test]
    fn audit_3a_duplicate_block_meta_is_still_rejected_after_partial_drain() {
        let mut sm = BlocksStateMachine::default();
        seal(&mut sm, 1, Some(0), 100);
        sm.process_consensus_event(cmt(1, Some(0), CommitmentLevel::Finalized, 100).into());

        // A consumer pops the block and only the synthesized Processed status.
        assert!(matches!(
            sm.pop_next_unprocess_blockstore_update().unwrap(),
            BlockStateMachineOutput::FrozenBlock(_)
        ));
        let BlockStateMachineOutput::SlotStatus(s) =
            sm.pop_next_unprocess_blockstore_update().unwrap()
        else {
            panic!("expected a slot status");
        };
        assert_eq!(s.commitment, CommitmentLevel::Processed);
        assert_eq!(
            sm.unprocess_blockstore_update_queue_len(),
            2,
            "precondition: Confirmed and Finalized are still unread"
        );
        sm.gc(None);

        let dup = sm.process_replay_event(
            BlockSummary {
                slot: 1,
                bank_id: 100,
                parent_slot: 0,
                entry_count: 4,
                executed_transaction_count: 40,
                blockhash: Hash::new_unique(),
                parent_blockhash: Hash::new_unique(),
                block_time: 1,
            }
            .into(),
        );
        assert!(
            dup.is_err(),
            "a duplicate BlockMeta for the already-frozen bank 100 must be rejected"
        );
        let ids = frozen_bank_ids(&drain(&mut sm));
        assert!(
            ids.is_empty(),
            "bank 100 was already delivered and must not be re-emitted, got {ids:?}"
        );
    }

    ///
    /// AUDIT.md finding 3, second cascade: premature teardown erases the commitment floor, so the
    /// "Confirmed and Finalized are final" guard can no longer fire and a rogue bank supersedes a
    /// finalized slot.
    ///
    #[test]
    fn audit_3b_finalized_resolution_survives_a_rogue_bank() {
        let mut sm = BlocksStateMachine::default();
        seal(&mut sm, 1, Some(0), 100);
        sm.process_consensus_event(cmt(1, Some(0), CommitmentLevel::Finalized, 100).into());
        sm.pop_next_unprocess_blockstore_update().unwrap(); // FrozenBlock
        sm.pop_next_unprocess_blockstore_update().unwrap(); // synthesized Processed
        sm.gc(None);

        // A different bank claims Confirmed for the already-finalized slot 1.
        sm.process_consensus_event(cmt(1, Some(99), CommitmentLevel::Confirmed, 101).into());

        assert_eq!(
            sm.resolved_bank_per_slot.get(&1),
            Some(&100),
            "slot 1 reached Finalized on bank 100, so that resolution is final"
        );
        assert_eq!(
            sm.forks.get_parent(&1),
            Some(0),
            "Forks must keep the finalized bank's parent, not the rogue bank's"
        );
    }

    ///
    /// AUDIT.md finding 4: superseding double-feeds `Forks` and leaves a stale forward parent edge.
    ///
    /// The slot ends up recorded as a child of two different parents. Marking the abandoned parent
    /// dead then walks the stale edge and reports the canonical confirmed slot as a fork, which
    /// makes the stream layer prune a valid block.
    ///
    #[test]
    fn audit_4_supersede_does_not_leave_a_stale_fork_edge() {
        let mut sm = BlocksStateMachine::default();
        // Bank 3000 is the sole candidate, claims parent 29, and freezing it resolves the slot by
        // sole-candidate inference, feeding Forks the edge from 29.
        seal(&mut sm, 30, Some(29), 3000);
        assert_eq!(
            sm.forks.get_parent(&30),
            Some(29),
            "precondition: Forks learned parent 29"
        );

        // A genuine second bank arrives claiming parent 27 and reaches Confirmed.
        sm.process_replay_event(created_bank(30, Some(27), 3001).into())
            .unwrap();
        sm.process_consensus_event(cmt(30, Some(27), CommitmentLevel::Confirmed, 3001).into());
        assert_eq!(
            sm.forks.get_parent(&30),
            Some(27),
            "precondition: the resolved parent is now 27"
        );
        drain(&mut sm);

        // Slot 29 turns out to be dead, which is why the repair re-parented to 27.
        sm.process_replay_event(dead(29).into()).unwrap();
        let reported: Vec<Slot> = fork_reports(&drain(&mut sm))
            .into_iter()
            .map(|(s, _)| s)
            .collect();

        assert!(
            !reported.contains(&30),
            "slot 30 is the canonical Confirmed slot and must not be reported as a fork, got {reported:?}"
        );
    }

    ///
    /// AUDIT.md finding 5: a dead slot's `ForkDetected` loses its bank ids.
    ///
    /// `mark_slot_as_dead` erases `slot_to_banks` before the flush rebuilds `bank_ids` from it, so
    /// the stream layer's prune loop iterates zero times and the banks keep every buffered event.
    ///
    #[test]
    fn audit_5_dead_slot_event_carries_its_bank_ids() {
        let mut sm = BlocksStateMachine::default();
        buffer(&mut sm, 7, Some(6), 70, 2);
        buffer(&mut sm, 7, Some(6), 71, 2);
        sm.process_replay_event(dead(7).into()).unwrap();

        let reports = fork_reports(&drain(&mut sm));
        let announced: Vec<BankId> = reports
            .iter()
            .filter(|(s, _)| *s == 7)
            .flat_map(|(_, ids)| ids.iter().copied())
            .collect();

        assert!(
            announced.contains(&70) && announced.contains(&71),
            "dead slot 7 had banks 70 and 71, both must be announced so downstream can prune, got {announced:?}"
        );
    }

    ///
    /// AUDIT.md finding 6: discarded loser banks reach no prune path.
    ///
    /// `discard_losing_banks` drops the loser from `slot_to_banks`, the only source `gc` builds its
    /// trace from, and pushes no deadletter event, so the accumulator holds the payload forever.
    ///
    #[test]
    fn audit_6_discarded_loser_is_announced_for_pruning() {
        let mut sm = BlocksStateMachine::default();
        sm.process_replay_event(created_bank(5, Some(4), 500).into())
            .unwrap();
        sm.process_replay_event(created_bank(5, Some(4), 501).into())
            .unwrap();
        buffer(&mut sm, 5, Some(4), 500, 3); // the loser accumulates payload and never seals
        seal(&mut sm, 5, Some(4), 501);
        drain(&mut sm);

        sm.process_consensus_event(cmt(5, Some(4), CommitmentLevel::Confirmed, 501).into());
        assert!(
            sm.discarded_bank_ids.contains_key(&500),
            "precondition: bank 500 lost and was discarded"
        );

        let mut gc_trace = Vec::new();
        for _ in 0..5 {
            sm.gc(Some(&mut gc_trace));
        }
        let mut dlq = Vec::new();
        while let Some(DeadletterEvent::Incomplete(bank_id)) = sm.pop_next_dlq() {
            dlq.push(bank_id);
        }

        assert!(
            gc_trace.contains(&500) || dlq.contains(&500),
            "the discarded loser must reach some prune path, gc trace {gc_trace:?} and dlq {dlq:?} name neither"
        );
    }

    ///
    /// AUDIT.md finding 7: unresolved slots are invisible to garbage collection.
    ///
    /// `gc` iterates `forks_history` only, and nothing enters the fork graph except a resolved
    /// bank's parent. This is what a stream restart mid-block looks like.
    ///
    #[test]
    fn audit_7_unresolved_slot_state_is_eventually_reclaimed() {
        let mut sm = BlocksStateMachine::default();
        for slot in 1000..1010u64 {
            let bank_id = slot + 500_000;
            for level in [CommitmentLevel::Processed, CommitmentLevel::Confirmed] {
                sm.process_consensus_event(cmt(slot, Some(slot - 1), level, bank_id).into());
            }
        }
        assert!(
            drain(&mut sm).is_empty(),
            "precondition: none of these banks ever froze, so nothing is emitted"
        );

        for _ in 0..25 {
            sm.gc(None);
        }

        assert_eq!(
            sm.pending_slot_status_update.len(),
            0,
            "queued statuses for banks that never froze must not be retained forever"
        );
        assert_eq!(
            sm.slot_to_banks.len(),
            0,
            "slot indexes for unresolved, abandoned slots must not be retained forever"
        );
    }

    ///
    /// AUDIT.md finding 8: events for discarded banks return `Ok`.
    ///
    /// `stream.rs` gates `insert_into_storage` on this `Result`, so an `Ok` makes the accumulator
    /// auto-vivify a fresh buffer for a bank the state machine has already given up on.
    ///
    #[test]
    fn audit_8_events_for_discarded_banks_are_rejected() {
        let mut sm = BlocksStateMachine::default();
        buffer(&mut sm, 7, Some(6), 70, 2);
        sm.process_replay_event(dead(7).into()).unwrap();
        assert!(
            sm.discarded_bank_ids.contains_key(&70),
            "precondition: bank 70 was discarded by Dead"
        );
        drain(&mut sm);

        let entry = sm.process_replay_event(
            EntryInfo {
                slot: 7,
                bank_id: 70,
                entry_index: 9,
                starting_txn_index: 90,
                entry_hash: Hash::new_unique(),
                executed_txn_count: 1,
            }
            .into(),
        );
        let meta = sm.process_replay_event(
            BlockSummary {
                slot: 7,
                bank_id: 70,
                parent_slot: 6,
                entry_count: 1,
                executed_transaction_count: 1,
                blockhash: Hash::new_unique(),
                parent_blockhash: Hash::new_unique(),
                block_time: 1,
            }
            .into(),
        );

        assert!(
            entry.is_err(),
            "a straggler entry for discarded bank 70 must be rejected so the stream stores nothing"
        );
        assert!(
            meta.is_err(),
            "a straggler BlockMeta for discarded bank 70 must be rejected so the stream stores nothing"
        );
    }

    ///
    /// AUDIT.md finding 9: `BlockStateMachineOutput::DeadSlotDetected` has no construction site, so
    /// the publicly documented `BlockStreamEvent::DeadBlockDetected` can never fire. A dead slot is
    /// currently indistinguishable from an ordinary fork.
    ///
    #[test]
    fn audit_9_dead_slot_emits_a_dead_slot_output() {
        let mut sm = BlocksStateMachine::default();
        buffer(&mut sm, 7, Some(6), 70, 2);
        sm.process_replay_event(dead(7).into()).unwrap();

        let outputs = drain(&mut sm);
        let dead_reports: Vec<Slot> = outputs
            .iter()
            .filter_map(|o| match o {
                BlockStateMachineOutput::DeadSlotDetected(d) => Some(d.slot),
                _ => None,
            })
            .collect();

        assert!(
            dead_reports.contains(&7),
            "a Dead lifecycle update must produce DeadSlotDetected, got {} output(s) and none of them dead",
            outputs.len()
        );
    }

    ///
    /// AUDIT.md finding 11: `FrozenBlock::entries` is built from `FxHashMap::values`, so its order
    /// is nondeterministic rather than sorted by `entry_index`.
    ///
    #[test]
    fn audit_11_frozen_block_entries_are_ordered_by_entry_index() {
        let mut sm = BlocksStateMachine::default();
        let bank_id = 8000;
        sm.process_replay_event(created_bank(80, Some(79), bank_id).into())
            .unwrap();
        // Deliberately out of order on the wire.
        for i in [5u64, 0, 3, 1, 4, 2] {
            sm.process_replay_event(
                EntryInfo {
                    slot: 80,
                    bank_id,
                    entry_index: i,
                    starting_txn_index: i * 10,
                    entry_hash: Hash::new_unique(),
                    executed_txn_count: 10,
                }
                .into(),
            )
            .unwrap();
        }
        sm.process_replay_event(
            BlockSummary {
                slot: 80,
                bank_id,
                parent_slot: 79,
                entry_count: 6,
                executed_transaction_count: 60,
                blockhash: Hash::new_unique(),
                parent_blockhash: Hash::new_unique(),
                block_time: 1,
            }
            .into(),
        )
        .unwrap();

        let outputs = drain(&mut sm);
        let block = outputs
            .iter()
            .find_map(|o| match o {
                BlockStateMachineOutput::FrozenBlock(b) if b.bank_id == bank_id => Some(b),
                _ => None,
            })
            .expect("bank 8000 froze");

        let indexes: Vec<u64> = block.entries.iter().map(|e| e.entry_index).collect();
        assert_eq!(
            indexes,
            vec![0, 1, 2, 3, 4, 5],
            "a frozen block's entries must be ordered by entry_index"
        );
    }
}
