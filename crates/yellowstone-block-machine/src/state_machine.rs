use {
    crate::forks::{Forks, ForksMutationTracer},
    derive_more::From,
    rustc_hash::{FxHashMap, FxHashSet},
    serde::{Deserialize, Serialize},
    solana_clock::Slot,
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
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SlotLifecycleUpdate {
    pub parent_slot: Option<Slot>,
    pub slot: Slot,
    pub stage: SlotLifecycle,
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
    pub block_time: i64,
}

// #[derive(Debug, From)]
// pub enum BlockstoreInputEvent {
//     Entry(EntryInfo),
//     SlotCommitmentStatus(SlotCommitmentStatusUpdate),
//     SlotLifecycleStatus(SlotLifecycleUpdate),
//     BlockSummary(BlockSummary),
// }

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
}

///
/// Block under construction
///
#[derive(Debug)]
pub struct Block {
    pub slot: Slot,
    entries: FxHashMap<u64, EntryInfo>,
    entry_cnt: u64,
    tick_entry_cnt: u64,
    created_at: std::time::Instant,
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq)]
pub struct FrozenBlock {
    pub slot: Slot,
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
    pub block_time: i64,
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
    pub fn new_with_clock(slot: Slot, clock: Instant) -> Self {
        Self {
            slot,
            entries: Default::default(),
            created_at: clock,
            entry_cnt: 0,
            tick_entry_cnt: 0,
        }
    }

    pub fn new(slot: Slot) -> Self {
        Self::new_with_clock(slot, Instant::now())
    }

    fn last_entry_hash(&self) -> Option<Hash> {
        self.entries
            .get(&(self.entry_cnt - 1))
            .map(|entry| entry.entry_hash)
    }

    fn freeze(self, summary: &BlockSummary) -> FrozenBlock {
        FrozenBlock {
            slot: self.slot,
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
            parent_slot,
            entry_count: self.entry_cnt,
            executed_transaction_count: self.entries.values().map(|e| e.executed_txn_count).sum(),
            blockhash: self.last_entry_hash().expect("last entry hash"),
            // Not derivable without a wire `BlockMeta` -- this path forges a summary before one
            // has arrived, so these are left at their "not reported" defaults.
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

///
/// Block State machine
///
/// This state machine is responsible for buffering blockstore events and provide a way to consume them in a controlled manner.
///
/// The entire blockstore state is logic is SANS-IO and must stay like this to ensure that we can test it properly.
///
///
/// IMPORTANT: SANS-IO means there is no IO operations in this state machine or async function that include channel communication.
///
/// Is to the developer to implement the IO part of the state machine by implementing your own "driver".
///
#[derive(Debug)]
pub struct BlocksStateMachine {
    /// Holds block under construction, not yet frozen
    block_buffer_map: FxHashMap<Slot, Block>,

    /// Holds blocks that are frozen
    frozen_block_index: FxHashMap<Slot, FxHashSet<CommitmentLevel>>,

    /// Maps when it is safe to "deregister" slot.
    /// Deregistering a slot is removing all index data about this slot as it cannot be reference by downstream consumer.
    /// We handle finalized block separately since they are the easiest to deregister.
    deregister_finalized_slot_schedule: FxHashMap<Revision, Vec<Slot>>,

    /// Data from deregister_slot_schedule goes into a queue since we may want to process it in the future.
    pending_finalized_slot_deregister: VecDeque<Slot>,

    /// We queue slot status update if the slot is not yet in the frozen block index.
    /// This is to provide nice guarantee such that you will read the entire block
    /// before any slot status update.
    pending_slot_status_update: FxHashMap<Slot, VecDeque<SlotCommitmentStatusUpdate>>,

    /// Represents the passage of time when blockstore update occurs.
    /// this can be useful to track how things are old or schedule things in future revision.
    revision: Revision,

    /// Keep track of the minimum revision [`blockstore_update_queue`].
    min_history_revision_in_queue: Option<usize>,

    /// Update queue for blockstore events.
    blockstore_update_queue: VecDeque<(usize, BlockStateMachineOutput)>,

    /// Maintain forks history of the blockchain.    
    pub forks: Forks<Slot>,
    forks_history: FxHashSet<Slot>,

    ///
    /// Keep track of the slots that were detected as forks in the current tick.
    ///
    forks_detected_in_current_tick: FxHashSet<Slot>,

    /// Holds deadletter queue message for blockstore events that cannot be processed.
    dlq: VecDeque<DeadletterEvent>,

    /// Keep track of the age of the slot (when the [`Block`] was first created in the event history).
    slot_age: FxHashMap<Slot, Revision>,

    /// Keep track of the maximum revision event referenced by slot.
    slot_max_version_referenced: FxHashMap<Slot, Revision>,

    /// Dead blocks are blocks who will never be frozen.
    /// This can happen when we boot fumarole initially : some block we receive slot status update before the block data.
    dead_blocks_queue: VecDeque<Slot>,

    ///
    /// Buffers slots that were retroactively rooted by a slot status update.
    ///
    retroactively_rooted_slots: FxHashSet<Slot>,

    ///
    /// Keep track of slots that need optimistic freeze because their child slot was frozen before them.
    /// When this happen, it probably means that either agave has a bug or the geyser plugin is buggy.
    ///
    need_optimistic_freeze: FxHashSet<Slot>,
}

#[derive(Debug)]
pub enum DeadletterEvent {
    Incomplete(Slot),
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
}

#[derive(Debug, Clone)]
pub struct DeadBlockDetected {
    pub slot: Slot,
}

#[derive(Debug)]
pub enum BlockStateMachineOutput {
    FrozenBlock(FrozenBlock),
    SlotStatus(SlotCommitmentStatusUpdate),
    ForksDetected(ForkDetected),
    DeadSlotDetected(DeadBlockDetected),
    BankCreated(Slot),
    /// Given that Anza did not expose any information about the duplicate-unconfirmed-slot scenario, we have to assume that it can happen and
    /// the only way to detect it is via a second BankCreated event for the same slot number.
    BankReset(Slot),
}

impl BlockStateMachineOutput {
    pub fn slot(&self) -> Slot {
        match self {
            Self::DeadSlotDetected(blk) => blk.slot,
            Self::FrozenBlock(blk) => blk.slot,
            Self::SlotStatus(update) => update.slot,
            Self::ForksDetected(info) => info.slot,
            Self::BankCreated(slot) => *slot,
            Self::BankReset(slot) => *slot,
        }
    }
}

///
/// Stats produce during [`BlockstoreSM::gc`] operation.
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

fn cmp_commitment_level(a: &CommitmentLevel, b: &CommitmentLevel) -> std::cmp::Ordering {
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
/// Occurred when a replay event is rejected by the state machine because it relates to a slot that cannot be tracked by the state machine.
///
#[derive(Debug, thiserror::Error)]
#[error("replay event rejected")]
pub struct UntrackedSlot;

impl BlocksStateMachine {
    ///
    /// Creates a new blockstore instance
    ///
    pub fn new() -> Self {
        Self {
            block_buffer_map: Default::default(),
            frozen_block_index: Default::default(),
            pending_slot_status_update: Default::default(),
            blockstore_update_queue: VecDeque::with_capacity(1000),
            revision: 0,
            min_history_revision_in_queue: Default::default(),
            dlq: Default::default(),
            slot_age: Default::default(),
            slot_max_version_referenced: Default::default(),
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

    fn next_history_revision(&mut self) -> usize {
        let temp = self.revision;
        self.revision += 1;
        temp
    }

    fn push_new_update(&mut self, update: BlockStateMachineOutput) -> Revision {
        let new_revision = self.next_history_revision();
        let slot = update.slot();
        let max_revision = self
            .slot_max_version_referenced
            .entry(slot)
            .or_insert(new_revision);
        *max_revision = std::cmp::max(*max_revision, new_revision);
        self.blockstore_update_queue
            .push_back((new_revision, update));
        new_revision
    }

    /// Pending slot update are pending because the slot is not yet in the frozen block index.
    fn flush_pending_slot_status_update(&mut self, slot: Slot) {
        if let Some(updates) = self.pending_slot_status_update.remove(&slot) {
            for mut slot_status in updates {
                if slot_status.parent_slot.is_none() {
                    if let Some(parent) = self.forks.get_parent(&slot_status.slot) {
                        slot_status.parent_slot = Some(parent);
                    }
                }
                self.handle_slot_commitment_status_update(slot_status);
            }
        }
    }

    pub fn is_slot_tracked(&self, slot: Slot) -> bool {
        self.frozen_block_index.contains_key(&slot) || self.block_buffer_map.contains_key(&slot)
    }

    fn handle_slot_lifecyle_status(
        &mut self,
        slot_lifecycle_status: SlotLifecycleUpdate,
    ) -> Result<(), UntrackedSlot> {
        let slot = slot_lifecycle_status.slot;

        if let Some(parent) = slot_lifecycle_status.parent_slot {
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

        match slot_lifecycle_status.stage {
            SlotLifecycle::FirstShredReceived => {
                tracing::trace!("First shred received for slot {}", slot);
                match self.block_buffer_map.entry(slot) {
                    std::collections::hash_map::Entry::Vacant(vacant_entry) => {
                        let block = Block::new(slot);
                        vacant_entry.insert(block);
                    }
                    _ => {
                        // Maybe duplicate events
                    }
                }
            }
            SlotLifecycle::CreatedBank => {
                tracing::trace!("Bank created for slot {}", slot);
                // In case of duplicate unconfirmed slot (replay of old slot), we may receive multiple "CreatedBank" event for the same slot.
                if self
                    .block_buffer_map
                    .insert(slot, Block::new(slot))
                    .is_none()
                {
                    // New bank created
                    if self.frozen_block_index.contains_key(&slot) {
                        self.push_new_update(BlockStateMachineOutput::BankReset(slot));
                    } else {
                        self.push_new_update(BlockStateMachineOutput::BankCreated(slot));
                    }
                } else {
                    self.push_new_update(BlockStateMachineOutput::BankReset(slot));
                }
                if let Some(pending) = self.pending_slot_status_update.get_mut(&slot) {
                    pending
                        .retain(|slot_status| slot_status.commitment != CommitmentLevel::Processed);
                }
                if let Some(visited_commitment) = self.frozen_block_index.get_mut(&slot) {
                    visited_commitment.remove(&CommitmentLevel::Processed);
                }
            }
            SlotLifecycle::Completed => {
                if !self.block_buffer_map.contains_key(&slot) {
                    tracing::trace!("Slot {} is not in the block buffer map, skipping", slot);
                    return Err(UntrackedSlot);
                }
            }
            SlotLifecycle::Dead => {
                self.mark_block_as_dead(slot);
            }
        }
        Ok(())
    }

    fn handle_slot_commitment_status_update(
        &mut self,
        mut slot_status: SlotCommitmentStatusUpdate,
    ) {
        let slot = slot_status.slot;

        if let Some(parent) = slot_status.parent_slot {
            let mut multitrace = LongShortForksMutationTracer {
                long: &mut self.forks_history,
                short: &mut self.forks_detected_in_current_tick,
            };
            self.forks.add_slot_with_parent_with_rooted_trace(
                slot,
                parent,
                &mut multitrace,
                &mut self.retroactively_rooted_slots,
            );
        } else {
            // If for some reason we already know the parent, just inject it into the slot status.
            if let Some(parent) = self.forks.get_parent(&slot_status.slot) {
                slot_status.parent_slot = Some(parent);
            }
        }

        if !self.frozen_block_index.contains_key(&slot)
            && !self.block_buffer_map.contains_key(&slot)
        {
            return;
        }

        match self.frozen_block_index.get_mut(&slot_status.slot) {
            Some(visited_commitment) => {
                let commitment = slot_status.commitment;
                let mut slot_status_to_push = vec![];
                for commitment2 in iter_to_commitment(&commitment) {
                    if visited_commitment.insert(commitment2) {
                        slot_status_to_push.push(SlotCommitmentStatusUpdate {
                            parent_slot: slot_status.parent_slot,
                            slot,
                            commitment: commitment2,
                        });
                    }
                }
                // This code handle a tricky part where slot status update of higher level commitment level may arrive
                // before lower level commitment.
                // By iterating from lower to higher level commitment, we can ensure that we don't miss any slot status update.
                // Why? Dragonsmouth is already doing something similar, I do it here to ensure that we don't miss any slot status update
                // if we change data source that may not guarantee the order of slot status update.
                for slot_status2 in slot_status_to_push {
                    let revision =
                        self.push_new_update(BlockStateMachineOutput::SlotStatus(slot_status2));
                    tracing::debug!(
                        "Slot status update for slot {} at revision {}",
                        slot,
                        revision
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
            _ => {
                if self.block_buffer_map.contains_key(&slot) {
                    self.pending_slot_status_update
                        .entry(slot_status.slot)
                        .or_default()
                        .push_back(slot_status);
                } else {
                    unreachable!("checks at the beginning of the function should prevent this");
                }
            }
        }
    }

    ///
    /// Mark a block as dead.
    ///
    fn mark_block_as_dead(&mut self, slot: Slot) {
        let mut multitrace = LongShortForksMutationTracer {
            long: &mut self.forks_history,
            short: &mut self.forks_detected_in_current_tick,
        };
        self.forks.mark_slot_as_forked(slot, &mut multitrace);
        self.remove_slot_references_in_state(slot);
    }

    fn handle_block_entry_insert(&mut self, data: EntryInfo) -> Result<(), UntrackedSlot> {
        let slot = data.slot;
        let Some(buffer) = self.block_buffer_map.get_mut(&slot) else {
            // If the block container has not been created yet, it means we never received FIRST_SHRED.
            // Therefore we cannot insert the block data.
            return Err(UntrackedSlot);
        };
        buffer.insert_entry(data);
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
            tracing::trace!("Retroactively rooting slot {}", slot);
            self.handle_slot_commitment_status_update(SlotCommitmentStatusUpdate {
                slot,
                parent_slot: self.forks.get_parent(&slot),
                commitment: CommitmentLevel::Finalized,
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
            self.push_new_update(BlockStateMachineOutput::ForksDetected(ForkDetected {
                slot,
            }));
        }
    }

    ///
    /// Freeze block when we receive block summary.
    ///
    fn handle_block_summary(&mut self, block_summary: BlockSummary) -> Result<(), UntrackedSlot> {
        let slot = block_summary.slot;
        let Some(block) = self.block_buffer_map.remove(&slot) else {
            tracing::debug!("Block summary for slot {slot} but no block data found",);
            return Err(UntrackedSlot);
        };

        let frozen_block = block.freeze(&block_summary);
        assert_eq!(slot, frozen_block.slot);

        if let Some(parent) = self.forks.get_parent(&slot) {
            if self.block_buffer_map.contains_key(&parent) {
                tracing::warn!(
                    "Freezing block for slot {} whose parent slot {} is still in the block buffer map",
                    slot,
                    parent
                );
                // This should never happen, but in case it does we will try to optimistically freeze the parent block.
                self.need_optimistic_freeze.insert(parent);
            }
        }

        tracing::debug!("Block frozen for slot {}", slot);
        self.frozen_block_index.entry(slot).or_default();
        self.push_new_update(BlockStateMachineOutput::FrozenBlock(frozen_block));

        if let Some(max_pending_commitment_level) = self
            .pending_slot_status_update
            .get(&slot)
            .iter()
            .flat_map(|update| update.iter())
            .filter(|update| {
                cmp_commitment_level(&update.commitment, &CommitmentLevel::Processed).is_gt()
            })
            .max_by(|x, y| cmp_commitment_level(&x.commitment, &y.commitment))
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
                "Slot {slot} froze after receiving slot status update higher than Processed: {}",
                max_pending_commitment_level.commitment
            );
        }
        self.flush_pending_slot_status_update(slot);
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
        self.execute_optimistic_freeze_for_needed_slots();
        Ok(())
    }

    fn execute_optimistic_freeze_for_needed_slots(&mut self) {
        if self.need_optimistic_freeze.is_empty() {
            return;
        }
        let slots_to_freeze = std::mem::take(&mut self.need_optimistic_freeze);
        for slot in slots_to_freeze {
            // Check if we can freeze the block : we must have some entry to compute the block hash.
            if let Some(block) = self.block_buffer_map.get(&slot) {
                let parent_slot = self.forks.get_parent(&slot);

                match (block.can_be_optimistic_frozen(), parent_slot) {
                    (true, Some(parent_slot)) => {
                        let forged_block_summary =
                            block.forge_optimistic_block_summary(parent_slot);
                        tracing::warn!(
                            "Recoverd block summary for slot {}: {:?}",
                            slot,
                            forged_block_summary
                        );
                        self.handle_block_summary(forged_block_summary)
                            .expect("untracked");
                    }
                    _ => {
                        tracing::error!(
                            "Cannot optimistically freeze slot {} because it has no entries",
                            slot
                        );
                        self.remove_slot_references_in_state(slot);
                        self.push_to_dlq(DeadletterEvent::Incomplete(slot));
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
    /// Make sure the remove any reference to `slot` in all the [`BlockstoreSM`] indexes.
    ///
    fn remove_slot_references_in_state(&mut self, slot: Slot) {
        self.block_buffer_map.remove(&slot);
        self.frozen_block_index.remove(&slot);
        self.pending_slot_status_update.remove(&slot);
        self.slot_max_version_referenced.remove(&slot);
        self.slot_age.remove(&slot);
    }

    ///
    /// Returns the oldest block in the buffer.
    ///
    pub fn oldest_block_in_buffer(&self) -> Option<OldestBufferedBlockInfo> {
        self.block_buffer_map
            .values()
            .max_by_key(|block| block.created_at.elapsed())
            .map(|block| OldestBufferedBlockInfo {
                slot: block.slot,
                age: block.created_at.elapsed(),
                parent_slot: self.forks.get_parent(&block.slot),
                pending_slot_status: self
                    .pending_slot_status_update
                    .get(&block.slot)
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
    pub fn gc(&mut self, mut deleted: Option<&mut Vec<Slot>>) -> BlockstoreGCStats {
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
            if *slot < oldest_rooted_slot {
                elligible_for_deletion.push(*slot);
            } else {
                tracing::debug!(
                    "Slot {} cannot be safely evicted from index because it is still part of the fork index memory",
                    slot
                );
                stats.slot_blocked_count += 1;
                continue;
            }

            if let Some(queue) = self.pending_slot_status_update.get(slot) {
                if queue
                    .iter()
                    .any(|s| s.commitment == CommitmentLevel::Processed)
                    && !forks_to_remove.contains(slot)
                {
                    tracing::debug!(
                        "Slot {} cannot be safely evicted from index because pending Processed slot status",
                        slot
                    );
                    stats.slot_blocked_count += 1;
                    continue;
                }
            }
            elligible_for_deletion.push(*slot);
        }
        stats.slot_purge_count = elligible_for_deletion.len();
        for slot in elligible_for_deletion {
            self.forks_history.remove(&slot);
            self.remove_slot_references_in_state(slot);
            if let Some(trace) = deleted.as_mut() {
                trace.push(slot);
            }
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
        // Check if we need to schedule deregister process.
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

pub fn module_path_for_test() -> &'static str {
    module_path!()
}

#[cfg(test)]
mod tests {
    use {
        crate::state_machine::{
            BlockStateMachineOutput, BlockSummary, EntryInfo, SlotCommitmentStatusUpdate,
            SlotLifecycle, SlotLifecycleUpdate, iter_to_commitment,
        },
        solana_clock::{DEFAULT_TICKS_PER_SLOT, Slot},
        solana_commitment_config::CommitmentLevel,
        solana_hash::Hash,
    };

    fn generate_entries(slot: Slot, num_data_entries: u64, tx_per_entry: u64) -> Vec<EntryInfo> {
        assert!(num_data_entries >= DEFAULT_TICKS_PER_SLOT);
        let mut entries = Vec::with_capacity((num_data_entries + DEFAULT_TICKS_PER_SLOT) as usize);
        let tick_entry_module = num_data_entries / DEFAULT_TICKS_PER_SLOT;
        let mut tick_entry_remain = DEFAULT_TICKS_PER_SLOT as usize;
        for i in 0..num_data_entries {
            let start_txn_index = i * tx_per_entry;
            let entry = EntryInfo {
                slot,
                entry_index: i,
                starting_txn_index: start_txn_index,
                entry_hash: Hash::new_unique(),
                executed_txn_count: tx_per_entry,
            };
            entries.push(entry);

            if i % tick_entry_module == 0 {
                // Add a tick entry
                entries.push(EntryInfo {
                    slot,
                    entry_index: i + DEFAULT_TICKS_PER_SLOT,
                    starting_txn_index: start_txn_index + tx_per_entry,
                    entry_hash: Hash::new_unique(),
                    executed_txn_count: 0, // Tick entry has no transactions
                });
                tick_entry_remain -= 1;
            }
        }
        for _ in 0..tick_entry_remain {
            // Add remaining tick entries
            entries.push(EntryInfo {
                slot,
                entry_index: num_data_entries + DEFAULT_TICKS_PER_SLOT,
                starting_txn_index: num_data_entries * tx_per_entry,
                entry_hash: Hash::new_unique(),
                executed_txn_count: 0, // Tick entry has no transactions
            });
        }
        entries
    }

    #[test]
    pub fn it_should_handle_all_lifecycle_transition_and_produce_frozen_block() {
        let mut blockstore = super::BlocksStateMachine::default();

        let first_shred_recv = SlotLifecycleUpdate {
            slot: 1,
            parent_slot: None,
            stage: SlotLifecycle::FirstShredReceived,
        };

        let completed_block = SlotLifecycleUpdate {
            slot: 1,
            parent_slot: None,
            stage: SlotLifecycle::Completed,
        };

        let slot_status_update = SlotCommitmentStatusUpdate {
            slot: 1,
            parent_slot: None,
            commitment: CommitmentLevel::Processed,
        };

        const NUM_DATA_ENTRIES: u64 = 64;
        let entries = generate_entries(1, NUM_DATA_ENTRIES, 10);
        let last_entry_hash = entries.last().unwrap().entry_hash;
        let summary = BlockSummary {
            slot: 1,
            parent_slot: 0,
            entry_count: NUM_DATA_ENTRIES + DEFAULT_TICKS_PER_SLOT,
            executed_transaction_count: NUM_DATA_ENTRIES * 10,
            blockhash: last_entry_hash,
            parent_blockhash: Hash::default(),
            block_time: 0,
        };

        // Whatever the order of insertion it should to notify the sealed block before slot status
        blockstore
            .process_replay_event(first_shred_recv.into())
            .unwrap();
        blockstore
            .process_replay_event(completed_block.into())
            .unwrap();
        for e in entries {
            blockstore.process_replay_event(e.into()).unwrap();
        }
        blockstore.process_replay_event(summary.into()).unwrap();
        blockstore.process_consensus_event(slot_status_update.into());

        let actual = blockstore.pop_next_unprocess_blockstore_update();
        assert!(matches!(
            actual,
            Some(super::BlockStateMachineOutput::FrozenBlock(_))
        ));
        let actual = blockstore.pop_next_unprocess_blockstore_update();
        assert!(matches!(
            actual,
            Some(super::BlockStateMachineOutput::SlotStatus(_))
        ));
        let actual = blockstore.pop_next_unprocess_blockstore_update();
        assert!(actual.is_none());
    }

    #[test]
    pub fn it_should_mark_slot_as_dead_if_not_received_first_shred() {
        let mut blockstore = super::BlocksStateMachine::default();

        let completed_block = SlotLifecycleUpdate {
            slot: 1,
            parent_slot: None,
            stage: SlotLifecycle::Completed,
        };

        // Send completed block without first shred received
        assert!(
            blockstore
                .process_replay_event(completed_block.into())
                .is_err()
        );
    }

    #[test]
    pub fn blockstore_gc_should_work_even_when_empty() {
        let mut blockstore = super::BlocksStateMachine::default();
        let mut gc_trace = Vec::new();
        let actual = blockstore.gc(Some(&mut gc_trace));
        assert_eq!(actual.slot_purge_count, 0);
        assert_eq!(actual.slot_blocked_count, 0);
        assert!(gc_trace.is_empty());
    }

    #[test]
    pub fn blockstore_should_correct_missing_processed_slot_status() {
        let mut blockstore = super::BlocksStateMachine::default();
        let slot_confirmed = SlotCommitmentStatusUpdate {
            parent_slot: None,
            slot: 1,
            commitment: CommitmentLevel::Confirmed,
        };

        let first_shred_recv = SlotLifecycleUpdate {
            slot: 1,
            parent_slot: None,
            stage: SlotLifecycle::FirstShredReceived,
        };

        let completed_block = SlotLifecycleUpdate {
            slot: 1,
            parent_slot: None,
            stage: SlotLifecycle::Completed,
        };

        const NUM_DATA_ENTRIES: u64 = 64;
        let entries = generate_entries(1, 64, 10);
        let last_entry_hash = entries.last().unwrap().entry_hash;
        let summary = BlockSummary {
            slot: 1,
            parent_slot: 0,
            entry_count: NUM_DATA_ENTRIES + DEFAULT_TICKS_PER_SLOT,
            executed_transaction_count: NUM_DATA_ENTRIES * 10,
            blockhash: last_entry_hash,
            parent_blockhash: Hash::default(),
            block_time: 0,
        };

        // Whatever the order of insertion it should to notify the sealed block before slot status
        blockstore
            .process_replay_event(first_shred_recv.into())
            .unwrap();
        blockstore
            .process_replay_event(completed_block.into())
            .unwrap();
        for e in entries {
            blockstore.process_replay_event(e.into()).unwrap();
        }
        blockstore.process_consensus_event(slot_confirmed.into());
        blockstore.process_replay_event(summary.into()).unwrap();

        let actual = blockstore.pop_next_unprocess_blockstore_update().unwrap();
        let BlockStateMachineOutput::FrozenBlock(frozen_block) = actual else {
            panic!("Expected frozen block");
        };
        assert_eq!(frozen_block.slot, 1);

        let BlockStateMachineOutput::SlotStatus(status) =
            blockstore.pop_next_unprocess_blockstore_update().unwrap()
        else {
            panic!("Expected slot status update");
        };

        assert_eq!(status.slot, 1);
        assert_eq!(status.commitment, CommitmentLevel::Processed);

        let actual = blockstore.pop_next_unprocess_blockstore_update().unwrap();
        let BlockStateMachineOutput::SlotStatus(status) = actual else {
            panic!("Expected slot status update");
        };
        assert_eq!(status.slot, 1);
        assert_eq!(status.commitment, CommitmentLevel::Confirmed);

        let actual = blockstore.pop_next_unprocess_blockstore_update();
        assert!(actual.is_none());
    }

    #[test]
    pub fn it_should_detect_retroactively_rooted_slots() {
        // Retroactively rooted slots are slots that were not rooted at the time of the slot status update,
        // but were later rooted by a descendant slot status update.
        let mut blockstore = super::BlocksStateMachine::default();

        let slot1_processed = SlotCommitmentStatusUpdate {
            parent_slot: None,
            slot: 1,
            commitment: CommitmentLevel::Processed,
        };

        let slot2_finalized = SlotCommitmentStatusUpdate {
            parent_slot: Some(1),
            slot: 2,
            commitment: CommitmentLevel::Finalized,
        };

        let slot1_first_shred_recv = SlotLifecycleUpdate {
            slot: 1,
            parent_slot: None,
            stage: SlotLifecycle::FirstShredReceived,
        };

        let slot2_first_shred_recv = SlotLifecycleUpdate {
            slot: 2,
            parent_slot: Some(1),
            stage: SlotLifecycle::FirstShredReceived,
        };

        let slot1_completed_block = SlotLifecycleUpdate {
            slot: 1,
            parent_slot: None,
            stage: SlotLifecycle::Completed,
        };

        let slot2_completed_block = SlotLifecycleUpdate {
            slot: 2,
            parent_slot: Some(1),
            stage: SlotLifecycle::Completed,
        };

        const NUM_DATA_ENTRIES: u64 = 64;
        let slot1_entries = generate_entries(1, 64, 10);
        let slot2_entries = generate_entries(2, 64, 10);

        let last_entry_hash1 = slot1_entries.last().unwrap().entry_hash;
        let last_entry_hash2 = slot2_entries.last().unwrap().entry_hash;

        let slot1_summary = BlockSummary {
            slot: 1,
            parent_slot: 0,
            entry_count: NUM_DATA_ENTRIES + DEFAULT_TICKS_PER_SLOT,
            executed_transaction_count: NUM_DATA_ENTRIES * 10,
            blockhash: last_entry_hash1,
            parent_blockhash: Hash::default(),
            block_time: 0,
        };

        let slot2_summary = BlockSummary {
            slot: 2,
            parent_slot: 1,
            entry_count: NUM_DATA_ENTRIES + DEFAULT_TICKS_PER_SLOT,
            executed_transaction_count: NUM_DATA_ENTRIES * 10,
            blockhash: last_entry_hash2,
            parent_blockhash: Hash::default(),
            block_time: 0,
        };

        // Whatever the order of insertion it should to notify the sealed block before slot status
        blockstore
            .process_replay_event(slot1_first_shred_recv.into())
            .unwrap();
        blockstore
            .process_replay_event(slot1_completed_block.into())
            .unwrap();
        for e in slot1_entries {
            blockstore.process_replay_event(e.into()).unwrap();
        }
        blockstore
            .process_replay_event(slot1_summary.into())
            .unwrap();
        // We only insert the slot status update for Confirmed, missing Processed
        blockstore.process_consensus_event(slot1_processed.into());

        let actual = blockstore.pop_next_unprocess_blockstore_update().unwrap();
        let BlockStateMachineOutput::FrozenBlock(frozen_block) = actual else {
            panic!("Expected frozen block");
        };
        assert_eq!(frozen_block.slot, 1);

        let BlockStateMachineOutput::SlotStatus(status) =
            blockstore.pop_next_unprocess_blockstore_update().unwrap()
        else {
            panic!("Expected slot status update");
        };

        assert_eq!(status.slot, 1);
        assert_eq!(status.commitment, CommitmentLevel::Processed);

        // Now we insert the second slot, which will retroactively root the first slot
        blockstore
            .process_replay_event(slot2_first_shred_recv.into())
            .unwrap();
        blockstore
            .process_replay_event(slot2_completed_block.into())
            .unwrap();
        for e in slot2_entries {
            blockstore.process_replay_event(e.into()).unwrap();
        }
        blockstore
            .process_replay_event(slot2_summary.into())
            .unwrap();
        blockstore.process_consensus_event(slot2_finalized.into());

        let actual = blockstore.pop_next_unprocess_blockstore_update().unwrap();
        let BlockStateMachineOutput::FrozenBlock(frozen_block) = actual else {
            panic!("Expected frozen block");
        };
        assert_eq!(frozen_block.slot, 2);

        for expected_cl in iter_to_commitment(&CommitmentLevel::Finalized) {
            let actual = blockstore.pop_next_unprocess_blockstore_update().unwrap();
            let BlockStateMachineOutput::SlotStatus(status) = actual else {
                panic!("Expected slot status update");
            };
            assert_eq!(status.slot, 2);
            assert_eq!(status.commitment, expected_cl);
        }
        // Now we should have a retroactively rooted slot for slot 1
        for expected_cl in [CommitmentLevel::Confirmed, CommitmentLevel::Finalized] {
            let actual = blockstore.pop_next_unprocess_blockstore_update().unwrap();
            let BlockStateMachineOutput::SlotStatus(status) = actual else {
                panic!("Expected slot status update");
            };
            assert_eq!(status.slot, 1);
            assert_eq!(status.commitment, expected_cl);
        }
    }

    #[test]
    pub fn it_should_handle_rolledback_slot() {
        // During an duplicate unconfirmed slot, we may process a slot, then restart all over again from bank_created.
        // the state machine should be able to handle this case and not panic, and correctly process the new slot lifecycle update.
        let mut blockstore = super::BlocksStateMachine::default();

        let bank_created = SlotLifecycleUpdate {
            slot: 1,
            parent_slot: None,
            stage: SlotLifecycle::CreatedBank,
        };

        let completed_block = SlotLifecycleUpdate {
            slot: 1,
            parent_slot: None,
            stage: SlotLifecycle::Completed,
        };

        let slot_status_update = SlotCommitmentStatusUpdate {
            slot: 1,
            parent_slot: None,
            commitment: CommitmentLevel::Processed,
        };

        const NUM_DATA_ENTRIES: u64 = 64;
        let entries = generate_entries(1, NUM_DATA_ENTRIES, 10);
        let last_entry_hash = entries.last().unwrap().entry_hash;
        let summary = BlockSummary {
            slot: 1,
            parent_slot: 0,
            entry_count: NUM_DATA_ENTRIES + DEFAULT_TICKS_PER_SLOT,
            executed_transaction_count: NUM_DATA_ENTRIES * 10,
            blockhash: last_entry_hash,
            parent_blockhash: Hash::default(),
            block_time: 0,
        };

        // Whatever the order of insertion it should to notify the sealed block before slot status
        blockstore
            .process_replay_event(bank_created.into())
            .unwrap();
        blockstore
            .process_replay_event(completed_block.into())
            .unwrap();
        for e in &entries {
            blockstore.process_replay_event(e.clone().into()).unwrap();
        }
        blockstore
            .process_replay_event(summary.clone().into())
            .unwrap();
        blockstore.process_consensus_event(slot_status_update.clone().into());

        let actual = blockstore.pop_next_unprocess_blockstore_update();
        assert!(matches!(
            actual,
            Some(super::BlockStateMachineOutput::BankCreated(_))
        ));

        let actual = blockstore.pop_next_unprocess_blockstore_update();
        assert!(matches!(
            actual,
            Some(super::BlockStateMachineOutput::FrozenBlock(_))
        ));
        let actual = blockstore.pop_next_unprocess_blockstore_update();
        assert!(matches!(
            actual,
            Some(super::BlockStateMachineOutput::SlotStatus(_))
        ));
        let actual = blockstore.pop_next_unprocess_blockstore_update();
        assert!(actual.is_none());

        // Now we receive a new bank created for the same slot, which mean the previous slot lifecycle is rolled back.
        blockstore
            .process_replay_event(bank_created.into())
            .unwrap();
        for e in &entries {
            blockstore.process_replay_event(e.clone().into()).unwrap();
        }
        blockstore
            .process_replay_event(summary.clone().into())
            .unwrap();
        blockstore.process_consensus_event(slot_status_update.clone().into());

        let actual = blockstore.pop_next_unprocess_blockstore_update();
        assert!(matches!(
            actual,
            Some(super::BlockStateMachineOutput::BankReset(_))
        ));

        let actual = blockstore.pop_next_unprocess_blockstore_update();
        assert!(matches!(
            actual,
            Some(super::BlockStateMachineOutput::FrozenBlock(_))
        ));
        let actual = blockstore.pop_next_unprocess_blockstore_update();
        assert!(actual.is_some());
        assert!(matches!(
            actual,
            Some(super::BlockStateMachineOutput::SlotStatus(_))
        ));
        let actual = blockstore.pop_next_unprocess_blockstore_update();
        assert!(actual.is_none());
    }
}
