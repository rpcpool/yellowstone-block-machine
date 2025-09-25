use {
    crate::forks::{Forks, ForksMutationTracer},
    derive_more::From,
    rustc_hash::{FxHashMap, FxHashSet},
    serde::{Deserialize, Serialize},
    solana_clock::{DEFAULT_TICKS_PER_SLOT, Slot},
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
    pub executed_transaction_count: u64,
    pub blockhash: Hash,
}

#[derive(Debug, From)]
pub enum BlockstoreInputEvent {
    Entry(EntryInfo),
    SlotCommitmentStatus(SlotCommitmentStatusUpdate),
    SlotLifecycleStatus(SlotLifecycleUpdate),
    BlockSummary(BlockSummary),
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
    pub entries: Vec<EntryInfo>,
    pub blockhash: Hash,
}

// Avg 2k tx + 2k account update
pub const AVG_BLOCK_LEN: usize = 4000;
// Avg transaction per block ~ 2000;
pub const AVG_TPB: usize = 2000;

#[derive(Debug, thiserror::Error)]
pub enum FreezeError {
    #[error("entry count mismatch, expected {expected}, got {got}")]
    InvalidTickCount {
        expected: u64,
        got: u64,
        invalid_block: Block,
    },
    #[error("entry count mismatch, expected {expected}, got {got}")]
    InvalidEntryCount {
        expected: u64,
        got: u64,
        invalid_block: Block,
    },
    #[error("blockhash mismatch, expected {expected}, got {got}")]
    InvalidBlockhash {
        expected: Hash,
        got: Hash,
        invalid_block: Block,
    },
}

impl FreezeError {
    fn get_block(&self) -> &Block {
        match self {
            FreezeError::InvalidTickCount { invalid_block, .. } => invalid_block,
            FreezeError::InvalidEntryCount { invalid_block, .. } => invalid_block,
            FreezeError::InvalidBlockhash { invalid_block, .. } => invalid_block,
        }
    }
}

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
            .map(|e| e.entry_hash)
    }

    fn freeze(self, summary: &BlockSummary) -> Result<FrozenBlock, FreezeError> {
        if self.entry_cnt != summary.entry_count {
            return Err(FreezeError::InvalidEntryCount {
                expected: summary.entry_count,
                got: self.entry_cnt,
                invalid_block: self,
            });
        }

        if self.tick_entry_cnt != DEFAULT_TICKS_PER_SLOT {
            return Err(FreezeError::InvalidTickCount {
                expected: DEFAULT_TICKS_PER_SLOT,
                got: self.tick_entry_cnt,
                invalid_block: self,
            });
        }

        // Last entry must be a tick entry (entry with no transactions)
        let entry = self.entries.get(&(self.entry_cnt - 1)).unwrap();

        assert!(entry.executed_txn_count == 0);

        let blockhash = self
            .last_entry_hash()
            .expect("Block must have at least one entry");

        if blockhash != summary.blockhash {
            return Err(FreezeError::InvalidBlockhash {
                expected: summary.blockhash,
                got: blockhash,
                invalid_block: self,
            });
        }

        let fb = FrozenBlock {
            slot: self.slot,
            entries: self.entries.values().cloned().collect(),
            blockhash,
        };

        Ok(fb)
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
pub struct BlockSM {
    /// Holds block under construction, not yet frozen
    block_buffer_map: FxHashMap<Slot, Block>,

    completed_blocks: FxHashSet<Slot>,

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
    blockstore_update_queue: VecDeque<(usize, BlockStateMachineOuput)>,

    /// Maintain forks history of the blockchain.    
    forks: Forks,
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
}

#[derive(Debug)]
pub enum DeadletterEvent {
    AlreadyFrozen(BlockstoreInputEvent),
    // Sent when we receive a block data for a slot that is skipped.
    SkippedBlock(BlockstoreInputEvent),
    // Sent when slot status arrived before any slot update
    OrphanSlotStatus(BlockstoreInputEvent),

    UnprocessableBlock(FreezeError),
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
pub enum BlockStateMachineOuput {
    FrozenBlock(FrozenBlock),
    SlotStatus(SlotCommitmentStatusUpdate),
    ForksDetected(ForkDetected),
    DeadSlotDetected(DeadBlockDetected),
}

impl BlockStateMachineOuput {
    pub fn slot(&self) -> Slot {
        match self {
            Self::DeadSlotDetected(blk) => blk.slot,
            Self::FrozenBlock(blk) => blk.slot,
            Self::SlotStatus(update) => update.slot,
            Self::ForksDetected(info) => info.slot,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum InsertBlockstoreEventError {
    #[error("Failed to send event to blockstore, no more capacity")]
    NotSend(BlockstoreInputEvent),
}

#[derive(Debug, thiserror::Error)]
#[error("Stale progress")]
pub struct StaleProgressError;

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

impl Default for BlockSM {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug)]
pub struct LongShortForksMutationTracer<'a> {
    long: &'a mut FxHashSet<Slot>,
    short: &'a mut FxHashSet<Slot>,
}

impl<'a> ForksMutationTracer for LongShortForksMutationTracer<'a> {
    fn insert(&mut self, slot: Slot) {
        // We only insert into short if slot not already present in long.
        if self.long.insert(slot) {
            self.short.insert(slot);
        }
    }
}

impl BlockSM {
    ///
    /// Creates a new blockstore instance
    ///
    pub fn new() -> Self {
        Self {
            block_buffer_map: Default::default(),
            completed_blocks: Default::default(),
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

    fn push_new_update(&mut self, update: BlockStateMachineOuput) -> Revision {
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
            for slot_status in updates {
                self.handle_slot_commitment_status_update(slot_status);
            }
        }
    }

    fn handle_slot_lifecyle_status(&mut self, slot_lifecycle_status: SlotLifecycleUpdate) {
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
            SlotLifecycle::FirstShredReceived | SlotLifecycle::CreatedBank => {
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
            SlotLifecycle::Completed => {
                if self.block_buffer_map.contains_key(&slot) {
                    if !self.completed_blocks.insert(slot) {
                        // This should never happen, but in case it does we should not panic.
                        tracing::warn!("Slot {} is already completed", slot);
                    }
                } else {
                    tracing::trace!("Slot {} is not in the block buffer map, skipping", slot);
                    self.push_to_dlq(DeadletterEvent::SkippedBlock(slot_lifecycle_status.into()));
                }
            }
            SlotLifecycle::Dead => {
                self.mark_block_as_dead(slot);
            }
        }
    }

    fn handle_slot_commitment_status_update(
        &mut self,
        mut slot_status: SlotCommitmentStatusUpdate,
    ) {
        let slot = slot_status.slot;

        if let Some(parent) = slot_status.parent_slot {
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
        } else {
            // If for some reason we already know the parent, just inject it into the slot status.
            if let Some(parent) = self.forks.get_parent(&slot_status.slot) {
                slot_status.parent_slot = Some(parent);
            }
        }

        if !self.frozen_block_index.contains_key(&slot)
            && !self.block_buffer_map.contains_key(&slot)
        {
            self.push_to_dlq(DeadletterEvent::OrphanSlotStatus(slot_status.into()));
            return;
        }

        if !self.completed_blocks.contains(&slot) && self.block_buffer_map.contains_key(&slot) {
            // If we receive a slot status before the block is completed then we should not process it.
            // This mean should never happen, but in case it does we should not panic.
            tracing::error!(
                "Received slot status for slot {} before the block is completed",
                slot
            );
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
                        self.push_new_update(BlockStateMachineOuput::SlotStatus(slot_status2));
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
                if self.block_buffer_map.get(&slot).is_some() {
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
        self.remove_slot_references_in_state(slot);
        self.push_new_update(BlockStateMachineOuput::DeadSlotDetected(
            DeadBlockDetected { slot },
        ));
    }

    fn handle_block_entry_insert(&mut self, data: EntryInfo) {
        let slot = data.slot;
        if self.frozen_block_index.contains_key(&slot) {
            self.push_to_dlq(DeadletterEvent::AlreadyFrozen(data.into()));
            return;
        }
        let Some(buffer) = self.block_buffer_map.get_mut(&slot) else {
            // If the block container has not been created yet, it means we never received FIRST_SHRED.
            // Therefore we cannot insert the block data.
            self.push_to_dlq(DeadletterEvent::SkippedBlock(data.into()));
            return;
        };
        buffer.insert_entry(data);
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
            tracing::trace!("Forks detected for slot {}", slot);
            self.push_new_update(BlockStateMachineOuput::ForksDetected(ForkDetected { slot }));
        }
    }

    fn handle_block_summary(&mut self, block_summary: BlockSummary) {
        let Some(block) = self.block_buffer_map.remove(&block_summary.slot) else {
            tracing::debug!(
                "Block summary for slot {} but no block data found",
                block_summary.slot
            );
            self.push_to_dlq(DeadletterEvent::SkippedBlock(block_summary.into()));
            return;
        };

        let frozen_block = match block.freeze(&block_summary) {
            Ok(frozen_block) => frozen_block,
            Err(e) => {
                tracing::error!(
                    "Failed to freeze block for slot {}: {:?}",
                    e.get_block().slot,
                    e
                );
                self.push_to_dlq(DeadletterEvent::UnprocessableBlock(e));
                return;
            }
        };
        let slot = frozen_block.slot;

        // Block is now frozen which mean every transaction and account update is now in the block
        if !self.completed_blocks.contains(&slot) {
            // This happened one time in 24 hours of testing.
            // This might be a code in either gRPC or Agave.
            // Completed block should event should be sent when all shred are received.
            // This should any Slot Status update.
            tracing::error!("Block {} is frozen but not completed", slot);
            // Artifially mark the block as completed.
            self.completed_blocks.insert(slot);
        }
        tracing::debug!("Block frozen for slot {}", slot);
        self.frozen_block_index.entry(slot).or_default();
        self.push_new_update(BlockStateMachineOuput::FrozenBlock(frozen_block));

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
    }

    pub fn process_event(&mut self, event: BlockstoreInputEvent) {
        match event {
            BlockstoreInputEvent::SlotCommitmentStatus(slot_status) => {
                tracing::trace!("Inserting slot status for slot {}", slot_status.slot);
                self.handle_slot_commitment_status_update(slot_status.clone());
            }
            BlockstoreInputEvent::Entry(data) => {
                self.handle_block_entry_insert(data);
            }
            BlockstoreInputEvent::SlotLifecycleStatus(slot_lifecycle_status) => {
                tracing::trace!(
                    "Inserting slot lifecycle status for slot {}",
                    slot_lifecycle_status.slot
                );
                self.handle_slot_lifecyle_status(slot_lifecycle_status);
            }
            BlockstoreInputEvent::BlockSummary(bs) => {
                tracing::trace!("Inserting block summary for slot {}", bs.slot);
                self.handle_block_summary(bs);
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
        self.completed_blocks.remove(&slot);
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
    pub fn gc(&mut self) -> BlockstoreGCStats {
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
    pub fn pop_next_unprocess_blockstore_update(&mut self) -> Option<BlockStateMachineOuput> {
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
            BlockStateMachineOuput, BlockSummary, EntryInfo, SlotCommitmentStatusUpdate,
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
        let mut blockstore = super::BlockSM::default();

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
            entry_count: NUM_DATA_ENTRIES + DEFAULT_TICKS_PER_SLOT,
            executed_transaction_count: NUM_DATA_ENTRIES * 10,
            blockhash: last_entry_hash,
        };

        // Whatever the order of insertion it should to notify the sealed block before slot status
        blockstore.process_event(first_shred_recv.into());
        blockstore.process_event(completed_block.into());
        for e in entries {
            blockstore.process_event(e.into());
        }
        blockstore.process_event(summary.into());
        blockstore.process_event(slot_status_update.into());

        let actual = blockstore.pop_next_unprocess_blockstore_update();
        assert!(matches!(
            actual,
            Some(super::BlockStateMachineOuput::FrozenBlock(_))
        ));
        let actual = blockstore.pop_next_unprocess_blockstore_update();
        assert!(matches!(
            actual,
            Some(super::BlockStateMachineOuput::SlotStatus(_))
        ));
        let actual = blockstore.pop_next_unprocess_blockstore_update();
        assert!(actual.is_none());
    }

    #[test]
    pub fn it_should_mark_slot_as_dead_if_not_received_first_shred() {
        let mut blockstore = super::BlockSM::default();

        let completed_block = SlotLifecycleUpdate {
            slot: 1,
            parent_slot: None,
            stage: SlotLifecycle::Completed,
        };

        // Send completed block without first shred received
        blockstore.process_event(completed_block.into());
        let ev = blockstore.pop_next_dlq();
        assert!(ev.is_some());
        assert!(matches!(
            ev.unwrap(),
            super::DeadletterEvent::SkippedBlock(_)
        ));
    }

    #[test]
    pub fn blockstore_gc_should_work_even_when_empty() {
        let mut blockstore = super::BlockSM::default();
        let actual = blockstore.gc();
        assert_eq!(actual.slot_purge_count, 0);
        assert_eq!(actual.slot_blocked_count, 0);
    }

    #[test]
    pub fn blockstore_should_correct_missing_processed_slot_status() {
        let mut blockstore = super::BlockSM::default();
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
            entry_count: NUM_DATA_ENTRIES + DEFAULT_TICKS_PER_SLOT,
            executed_transaction_count: NUM_DATA_ENTRIES * 10,
            blockhash: last_entry_hash,
        };

        // Whatever the order of insertion it should to notify the sealed block before slot status
        blockstore.process_event(first_shred_recv.into());
        blockstore.process_event(completed_block.into());
        for e in entries {
            blockstore.process_event(e.into());
        }
        blockstore.process_event(slot_confirmed.into());
        blockstore.process_event(summary.into());

        let actual = blockstore.pop_next_unprocess_blockstore_update().unwrap();
        let BlockStateMachineOuput::FrozenBlock(frozen_block) = actual else {
            panic!("Expected frozen block");
        };
        assert_eq!(frozen_block.slot, 1);

        let BlockStateMachineOuput::SlotStatus(status) =
            blockstore.pop_next_unprocess_blockstore_update().unwrap()
        else {
            panic!("Expected slot status update");
        };

        assert_eq!(status.slot, 1);
        assert_eq!(status.commitment, CommitmentLevel::Processed);

        let actual = blockstore.pop_next_unprocess_blockstore_update().unwrap();
        let BlockStateMachineOuput::SlotStatus(status) = actual else {
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
        let mut blockstore = super::BlockSM::default();

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
            entry_count: NUM_DATA_ENTRIES + DEFAULT_TICKS_PER_SLOT,
            executed_transaction_count: NUM_DATA_ENTRIES * 10,
            blockhash: last_entry_hash1,
        };

        let slot2_summary = BlockSummary {
            slot: 2,
            entry_count: NUM_DATA_ENTRIES + DEFAULT_TICKS_PER_SLOT,
            executed_transaction_count: NUM_DATA_ENTRIES * 10,
            blockhash: last_entry_hash2,
        };

        // Whatever the order of insertion it should to notify the sealed block before slot status
        blockstore.process_event(slot1_first_shred_recv.into());
        blockstore.process_event(slot1_completed_block.into());
        for e in slot1_entries {
            blockstore.process_event(e.into());
        }
        blockstore.process_event(slot1_summary.into());
        // We only insert the slot status update for Confirmed, missing Processed
        blockstore.process_event(slot1_processed.into());

        let actual = blockstore.pop_next_unprocess_blockstore_update().unwrap();
        let BlockStateMachineOuput::FrozenBlock(frozen_block) = actual else {
            panic!("Expected frozen block");
        };
        assert_eq!(frozen_block.slot, 1);

        let BlockStateMachineOuput::SlotStatus(status) =
            blockstore.pop_next_unprocess_blockstore_update().unwrap()
        else {
            panic!("Expected slot status update");
        };

        assert_eq!(status.slot, 1);
        assert_eq!(status.commitment, CommitmentLevel::Processed);

        // Now we insert the second slot, which will retroactively root the first slot
        blockstore.process_event(slot2_first_shred_recv.into());
        blockstore.process_event(slot2_completed_block.into());
        for e in slot2_entries {
            blockstore.process_event(e.into());
        }
        blockstore.process_event(slot2_summary.into());
        blockstore.process_event(slot2_finalized.into());

        let actual = blockstore.pop_next_unprocess_blockstore_update().unwrap();
        let BlockStateMachineOuput::FrozenBlock(frozen_block) = actual else {
            panic!("Expected frozen block");
        };
        assert_eq!(frozen_block.slot, 2);

        for expected_cl in iter_to_commitment(&CommitmentLevel::Finalized) {
            let actual = blockstore.pop_next_unprocess_blockstore_update().unwrap();
            let BlockStateMachineOuput::SlotStatus(status) = actual else {
                panic!("Expected slot status update");
            };
            assert_eq!(status.slot, 2);
            assert_eq!(status.commitment, expected_cl);
        }
        // Now we should have a retroactively rooted slot for slot 1
        for expected_cl in [CommitmentLevel::Confirmed, CommitmentLevel::Finalized] {
            let actual = blockstore.pop_next_unprocess_blockstore_update().unwrap();
            let BlockStateMachineOuput::SlotStatus(status) = actual else {
                panic!("Expected slot status update");
            };
            assert_eq!(status.slot, 1);
            assert_eq!(status.commitment, expected_cl);
        }
    }
}
