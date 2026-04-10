use {
    crate::{
        dragonsmouth::wrapper::{BlocksStateMachineWrapper, RESERVED_FILTER_NAME},
        state_machine::{
            BlockStateMachineOutput, DeadBlockDetected, DeadletterEvent, ForkDetected, SlotCommitmentStatusUpdate
        },
    },
    derive_more::From,
    futures_util::{Stream, TryStream, TryStreamExt},
    rustc_hash::FxHashMap,
    solana_clock::Slot,
    solana_commitment_config::CommitmentLevel,
    std::{cmp::Ordering, collections::VecDeque, sync::Arc},
    yellowstone_grpc_proto::geyser::{SubscribeUpdate, subscribe_update::UpdateOneof},
};

///
/// A fully reconstructed block, containing all events (accounts, transactions, entries) for a given slot.
///
#[derive(Debug, Clone)]
pub struct Block {
    pub slot: Slot,
    pub events: Vec<Box<SubscribeUpdate>>,
    pub account_idx_map: Vec<usize>,
    pub transaction_idx_map: Vec<usize>,
    entry_idx_map: Vec<usize>,
}

impl Block {
    ///
    /// Returns the number of transactions in this block.
    ///
    pub fn txn_len(&self) -> usize {
        self.transaction_idx_map.len()
    }

    ///
    /// Returns the number of accounts in this block.
    ///
    pub fn account_len(&self) -> usize {
        self.account_idx_map.len()
    }

    ///
    /// Returns the number of entries in this block.
    ///
    pub fn entry_len(&self) -> usize {
        self.entry_idx_map.len()
    }

    ///
    /// Checks if the block has no events.
    ///
    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }

    ///
    /// Returns the number of events in this block.
    ///
    pub fn len(&self) -> usize {
        self.events.len()
    }
}

///
/// The different types of outputs produced by the Dragon's mouth block machine.
///
#[derive(Debug, From)]
pub enum BlockMachineOutput {
    ///
    /// A fully reconstructed block, ready for processing.
    ///
    FrozenBlock(Block),
    ///
    /// An update on the commitment status of a slot.
    /// Note: This is sent when the slot reaches or exceeds the minimum commitment level set during initialization.
    /// It is guaranteed that the block for this slot has been sent before this update.
    ///  
    SlotCommitmentUpdate(SlotCommitmentStatusUpdate),
    ///
    /// A notification that a fork has been detected.
    ///
    ForkDetected(ForkDetected),
    ///
    /// A notification that a dead block has been detected.
    /// Note: All Dead blocks are Forks, but not all Forks are Dead blocks.
    /// Dead blocks mostly come from corrupted entries early in the replay process of a slot.
    ///
    DeadBlockDetect(DeadBlockDetected),
}

///
/// A stream that yields [`BlockMachineOutput`] items.
///
/// # Generic Parameters
///
/// - `Source`: The underlying source of `SubscribeUpdate` events, typically a gRPC stream from the Geyser plugin.
///
pub struct BlockStream<Source> {
    pub(crate) min_commitment_level: CommitmentLevel,
    pub(crate) source: Source,
    pub(crate) machine: BlocksStateMachineWrapper,
    pub(crate) storage: InMemoryBlockStore,
    pub(crate) pending: VecDeque<BlockMachineOutput>,
}

impl<Source> BlockStream<Source> {
    pub fn new(source: Source, min_commitment_level: CommitmentLevel) -> Self {
        Self {
            min_commitment_level,
            source,
            machine: BlocksStateMachineWrapper::new_with_slot_gc_tracing(),
            storage: InMemoryBlockStore::default(),
            pending: VecDeque::new(),
        }
    }
}

fn compare_commitment(cl1: CommitmentLevel, cl2: CommitmentLevel) -> Ordering {
    match (cl1, cl2) {
        (CommitmentLevel::Processed, CommitmentLevel::Processed) => Ordering::Equal,
        (CommitmentLevel::Confirmed, CommitmentLevel::Confirmed) => Ordering::Equal,
        (CommitmentLevel::Finalized, CommitmentLevel::Finalized) => Ordering::Equal,
        (CommitmentLevel::Processed, _) => Ordering::Less,
        (CommitmentLevel::Confirmed, CommitmentLevel::Processed) => Ordering::Greater,
        (CommitmentLevel::Finalized, CommitmentLevel::Processed) => Ordering::Greater,
        (CommitmentLevel::Finalized, CommitmentLevel::Confirmed) => Ordering::Greater,
        (CommitmentLevel::Confirmed, CommitmentLevel::Finalized) => Ordering::Less,
    }
}

impl<Source> BlockStream<Source> {
    fn insert_into_storage(&mut self, event: SubscribeUpdate) {
        let SubscribeUpdate {
            filters,
            created_at,
            update_oneof,
        } = event;
        let Some(update_oneof) = update_oneof else {
            return;
        };
        match update_oneof {
            UpdateOneof::Account(update) => {
                let slot = update.slot;
                self.storage.insert_block_data(
                    slot,
                    SubscribeUpdate {
                        filters,
                        created_at,
                        update_oneof: Some(UpdateOneof::Account(update)),
                    },
                );
            }
            UpdateOneof::Transaction(update) => {
                let slot = update.slot;
                self.storage.insert_block_data(
                    slot,
                    SubscribeUpdate {
                        filters,
                        created_at,
                        update_oneof: Some(UpdateOneof::Transaction(update)),
                    },
                );
            }
            UpdateOneof::Entry(update) => {
                let slot = update.slot;
                if filters.iter().any(|k| k != RESERVED_FILTER_NAME) {
                    self.storage.insert_block_data(
                        slot,
                        SubscribeUpdate {
                            filters,
                            created_at,
                            update_oneof: Some(UpdateOneof::Entry(update)),
                        },
                    );
                }
            }
            _ => {}
        }
    }

    fn on_new_frozen_block(&mut self) {
        // Drain DLQ — clean up slots the state machine gave up on
        while let Some(dlq_event) = self.machine.pop_next_dlq() {
            match dlq_event {
                DeadletterEvent::Incomplete(slot) => {
                    self.storage.remove_slot(slot);
                }
            }
        }

        while let Some(slot) = self.machine.pop_slot_gc_trace() {
            self.storage.remove_slot(slot);
        }
    }

    fn process_state_machine_output(&mut self) {
        while let Some(output) = self.machine.pop_next_state_machine_output() {
            match output {
                BlockStateMachineOutput::FrozenBlock(frozen_block) => {
                    let slot = frozen_block.slot;
                    self.on_new_frozen_block();
                    self.storage.mark_block_as_frozen(slot);
                }
                BlockStateMachineOutput::SlotStatus(slot_status) => {
                    let slot = slot_status.slot;
                    let cl = slot_status.commitment;
                    match compare_commitment(cl, self.min_commitment_level) {
                        Ordering::Less => continue,
                        _ => {
                            let commitment_level_update = SlotCommitmentStatusUpdate {
                                parent_slot: slot_status.parent_slot,
                                slot: slot_status.slot,
                                commitment: cl,
                            };
                            if let Some(block) = self.storage.finish_slot(slot) {
                                self.pending
                                    .push_back(BlockMachineOutput::FrozenBlock(block));
                            }

                            self.pending
                                .push_back(BlockMachineOutput::SlotCommitmentUpdate(
                                    commitment_level_update,
                                ));
                        }
                    }
                }
                BlockStateMachineOutput::ForksDetected(fork_detected) => {
                    self.storage.remove_slot(fork_detected.slot);
                    self.pending
                        .push_back(BlockMachineOutput::ForkDetected(fork_detected));
                }
                BlockStateMachineOutput::DeadSlotDetected(dead_block) => {
                    self.storage.remove_slot(dead_block.slot);
                    self.pending
                        .push_back(BlockMachineOutput::DeadBlockDetect(dead_block));
                }
            }
        }
    }
}

impl<Source> Stream for BlockStream<Source>
where
    Source: TryStream<Ok = SubscribeUpdate> + Unpin,
{
    type Item = Result<BlockMachineOutput, Source::Error>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        loop {
            if let Some(output) = self.pending.pop_front() {
                return std::task::Poll::Ready(Some(Ok(output)));
            }

            match self.source.try_poll_next_unpin(cx) {
                std::task::Poll::Ready(Some(Ok(ev))) => {
                    if self.machine.handle_new_geyser_event(&ev).is_ok() {
                        self.insert_into_storage(ev);
                    }
                }
                std::task::Poll::Ready(Some(Err(e))) => {
                    return std::task::Poll::Ready(Some(Err(e)));
                }
                std::task::Poll::Ready(None) => {
                    return std::task::Poll::Ready(None);
                }
                std::task::Poll::Pending => {
                    return std::task::Poll::Pending;
                }
            }
            self.process_state_machine_output();
        }
    }
}

#[derive(Debug, Default)]
struct BlockAccumulator {
    events: Vec<Box<SubscribeUpdate>>,
    account_idx_map: Vec<usize>,
    transaction_idx_map: Vec<usize>,
    entry_idx_map: Vec<usize>,
}

impl BlockAccumulator {
    fn finish(self, slot: Slot) -> Block {
        Block {
            slot,
            events: self.events,
            account_idx_map: self.account_idx_map,
            transaction_idx_map: self.transaction_idx_map,
            entry_idx_map: self.entry_idx_map,
        }
    }
}

///
/// An in-memory store for blocks being reconstructed.
///
/// It maintains active blocks (currently being reconstructed) and frozen blocks (fully reconstructed).
#[derive(Default)]
pub struct InMemoryBlockStore {
    active_block_map: FxHashMap<Slot, BlockAccumulator>,
    frozen_block_map: FxHashMap<Slot, BlockAccumulator>,
}

impl InMemoryBlockStore {
    fn insert_block_data(&mut self, slot: Slot, update: SubscribeUpdate) {
        let block = self.active_block_map.entry(slot).or_default();
        let idx = block.events.len();
        match &update.update_oneof {
            Some(UpdateOneof::Account(_)) => {
                block.account_idx_map.push(idx);
            }
            Some(UpdateOneof::Transaction(_)) => {
                block.transaction_idx_map.push(idx);
            }
            Some(UpdateOneof::Entry(_)) => {
                block.entry_idx_map.push(idx);
            }
            _ => {
                unreachable!("unsupported update type for block data insertion");
            }
        }
        block.events.push(Box::new(update));
    }

    fn mark_block_as_frozen(&mut self, slot: Slot) {
        let Some(block) = self.active_block_map.remove(&slot) else {
            return;
        };
        self.frozen_block_map.insert(slot, block);
    }

    fn remove_slot(&mut self, slot: Slot) {
        self.active_block_map.remove(&slot);
        self.frozen_block_map.remove(&slot);
    }

    fn finish_slot(&mut self, slot: Slot) -> Option<Block> {
        let acc = self.frozen_block_map.remove(&slot)?;
        Some(acc.finish(slot))
    }
}

#[cfg(test)]
mod tests {
    use {
        super::{BlockMachineOutput, BlockStream},
        futures_util::{Stream, stream},
        solana_commitment_config::CommitmentLevel,
        solana_hash::Hash,
        std::{
            io,
            pin::Pin,
            task::{Context, Poll},
        },
        yellowstone_grpc_proto::geyser::{
            SlotStatus, SubscribeUpdate, SubscribeUpdateAccount, SubscribeUpdateBlockMeta,
            SubscribeUpdateEntry, SubscribeUpdateSlot, SubscribeUpdateTransaction,
            subscribe_update::UpdateOneof,
        },
    };

    fn update(oneof: UpdateOneof, filters: Vec<String>) -> SubscribeUpdate {
        SubscribeUpdate {
            filters,
            created_at: None,
            update_oneof: Some(oneof),
        }
    }

    fn slot_update(slot: u64, parent: Option<u64>, status: SlotStatus) -> SubscribeUpdate {
        update(
            UpdateOneof::Slot(SubscribeUpdateSlot {
                slot,
                parent,
                status: status as i32,
                dead_error: None,
            }),
            vec!["test".to_string()],
        )
    }

    fn entry_update(slot: u64, index: u64) -> SubscribeUpdate {
        update(
            UpdateOneof::Entry(SubscribeUpdateEntry {
                slot,
                index,
                num_hashes: 0,
                hash: Hash::new_unique().to_bytes().to_vec(),
                executed_transaction_count: 1,
                starting_transaction_index: index,
            }),
            vec!["client-filter".to_string()],
        )
    }

    fn tx_update(slot: u64) -> SubscribeUpdate {
        update(
            UpdateOneof::Transaction(SubscribeUpdateTransaction {
                slot,
                ..Default::default()
            }),
            vec!["client-filter".to_string()],
        )
    }

    fn account_update(slot: u64) -> SubscribeUpdate {
        update(
            UpdateOneof::Account(SubscribeUpdateAccount {
                slot,
                ..Default::default()
            }),
            vec!["client-filter".to_string()],
        )
    }

    fn block_meta_update(slot: u64, parent_slot: u64, entries_count: u64) -> SubscribeUpdate {
        let blockhash = bs58::encode(Hash::new_unique().to_bytes()).into_string();
        update(
            UpdateOneof::BlockMeta(SubscribeUpdateBlockMeta {
                slot,
                parent_slot,
                blockhash,
                executed_transaction_count: entries_count,
                entries_count,
                ..Default::default()
            }),
            vec!["test".to_string()],
        )
    }

    fn feed(
        stream: &mut BlockStream<
            stream::Iter<std::vec::IntoIter<Result<SubscribeUpdate, io::Error>>>,
        >,
        ev: SubscribeUpdate,
    ) {
        if stream.machine.handle_new_geyser_event(&ev).is_ok() {
            stream.insert_into_storage(ev);
        }
        stream.process_state_machine_output();
    }

    fn empty_source_stream(
        min_commitment_level: CommitmentLevel,
    ) -> BlockStream<stream::Iter<std::vec::IntoIter<Result<SubscribeUpdate, io::Error>>>> {
        BlockStream::new(
            stream::iter(Vec::<Result<SubscribeUpdate, io::Error>>::new()),
            min_commitment_level,
        )
    }

    #[test]
    fn emits_frozen_block_before_slot_commitment_update() {
        let mut bs = empty_source_stream(CommitmentLevel::Processed);

        feed(
            &mut bs,
            slot_update(10, Some(9), SlotStatus::SlotFirstShredReceived),
        );
        feed(&mut bs, slot_update(10, Some(9), SlotStatus::SlotCompleted));
        feed(&mut bs, entry_update(10, 0));
        feed(&mut bs, tx_update(10));
        feed(&mut bs, account_update(10));
        feed(&mut bs, block_meta_update(10, 9, 1));
        feed(&mut bs, slot_update(10, Some(9), SlotStatus::SlotProcessed));

        let first = bs.pending.pop_front().expect("first output");
        let second = bs.pending.pop_front().expect("second output");

        let BlockMachineOutput::FrozenBlock(block) = first else {
            panic!("expected FrozenBlock first");
        };
        assert_eq!(block.slot, 10);
        assert_eq!(block.entry_len(), 1);
        assert_eq!(block.txn_len(), 1);
        assert_eq!(block.account_len(), 1);

        let BlockMachineOutput::SlotCommitmentUpdate(update) = second else {
            panic!("expected SlotCommitmentUpdate second");
        };
        assert_eq!(update.slot, 10);
        assert_eq!(update.commitment, CommitmentLevel::Processed);
    }

    #[test]
    fn respects_minimum_commitment_filter() {
        let mut bs = empty_source_stream(CommitmentLevel::Confirmed);

        feed(
            &mut bs,
            slot_update(42, Some(41), SlotStatus::SlotFirstShredReceived),
        );
        feed(
            &mut bs,
            slot_update(42, Some(41), SlotStatus::SlotCompleted),
        );
        feed(&mut bs, entry_update(42, 0));
        feed(&mut bs, block_meta_update(42, 41, 1));

        // Processed is below minimum commitment and should produce no output.
        feed(
            &mut bs,
            slot_update(42, Some(41), SlotStatus::SlotProcessed),
        );
        assert!(bs.pending.is_empty());

        // Confirmed reaches minimum commitment and should emit both block and commitment update.
        feed(
            &mut bs,
            slot_update(42, Some(41), SlotStatus::SlotConfirmed),
        );
        assert!(matches!(
            bs.pending.pop_front(),
            Some(BlockMachineOutput::FrozenBlock(_))
        ));
        assert!(matches!(
            bs.pending.pop_front(),
            Some(BlockMachineOutput::SlotCommitmentUpdate(_))
        ));
    }

    #[test]
    fn stream_forwards_source_error_and_end_of_stream() {
        let source = stream::iter(vec![Err(io::Error::other("boom"))]);
        let mut bs = BlockStream::new(source, CommitmentLevel::Processed);
        let waker = futures_util::task::noop_waker();
        let mut cx = Context::from_waker(&waker);

        let first = Pin::new(&mut bs).poll_next(&mut cx);
        assert!(matches!(first, Poll::Ready(Some(Err(_)))));

        let source = stream::iter(Vec::<Result<SubscribeUpdate, io::Error>>::new());
        let mut bs = BlockStream::new(source, CommitmentLevel::Processed);
        let second = Pin::new(&mut bs).poll_next(&mut cx);
        assert!(matches!(second, Poll::Ready(None)));
    }
}
