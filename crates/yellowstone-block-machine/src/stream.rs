use {
    crate::{
        event::{GeyserEventAdapter, GeyserEventInfo},
        state_machine::{
            BlockStateMachineOutput, BlockstoreStats, DeadBlockDetected, DeadletterEvent,
            ForkDetected, FrozenBlock, SlotCommitmentStatusUpdate,
        },
        wrapper::BlocksStateMachineWrapper,
    },
    derive_more::From,
    futures_util::{Stream, TryStream, TryStreamExt},
    rustc_hash::FxHashMap,
    solana_clock::Slot,
    solana_commitment_config::CommitmentLevel,
    solana_hash::HASH_BYTES,
    std::{cmp::Ordering, collections::VecDeque, marker::PhantomData},
};

///
/// A fully reconstructed block, containing all events (accounts, transactions, entries) for a given slot.
///
#[derive(Debug, Clone)]
pub struct Block<E> {
    pub slot: Slot,
    pub blockhash: [u8; HASH_BYTES],
    pub events: Vec<E>,
    pub account_idx_map: Vec<usize>,
    pub transaction_idx_map: Vec<usize>,
    pub entry_idx_map: Vec<usize>,
    pub other_idx_map: Vec<usize>,
}

///
/// A trait for types that can accumulate events into blocks.
///
pub trait BlockAccumulator {
    ///
    /// The type of events that this cumulator can handle. This is typically the same as the `EventT` associated type of the `GeyserEventAdapter` used by the `BlockStream`.
    type EventT;

    ///
    /// Inserts a new event into the block accumulator for the given slot, under the given
    /// [`Bucket`].
    fn add_event(&mut self, event: Self::EventT, slot: Slot, ev_info: &GeyserEventInfo);

    ///
    /// Marks a block as frozen, indicating that it has been fully reconstructed and is ready for processing.
    ///
    /// See [`BlockAccumulator::finish_block`] for how to retrieve the frozen block.
    fn freeze_block(&mut self, frozen_block_info: FrozenBlock);

    ///
    /// Finishes a block and returns it, if it exists. This is typically called when the block has been fully processed and is ready to be consumed.
    ///
    /// # Note
    ///
    /// This function should only return Some if the block was previously `freeze_block`.
    ///
    /// # Idempotency
    ///
    /// This function is NOT idempotent. Calling it multiple times for the same slot will return None after the first call.
    fn finish_block(&mut self, slot: Slot) -> Option<Block<Self::EventT>>;

    ///
    /// Prunes a block from the accumulator, removing all associated events and data for the given slot.
    /// This is typically called when a block is no longer needed, such as when it has been finalized or when a fork has been detected.
    ///
    fn prune_block(&mut self, slot: Slot);
}

impl<E> Block<E> {
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
pub enum BlockMachineOutput<E> {
    ///
    /// A fully reconstructed block, ready for processing.
    ///
    FrozenBlock(Block<E>),
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
/// - `Source`: The underlying source of raw Geyser events, typically a gRPC stream from the Geyser
///   plugin. Its `Ok` item type must match `V::EventT`.
/// - `V`: A [`GeyserEventAdapter`] that knows how to view the events yielded by `Source`. Use
///   `yellowstone_grpc_proto::geyser::SubscribeUpdate` (behind the `dragonsmouth-thin` feature,
///   which implements this trait on itself) or implement [`GeyserEventAdapter`] on your own type to
///   avoid depending on a specific version of `yellowstone-grpc-proto`.
///
pub struct BlockStream<Source, Adaptor, Acc>
where
    Adaptor: GeyserEventAdapter,
{
    pub(crate) min_commitment_level: CommitmentLevel,
    pub(crate) source: Source,
    pub(crate) machine: BlocksStateMachineWrapper,
    pub(crate) storage: Acc,
    pub(crate) pending: VecDeque<BlockMachineOutput<Adaptor::EventT>>,
    pub(crate) _adapter: PhantomData<Adaptor>,
}

impl<Source, Adaptor, Acc> BlockStream<Source, Adaptor, Acc>
where
    Adaptor: GeyserEventAdapter,
{
    pub fn new(source: Source, block_acc: Acc, min_commitment_level: CommitmentLevel) -> Self {
        Self {
            min_commitment_level,
            source,
            machine: BlocksStateMachineWrapper::new_with_slot_gc_tracing(),
            storage: block_acc,
            pending: VecDeque::new(),
            _adapter: PhantomData,
        }
    }
}

// Auto-derivation of `Unpin` doesn't see through the `Adaptor::EventT` associated-type projection
// held (transitively) by `pending`, so it's implemented explicitly here instead.
impl<Source, Adaptor, Acc> Unpin for BlockStream<Source, Adaptor, Acc>
where
    Source: Unpin,
    Adaptor: GeyserEventAdapter,
    Adaptor::EventT: Unpin,
    Acc: Unpin,
{
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

impl<Source, Adaptor, Acc> BlockStream<Source, Adaptor, Acc>
where
    Adaptor: GeyserEventAdapter,
    Acc: BlockAccumulator<EventT = Adaptor::EventT>,
{
    pub fn state_machine_stats(&self) -> BlockstoreStats {
        self.machine.sm.stats()
    }

    fn insert_into_storage(&mut self, event: Adaptor::EventT, ev_info: &GeyserEventInfo) {
        let slot = ev_info.slot();
        self.storage.add_event(event, slot, &ev_info);
    }

    fn on_new_frozen_block(&mut self) {
        // Drain DLQ — clean up slots the state machine gave up on
        while let Some(dlq_event) = self.machine.pop_next_dlq() {
            match dlq_event {
                DeadletterEvent::Incomplete(slot) => {
                    self.storage.prune_block(slot);
                }
            }
        }

        while let Some(slot) = self.machine.pop_slot_gc_trace() {
            self.storage.prune_block(slot);
        }
    }

    fn process_state_machine_output(&mut self) {
        while let Some(output) = self.machine.pop_next_state_machine_output() {
            match output {
                BlockStateMachineOutput::FrozenBlock(frozen_block) => {
                    self.on_new_frozen_block();
                    self.storage.freeze_block(frozen_block);
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
                            if let Some(block) = self.storage.finish_block(slot) {
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
                    self.storage.prune_block(fork_detected.slot);
                    self.pending
                        .push_back(BlockMachineOutput::ForkDetected(fork_detected));
                }
                BlockStateMachineOutput::DeadSlotDetected(dead_block) => {
                    self.storage.prune_block(dead_block.slot);
                    self.pending
                        .push_back(BlockMachineOutput::DeadBlockDetect(dead_block));
                }
                BlockStateMachineOutput::BankCreated(_) => {}
                BlockStateMachineOutput::BankReset(slot) => {
                    self.storage.prune_block(slot);
                }
            }
        }
    }
}

impl<Source, Adaptor, Acc> Stream for BlockStream<Source, Adaptor, Acc>
where
    Source: TryStream<Ok = Adaptor::EventT> + Unpin,
    Adaptor: GeyserEventAdapter,
    Adaptor::EventT: Unpin,
    Acc: BlockAccumulator<EventT = Adaptor::EventT> + Unpin,
{
    type Item = Result<BlockMachineOutput<Adaptor::EventT>, Source::Error>;

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
                    let event_view = match Adaptor::extract_geyser_ev_info(&ev) {
                        Some(ev) => ev,
                        None => continue,
                    };

                    if self
                        .machine
                        .handle_new_geyser_event(event_view.clone())
                        .is_ok()
                    {
                        self.insert_into_storage(ev, &event_view);
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

///
/// Which per-block index map an event's position should be recorded in.
///
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Bucket {
    Account,
    Transaction,
    Entry,
    Other,
}

#[derive(Debug)]
struct BlockBuffer<E> {
    blockhash: [u8; HASH_BYTES],
    events: Vec<E>,
    account_idx_map: Vec<usize>,
    transaction_idx_map: Vec<usize>,
    entry_idx_map: Vec<usize>,
    other_idx_map: Vec<usize>,
}

impl<E> Default for BlockBuffer<E> {
    fn default() -> Self {
        Self {
            blockhash: [0; HASH_BYTES],
            events: Vec::new(),
            account_idx_map: Vec::new(),
            transaction_idx_map: Vec::new(),
            entry_idx_map: Vec::new(),
            other_idx_map: Vec::new(),
        }
    }
}

impl<E> BlockBuffer<E> {
    fn finish(self, slot: Slot) -> Block<E> {
        Block {
            slot,
            blockhash: self.blockhash,
            events: self.events,
            account_idx_map: self.account_idx_map,
            transaction_idx_map: self.transaction_idx_map,
            entry_idx_map: self.entry_idx_map,
            other_idx_map: self.other_idx_map,
        }
    }
}

///
/// An in-memory store for blocks being reconstructed.
///
/// It maintains active blocks (currently being reconstructed) and frozen blocks (fully reconstructed).
pub struct SimpleBlockCumulator<E> {
    active_block_map: FxHashMap<Slot, BlockBuffer<E>>,
    frozen_block_map: FxHashMap<Slot, BlockBuffer<E>>,
}

impl<E> Default for SimpleBlockCumulator<E> {
    fn default() -> Self {
        Self {
            active_block_map: FxHashMap::default(),
            frozen_block_map: FxHashMap::default(),
        }
    }
}

impl<E> BlockAccumulator for SimpleBlockCumulator<E> {
    type EventT = E;

    fn add_event(&mut self, event: E, slot: Slot, ev_info: &GeyserEventInfo) {
        let block = self.active_block_map.entry(slot).or_default();
        let idx = block.events.len();
        match ev_info {
            GeyserEventInfo::Account { .. } => block.account_idx_map.push(idx),
            GeyserEventInfo::Transaction { .. } => block.transaction_idx_map.push(idx),
            GeyserEventInfo::Entry(_) => block.entry_idx_map.push(idx),
            GeyserEventInfo::Other { .. } => block.other_idx_map.push(idx),
            _ => {
                //block meta and slot are ignored
                return;
            }
        }
        block.events.push(event);
    }

    fn freeze_block(&mut self, frozen_block_info: FrozenBlock) {
        let Some(mut block) = self.active_block_map.remove(&frozen_block_info.slot) else {
            return;
        };
        block.blockhash = frozen_block_info.blockhash.to_bytes();
        self.frozen_block_map.insert(frozen_block_info.slot, block);
    }

    fn finish_block(&mut self, slot: Slot) -> Option<Block<E>> {
        let acc = self.frozen_block_map.remove(&slot)?;
        Some(acc.finish(slot))
    }

    fn prune_block(&mut self, slot: Slot) {
        self.active_block_map.remove(&slot);
        self.frozen_block_map.remove(&slot);
    }
}

#[cfg(all(test, feature = "dragonsmouth-thin"))]
mod tests {
    use {
        super::{BlockMachineOutput, BlockStream, SimpleBlockCumulator},
        crate::event::GeyserEventAdapter,
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
            SubscribeUpdate,
            SimpleBlockCumulator<SubscribeUpdate>,
        >,
        ev: SubscribeUpdate,
    ) {
        let ev_info = SubscribeUpdate::extract_geyser_ev_info(&ev).unwrap();
        if stream
            .machine
            .handle_new_geyser_event(ev_info.clone())
            .is_ok()
        {
            stream.insert_into_storage(ev, &ev_info);
        }
        stream.process_state_machine_output();
    }

    fn empty_source_stream(
        min_commitment_level: CommitmentLevel,
    ) -> BlockStream<
        stream::Iter<std::vec::IntoIter<Result<SubscribeUpdate, io::Error>>>,
        SubscribeUpdate,
        SimpleBlockCumulator<SubscribeUpdate>,
    > {
        BlockStream::new(
            stream::iter(Vec::<Result<SubscribeUpdate, io::Error>>::new()),
            SimpleBlockCumulator::default(),
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
        let source = stream::iter(vec![Err::<SubscribeUpdate, _>(io::Error::other("boom"))]);
        let mut bs = BlockStream::<_, SubscribeUpdate, SimpleBlockCumulator<SubscribeUpdate>>::new(
            source,
            SimpleBlockCumulator::default(),
            CommitmentLevel::Processed,
        );
        let waker = futures_util::task::noop_waker();
        let mut cx = Context::from_waker(&waker);

        let first = Pin::new(&mut bs).poll_next(&mut cx);
        assert!(matches!(first, Poll::Ready(Some(Err(_)))));

        let source = stream::iter(Vec::<Result<SubscribeUpdate, io::Error>>::new());
        let mut bs = BlockStream::<_, SubscribeUpdate, SimpleBlockCumulator<SubscribeUpdate>>::new(
            source,
            SimpleBlockCumulator::default(),
            CommitmentLevel::Processed,
        );
        let second = Pin::new(&mut bs).poll_next(&mut cx);
        assert!(matches!(second, Poll::Ready(None)));
    }
}
