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
    solana_clock::{BankId, Slot},
    solana_commitment_config::CommitmentLevel,
    solana_hash::HASH_BYTES,
    std::{cmp::Ordering, collections::VecDeque, marker::PhantomData},
};

///
/// A fully reconstructed block, containing all events (accounts, transactions, entries) for a
/// given bank instance.
///
#[derive(Debug, Clone)]
pub struct Block<Storage> {
    pub slot: Slot,
    pub bank_id: BankId,
    pub blockhash: [u8; HASH_BYTES],
    pub events: Storage,
    ///
    /// The entry count reported by the wire's `BlockMeta` itself -- see
    /// [`FrozenBlock::entries_count`].
    ///
    pub entry_count: u64,
    pub executed_transaction_count: u64,
    pub parent_slot: Slot,
    pub parent_blockhash: [u8; HASH_BYTES],
    ///
    /// Unix timestamp the block was produced at. `0` if the wire didn't report one.
    ///
    pub blocktime_unix_ts: u64,
}

impl<Storage> AsRef<Storage> for Block<Storage> {
    fn as_ref(&self) -> &Storage {
        &self.events
    }
}

///
/// A trait for types that can store events for a block, and provide iterators over those events.
///
pub trait BlockEventStore {
    type EventT;

    type Iter<'a>: Iterator<Item = &'a Self::EventT>
    where
        Self: 'a,
        Self::EventT: 'a;

    type IntoIter: IntoIterator<Item = Self::EventT>;

    fn len(&self) -> usize;

    fn blockhash(&self) -> [u8; HASH_BYTES];

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn iter(&self) -> Self::Iter<'_>;

    fn into_iter(self) -> Self::IntoIter;
}

///
/// A trait for types that can accumulate events into blocks.
///
pub trait BlockAccumulator {
    ///
    /// The type of events that this cumulator can handle. This is typically the same as the `EventT` associated type of the `GeyserEventAdapter` used by the `BlockStream`.
    type EventT;

    ///
    /// The type of storage used to hold the events for a block. This is typically a `Vec<EventT>`, but can be any type that can hold the events for a block.
    type EventStore: BlockEventStore;

    ///
    /// Inserts a new event into the block accumulator for the given bank instance. Callers only
    /// ever invoke this for events that carry a `bank_id` (see [`GeyserEventInfo::bank_id`]) —
    /// content with no bank_id (e.g. a startup/snapshot account) isn't part of live bank
    /// reconstruction and is never routed here.
    fn add_event(&mut self, event: Self::EventT, bank_id: BankId, ev_info: &GeyserEventInfo);

    ///
    /// Marks a bank instance as frozen, indicating that it has been fully reconstructed and is ready for processing.
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
    /// This function is NOT idempotent. Calling it multiple times for the same bank_id will return None after the first call.
    fn finish_block(&mut self, bank_id: BankId) -> Option<Block<Self::EventStore>>;

    ///
    /// Prunes a bank instance from the accumulator, removing all associated events and data for
    /// it. This is typically called when a bank is no longer needed, such as when it lost to a
    /// sibling that resolved to a slot's canonical bank, or when a fork has been detected.
    ///
    fn prune_block(&mut self, bank_id: BankId);

    ///
    /// Pops a bank_id that just became ready for [`BlockAccumulator::finish_block`], if the
    /// implementation stages sealing behind some readiness condition of its own (e.g. requiring
    /// specific accounts to have been observed) rather than sealing unconditionally on
    /// [`BlockAccumulator::freeze_block`].
    ///
    /// [`BlockStream`] calls [`BlockAccumulator::finish_block`] once, in response to the state
    /// machine's own commitment delivery; if that call returns `None` because this
    /// implementation wasn't ready yet, this is how it later tells [`BlockStream`] to retry once
    /// it becomes ready on its own. Implementations that always seal immediately in
    /// `freeze_block` (the common case) can leave this at its default, always-`None`
    /// implementation.
    ///
    fn pop_newly_sealed(&mut self) -> Option<BankId> {
        None
    }
}

enum PendingEvent {
    FrozenBlock(BankId),
    SlotCommitmentUpdate(SlotCommitmentStatusUpdate),
    ForkDetected(ForkDetected),
    DeadBlockDetect(DeadBlockDetected),
}

///
/// The different types of outputs produced by the Dragon's mouth block machine.
///
#[derive(Debug, From)]
pub enum BlockMachineOutput<EventStore> {
    ///
    /// A fully reconstructed block, ready for processing.
    ///
    FrozenBlock(Block<EventStore>),
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
    DeadBlockDetected(DeadBlockDetected),
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
pub struct BlockStream<Source, Adaptor, Acc> {
    min_commitment_level: CommitmentLevel,
    source: Source,
    machine: BlocksStateMachineWrapper,
    storage: Acc,
    pending: VecDeque<PendingEvent>,
    _adapter: PhantomData<Adaptor>,
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

const fn compare_commitment(cl1: CommitmentLevel, cl2: CommitmentLevel) -> Ordering {
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
        if let Some(bank_id) = ev_info.bank_id() {
            self.storage.add_event(event, bank_id, ev_info);
        }
    }

    fn on_new_frozen_block(&mut self) {
        // Drain DLQ — clean up banks the state machine gave up on, or discarded as losers.
        while let Some(dlq_event) = self.machine.pop_next_dlq() {
            match dlq_event {
                DeadletterEvent::Incomplete(bank_id) | DeadletterEvent::Discarded(bank_id) => {
                    self.storage.prune_block(bank_id);
                }
            }
        }

        while let Some(bank_id) = self.machine.pop_bank_gc_trace() {
            self.storage.prune_block(bank_id);
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
                    let bank_id = slot_status.bank_id;
                    let cl = slot_status.commitment;
                    match compare_commitment(cl, self.min_commitment_level) {
                        Ordering::Less => continue,
                        _ => {
                            let commitment_level_update = SlotCommitmentStatusUpdate {
                                parent_slot: slot_status.parent_slot,
                                slot: slot_status.slot,
                                commitment: cl,
                                bank_id,
                            };

                            self.pending.push_back(PendingEvent::FrozenBlock(bank_id));

                            self.pending.push_back(PendingEvent::SlotCommitmentUpdate(
                                commitment_level_update,
                            ));
                        }
                    }
                }
                BlockStateMachineOutput::ForksDetected(fork_detected) => {
                    for bank_id in &fork_detected.bank_ids {
                        self.storage.prune_block(*bank_id);
                    }
                    self.pending
                        .push_back(PendingEvent::ForkDetected(fork_detected));
                }
                BlockStateMachineOutput::DeadSlotDetected(dead_block) => {
                    for bank_id in &dead_block.bank_ids {
                        self.storage.prune_block(*bank_id);
                    }
                    self.pending
                        .push_back(PendingEvent::DeadBlockDetect(dead_block));
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
    type Item = Result<BlockMachineOutput<Acc::EventStore>, Source::Error>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        loop {
            if let Some(pending_ev) = self.pending.pop_front() {
                let output = match pending_ev {
                    PendingEvent::FrozenBlock(bank_id) => {
                        if let Some(block) = self.storage.finish_block(bank_id) {
                            BlockMachineOutput::FrozenBlock(block)
                        } else {
                            continue;
                        }
                    }
                    PendingEvent::SlotCommitmentUpdate(update) => {
                        BlockMachineOutput::SlotCommitmentUpdate(update)
                    }
                    PendingEvent::ForkDetected(fork) => BlockMachineOutput::ForkDetected(fork),
                    PendingEvent::DeadBlockDetect(dead) => {
                        BlockMachineOutput::DeadBlockDetected(dead)
                    }
                };

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
            // Some accumulator implementations stage sealing behind a readiness condition of
            // their own (see `BlockAccumulator::pop_newly_sealed`) -- a bank_id popped here
            // just became ready for `finish_block`, independent of (and possibly later than)
            // whatever `PendingEvent::FrozenBlock` the state machine's own commitment delivery
            // already queued (and which may have found the accumulator not ready yet).
            while let Some(bank_id) = self.storage.pop_newly_sealed() {
                self.pending.push_back(PendingEvent::FrozenBlock(bank_id));
            }
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
    slot: Slot,
    bank_id: BankId,
    blockhash: [u8; HASH_BYTES],
    events: Vec<E>,
    entry_count: u64,
    executed_transaction_count: u64,
    parent_slot: Slot,
    parent_blockhash: [u8; HASH_BYTES],
    blocktime_unix_ts: u64,
}

impl<E> BlockBuffer<E> {
    const fn new(bank_id: BankId, slot: Slot) -> Self {
        Self {
            slot,
            bank_id,
            blockhash: [0; HASH_BYTES],
            events: Vec::new(),
            entry_count: 0,
            executed_transaction_count: 0,
            parent_slot: 0,
            parent_blockhash: [0; HASH_BYTES],
            blocktime_unix_ts: 0,
        }
    }
}

pub struct SimpleBlockStore<E> {
    pub slot: Slot,
    pub blockhash: [u8; HASH_BYTES],
    pub events: Vec<E>,
}

pub struct SimpleBlockStoreIter<'a, E> {
    events: &'a [E],
    idx_map: Option<&'a [usize]>,
    idx_pos: usize,
}

impl<'a, E> Iterator for SimpleBlockStoreIter<'a, E> {
    type Item = &'a E;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(idx_map) = self.idx_map {
            if self.idx_pos < idx_map.len() {
                let idx = idx_map[self.idx_pos];
                self.idx_pos += 1;
                self.events.get(idx)
            } else {
                None
            }
        } else {
            if self.idx_pos < self.events.len() {
                let event = &self.events[self.idx_pos];
                self.idx_pos += 1;
                Some(event)
            } else {
                None
            }
        }
    }
}

impl<E> BlockEventStore for SimpleBlockStore<E> {
    type EventT = E;
    type Iter<'a>
        = SimpleBlockStoreIter<'a, E>
    where
        Self: 'a,
        E: 'a;

    type IntoIter = std::vec::IntoIter<E>;

    fn len(&self) -> usize {
        self.events.len()
    }

    fn blockhash(&self) -> [u8; HASH_BYTES] {
        self.blockhash
    }

    fn iter(&self) -> Self::Iter<'_> {
        SimpleBlockStoreIter {
            events: &self.events,
            idx_map: None,
            idx_pos: 0,
        }
    }

    fn into_iter(self) -> Self::IntoIter {
        self.events.into_iter()
    }
}

impl<E> BlockBuffer<E> {
    fn finish(self) -> Block<SimpleBlockStore<E>> {
        Block {
            slot: self.slot,
            bank_id: self.bank_id,
            blockhash: self.blockhash,
            entry_count: self.entry_count,
            executed_transaction_count: self.executed_transaction_count,
            parent_slot: self.parent_slot,
            parent_blockhash: self.parent_blockhash,
            blocktime_unix_ts: self.blocktime_unix_ts,
            events: SimpleBlockStore {
                slot: self.slot,
                blockhash: self.blockhash,
                events: self.events,
            },
        }
    }
}

///
/// An in-memory store for blocks being reconstructed, keyed by `bank_id` — each bank instance
/// gets its own independent buffer, so competing banks for the same slot never clobber each
/// other's accumulated content.
///
/// It maintains active blocks (currently being reconstructed) and frozen blocks (fully reconstructed).
pub struct SimpleBlockAccumulator<E> {
    active_block_map: FxHashMap<BankId, BlockBuffer<E>>,
    frozen_block_map: FxHashMap<BankId, BlockBuffer<E>>,
}

impl<E> Default for SimpleBlockAccumulator<E> {
    fn default() -> Self {
        Self {
            active_block_map: FxHashMap::default(),
            frozen_block_map: FxHashMap::default(),
        }
    }
}

impl<E> BlockAccumulator for SimpleBlockAccumulator<E> {
    type EventT = E;
    type EventStore = SimpleBlockStore<E>;

    fn add_event(&mut self, event: E, bank_id: BankId, ev_info: &GeyserEventInfo) {
        // Slot/BlockMeta are lifecycle signals the state machine consumes internally -- not
        // part of the block's actual content, so they're never stored here (matches
        // `DragonsmouthBlockCumulator`'s own filtering).
        if matches!(
            ev_info,
            GeyserEventInfo::Slot(_) | GeyserEventInfo::BlockMeta(_)
        ) {
            return;
        }
        let slot = ev_info.slot();
        let block = self
            .active_block_map
            .entry(bank_id)
            .or_insert_with(|| BlockBuffer::new(bank_id, slot));
        block.events.push(event);
    }

    fn freeze_block(&mut self, frozen_block_info: FrozenBlock) {
        let Some(mut block) = self.active_block_map.remove(&frozen_block_info.bank_id) else {
            return;
        };
        block.blockhash = frozen_block_info.blockhash.to_bytes();
        block.entry_count = frozen_block_info.entries_count;
        block.executed_transaction_count = frozen_block_info.executed_transaction_count;
        block.parent_slot = frozen_block_info.parent_slot;
        block.parent_blockhash = frozen_block_info.parent_blockhash.to_bytes();
        block.blocktime_unix_ts = frozen_block_info.block_time;
        self.frozen_block_map
            .insert(frozen_block_info.bank_id, block);
    }

    fn finish_block(&mut self, bank_id: BankId) -> Option<Block<SimpleBlockStore<E>>> {
        let acc = self.frozen_block_map.remove(&bank_id)?;
        Some(acc.finish())
    }

    fn prune_block(&mut self, bank_id: BankId) {
        self.active_block_map.remove(&bank_id);
        self.frozen_block_map.remove(&bank_id);
    }
}

#[cfg(all(test, feature = "dragonsmouth-thin"))]
mod tests {
    use {
        super::{
            BlockEventStore, BlockMachineOutput, BlockStream, PendingEvent, SimpleBlockAccumulator,
            SimpleBlockStore,
        },
        crate::{event::GeyserEventAdapter, state_machine::SlotCommitmentStatusUpdate},
        futures_util::{Stream, stream},
        solana_commitment_config::CommitmentLevel,
        solana_hash::{HASH_BYTES, Hash},
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

    const fn update(oneof: UpdateOneof, filters: Vec<String>) -> SubscribeUpdate {
        SubscribeUpdate {
            filters,
            created_at: None,
            update_oneof: Some(oneof),
        }
    }

    fn slot_update(
        slot: u64,
        parent: Option<u64>,
        status: SlotStatus,
        bank_id: Option<u64>,
    ) -> SubscribeUpdate {
        update(
            UpdateOneof::Slot(SubscribeUpdateSlot {
                slot,
                parent,
                status: status as i32,
                dead_error: None,
                bank_id,
            }),
            vec!["test".to_string()],
        )
    }

    fn entry_update(slot: u64, index: u64, bank_id: u64) -> SubscribeUpdate {
        update(
            UpdateOneof::Entry(SubscribeUpdateEntry {
                slot,
                index,
                num_hashes: 0,
                hash: Hash::new_unique().to_bytes().to_vec(),
                executed_transaction_count: 1,
                starting_transaction_index: index,
                bank_id,
            }),
            vec!["client-filter".to_string()],
        )
    }

    fn tx_update(slot: u64, bank_id: u64) -> SubscribeUpdate {
        update(
            UpdateOneof::Transaction(SubscribeUpdateTransaction {
                slot,
                bank_id,
                ..Default::default()
            }),
            vec!["client-filter".to_string()],
        )
    }

    fn account_update(slot: u64, bank_id: Option<u64>) -> SubscribeUpdate {
        update(
            UpdateOneof::Account(SubscribeUpdateAccount {
                slot,
                bank_id,
                ..Default::default()
            }),
            vec!["client-filter".to_string()],
        )
    }

    fn block_meta_update(
        slot: u64,
        parent_slot: u64,
        entries_count: u64,
        bank_id: u64,
    ) -> SubscribeUpdate {
        let blockhash = bs58::encode(Hash::new_unique().to_bytes()).into_string();
        update(
            UpdateOneof::BlockMeta(SubscribeUpdateBlockMeta {
                slot,
                parent_slot,
                blockhash,
                executed_transaction_count: entries_count,
                entries_count,
                bank_id,
                ..Default::default()
            }),
            vec!["test".to_string()],
        )
    }

    #[allow(clippy::type_complexity)]
    fn feed(
        stream: &mut BlockStream<
            stream::Iter<std::vec::IntoIter<Result<SubscribeUpdate, io::Error>>>,
            SubscribeUpdate,
            SimpleBlockAccumulator<SubscribeUpdate>,
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

    #[allow(clippy::type_complexity)]
    fn empty_source_stream(
        min_commitment_level: CommitmentLevel,
    ) -> BlockStream<
        stream::Iter<std::vec::IntoIter<Result<SubscribeUpdate, io::Error>>>,
        SubscribeUpdate,
        SimpleBlockAccumulator<SubscribeUpdate>,
    > {
        BlockStream::new(
            stream::iter(Vec::<Result<SubscribeUpdate, io::Error>>::new()),
            SimpleBlockAccumulator::default(),
            min_commitment_level,
        )
    }

    #[test]
    fn emits_frozen_block_before_slot_commitment_update() {
        let mut bs = empty_source_stream(CommitmentLevel::Processed);
        let bank_id = 1000;

        feed(
            &mut bs,
            slot_update(10, Some(9), SlotStatus::SlotFirstShredReceived, None),
        );
        feed(
            &mut bs,
            slot_update(10, Some(9), SlotStatus::SlotCompleted, None),
        );
        feed(&mut bs, entry_update(10, 0, bank_id));
        feed(&mut bs, tx_update(10, bank_id));
        feed(&mut bs, account_update(10, Some(bank_id)));
        feed(&mut bs, block_meta_update(10, 9, 1, bank_id));
        feed(
            &mut bs,
            slot_update(10, Some(9), SlotStatus::SlotProcessed, Some(bank_id)),
        );

        let waker = futures_util::task::noop_waker();
        let mut cx = Context::from_waker(&waker);

        let first = Pin::new(&mut bs).poll_next(&mut cx);
        let second = Pin::new(&mut bs).poll_next(&mut cx);

        let Poll::Ready(Some(Ok(BlockMachineOutput::FrozenBlock(block)))) = first else {
            panic!("expected FrozenBlock first");
        };
        assert_eq!(block.slot, 10);
        assert_eq!(block.bank_id, bank_id);
        assert_eq!(block.events.events.len(), 3);

        let Poll::Ready(Some(Ok(BlockMachineOutput::SlotCommitmentUpdate(update)))) = second else {
            panic!("expected SlotCommitmentUpdate second");
        };
        assert_eq!(update.slot, 10);
        assert_eq!(update.bank_id, bank_id);
        assert_eq!(update.commitment, CommitmentLevel::Processed);
    }

    #[test]
    fn respects_minimum_commitment_filter() {
        let mut bs = empty_source_stream(CommitmentLevel::Confirmed);
        let bank_id = 2000;

        feed(
            &mut bs,
            slot_update(42, Some(41), SlotStatus::SlotFirstShredReceived, None),
        );
        feed(
            &mut bs,
            slot_update(42, Some(41), SlotStatus::SlotCompleted, None),
        );
        feed(&mut bs, entry_update(42, 0, bank_id));
        feed(&mut bs, block_meta_update(42, 41, 1, bank_id));

        // Processed is below minimum commitment and should produce no output.
        feed(
            &mut bs,
            slot_update(42, Some(41), SlotStatus::SlotProcessed, Some(bank_id)),
        );

        let waker = futures_util::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        let none_after_processed = Pin::new(&mut bs).poll_next(&mut cx);
        assert!(matches!(none_after_processed, Poll::Ready(None)));

        // Confirmed reaches minimum commitment and should emit both block and commitment update.
        feed(
            &mut bs,
            slot_update(42, Some(41), SlotStatus::SlotConfirmed, Some(bank_id)),
        );

        let first = Pin::new(&mut bs).poll_next(&mut cx);
        assert!(matches!(
            first,
            Poll::Ready(Some(Ok(BlockMachineOutput::FrozenBlock(_))))
        ));

        let second = Pin::new(&mut bs).poll_next(&mut cx);
        assert!(matches!(
            second,
            Poll::Ready(Some(Ok(BlockMachineOutput::SlotCommitmentUpdate(_))))
        ));

        let third = Pin::new(&mut bs).poll_next(&mut cx);
        assert!(matches!(third, Poll::Ready(None)));
    }

    #[test]
    fn stream_forwards_source_error_and_end_of_stream() {
        let source = stream::iter(vec![Err::<SubscribeUpdate, _>(io::Error::other("boom"))]);
        let mut bs =
            BlockStream::<_, SubscribeUpdate, SimpleBlockAccumulator<SubscribeUpdate>>::new(
                source,
                SimpleBlockAccumulator::default(),
                CommitmentLevel::Processed,
            );
        let waker = futures_util::task::noop_waker();
        let mut cx = Context::from_waker(&waker);

        let first = Pin::new(&mut bs).poll_next(&mut cx);
        assert!(matches!(first, Poll::Ready(Some(Err(_)))));

        let source = stream::iter(Vec::<Result<SubscribeUpdate, io::Error>>::new());
        let mut bs =
            BlockStream::<_, SubscribeUpdate, SimpleBlockAccumulator<SubscribeUpdate>>::new(
                source,
                SimpleBlockAccumulator::default(),
                CommitmentLevel::Processed,
            );
        let second = Pin::new(&mut bs).poll_next(&mut cx);
        assert!(matches!(second, Poll::Ready(None)));
    }

    #[test]
    fn simple_block_store_empty_iterators_are_empty() {
        let store = SimpleBlockStore::<u64> {
            slot: 99,
            blockhash: [0; HASH_BYTES],
            events: Vec::new(),
        };

        assert!(store.is_empty());
        assert_eq!(store.len(), 0);
        assert_eq!(store.iter().count(), 0);
    }

    #[test]
    fn simple_block_store_partition_iterators_return_expected_events() {
        let store = SimpleBlockStore {
            slot: 7,
            blockhash: [42; HASH_BYTES],
            events: vec![10_u64, 11, 12, 13, 14],
        };

        let all: Vec<u64> = store.iter().copied().collect();

        assert_eq!(all, vec![10, 11, 12, 13, 14]);
        assert_eq!(store.blockhash(), [42; HASH_BYTES]);
    }

    #[test]
    fn skips_missing_frozen_block_and_emits_following_commitment_update() {
        let mut bs = empty_source_stream(CommitmentLevel::Processed);

        // Simulate a pending FrozenBlock for a bank_id that no longer exists in storage,
        // followed by a valid commitment update for the same bank.
        bs.pending.push_back(PendingEvent::FrozenBlock(77));
        bs.pending.push_back(PendingEvent::SlotCommitmentUpdate(
            SlotCommitmentStatusUpdate {
                parent_slot: Some(76),
                slot: 77,
                commitment: CommitmentLevel::Processed,
                bank_id: 77,
            },
        ));

        let waker = futures_util::task::noop_waker();
        let mut cx = Context::from_waker(&waker);

        let first = Pin::new(&mut bs).poll_next(&mut cx);
        assert!(matches!(
            first,
            Poll::Ready(Some(Ok(BlockMachineOutput::SlotCommitmentUpdate(_))))
        ));

        let second = Pin::new(&mut bs).poll_next(&mut cx);
        assert!(matches!(second, Poll::Ready(None)));
    }
}
