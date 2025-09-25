use {
    crate::state_machine::{
        BlockSM, BlockStateMachineOuput, BlockSummary, DeadBlockDetected, EntryInfo, ForkDetected,
        SlotCommitmentStatusUpdate, SlotLifecycle, SlotLifecycleUpdate,
    },
    derive_more::From,
    futures_util::{Sink, SinkExt, Stream, StreamExt, stream::BoxStream},
    rustc_hash::FxHashMap,
    solana_clock::Slot,
    solana_commitment_config::CommitmentLevel,
    solana_hash::Hash,
    std::{cmp::Ordering, collections::HashMap, marker::PhantomData, sync::Arc},
    tokio::sync::mpsc,
    tokio_stream::wrappers::ReceiverStream,
    tokio_util::sync::PollSender,
    tonic::async_trait,
    yellowstone_grpc_client::{GeyserGrpcClient, GeyserGrpcClientError, Interceptor},
    yellowstone_grpc_proto::geyser::{
        CommitmentLevel as ProtoCommitmentLevel, SlotStatus, SubscribeRequest,
        SubscribeRequestFilterAccounts, SubscribeRequestFilterSlots,
        SubscribeRequestFilterTransactions, SubscribeUpdate, SubscribeUpdateAccount,
        SubscribeUpdateBlockMeta, SubscribeUpdateEntry, SubscribeUpdateSlot,
        SubscribeUpdateTransaction, subscribe_update::UpdateOneof,
    },
};

pub type BlockMachineResult = Result<BlockMachineOutput, BlockMachineError>;

#[async_trait]
pub trait GeyserGrpcExt<F> {
    fn this(&mut self) -> &mut GeyserGrpcClient<F>;

    async fn subscribe_block(
        &mut self,
        subscribe_request: SubscribeRequest,
    ) -> Result<mpsc::Receiver<BlockMachineResult>, GeyserGrpcClientError>;
}

pub struct SimplifiedSubscribeRequest {
    pub accounts: HashMap<String, SubscribeRequestFilterAccounts>,
    pub transactions: HashMap<String, SubscribeRequestFilterTransactions>,
}

#[derive(Debug, Default)]
struct BlockAccumulator {
    events: Vec<Arc<SubscribeUpdate>>,
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

#[derive(Debug, Clone)]
pub struct Block {
    pub slot: Slot,
    pub events: Vec<Arc<SubscribeUpdate>>,
    account_idx_map: Vec<usize>,
    transaction_idx_map: Vec<usize>,
    #[allow(dead_code)]
    entry_idx_map: Vec<usize>,
}

impl Block {
    pub fn tx_len(&self) -> usize {
        self.transaction_idx_map.len()
    }

    pub fn accoutns_len(&self) -> usize {
        self.account_idx_map.len()
    }

    pub fn len(&self) -> usize {
        self.events.len()
    }

    ///
    /// Returns a projection that allows iterating over account updates in this block.
    ///
    pub fn project_accounts(&self) -> Projection<SubscribeUpdateAccount> {
        Projection {
            events: &self.events,
            index_vec: &self.account_idx_map,
            dest_type: PhantomData,
        }
    }

    ///
    /// Returns a projection that allows iterating over entry updates in this block.
    ///
    pub fn project_transactions(&self) -> Projection<SubscribeUpdateTransaction> {
        Projection {
            events: &self.events,
            index_vec: &self.transaction_idx_map,
            dest_type: PhantomData,
        }
    }
}

///
/// An iterator that yields references to elements of type `T` from a vector of `SubscribeUpdate`
/// based on a provided index vector.
pub struct ProjectionIterator<'a, T> {
    events: &'a Vec<Arc<SubscribeUpdate>>,
    index_vec: &'a [usize],
    dest_type: PhantomData<T>,
    curr: usize,
}

macro_rules! impl_projection_for {
    ($src:path, $dest:ty) => {
        impl<'a> Iterator for ProjectionIterator<'a, $dest> {
            type Item = &'a $dest;

            fn next(&mut self) -> Option<Self::Item> {
                if self.curr < self.index_vec.len() {
                    let i = self.index_vec[self.curr];
                    self.curr += 1;
                    let update_oneof = &self.events[i].update_oneof.as_ref().expect("update_oneof");

                    if let $src(event) = update_oneof {
                        Some(event)
                    } else {
                        panic!("unexpected type at {i}, expected $type");
                    }
                } else {
                    None
                }
            }
        }

        impl<'a> Projection<'a, $dest> {
            pub fn iter(&'a self) -> ProjectionIterator<'a, $dest> {
                ProjectionIterator {
                    events: self.events,
                    index_vec: self.index_vec.as_ref(),
                    dest_type: PhantomData,
                    curr: 0,
                }
            }
        }

        impl<'a> IntoIterator for Projection<'a, $dest> {
            type Item = &'a $dest;
            type IntoIter = ProjectionIterator<'a, $dest>;

            fn into_iter(self) -> Self::IntoIter {
                ProjectionIterator {
                    events: self.events,
                    index_vec: self.index_vec,
                    dest_type: PhantomData,
                    curr: 0,
                }
            }
        }
    };
}

impl_projection_for!(UpdateOneof::Transaction, SubscribeUpdateTransaction);
impl_projection_for!(UpdateOneof::Account, SubscribeUpdateAccount);
impl_projection_for!(UpdateOneof::BlockMeta, SubscribeUpdateBlockMeta);
impl_projection_for!(UpdateOneof::Entry, SubscribeUpdateEntry);

pub struct Projection<'a, T> {
    events: &'a Vec<Arc<SubscribeUpdate>>,
    index_vec: &'a [usize],
    dest_type: PhantomData<T>,
}

impl<'a, T> Clone for Projection<'a, T> {
    fn clone(&self) -> Self {
        Self {
            events: self.events,
            index_vec: self.index_vec,
            dest_type: PhantomData,
        }
    }
}

struct BlockStream {
    inner: ReceiverStream<Result<BlockMachineOutput, BlockMachineError>>,
}

impl Stream for BlockStream {
    type Item = Result<BlockMachineOutput, BlockMachineError>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.inner.poll_next_unpin(cx)
    }
}

pub const DEFAULT_SUBSCRIBE_BLOCK_CHANNEL_CAPACITY: usize = 1_000_000;

pub const RESERVED_FILTER_NAME: &str = "_block-machine";

#[async_trait]
impl<F> GeyserGrpcExt<F> for GeyserGrpcClient<F>
where
    F: Interceptor + Send + Sync + 'static,
{
    fn this(&mut self) -> &mut GeyserGrpcClient<F> {
        self
    }

    async fn subscribe_block(
        &mut self,
        mut subscribe_request: SubscribeRequest,
    ) -> Result<mpsc::Receiver<BlockMachineResult>, GeyserGrpcClientError> {
        let proto_commitment_level =
            ProtoCommitmentLevel::try_from(subscribe_request.commitment.unwrap_or(0))
                .expect("Invalid commitment level in subscribe request");

        let commitment_level = match proto_commitment_level {
            ProtoCommitmentLevel::Processed => CommitmentLevel::Processed,
            ProtoCommitmentLevel::Confirmed => CommitmentLevel::Confirmed,
            ProtoCommitmentLevel::Finalized => CommitmentLevel::Finalized,
        };
        subscribe_request.slots.insert(
            RESERVED_FILTER_NAME.to_owned(),
            SubscribeRequestFilterSlots {
                interslot_updates: Some(true),
                ..Default::default()
            },
        );
        subscribe_request
            .blocks_meta
            .insert(RESERVED_FILTER_NAME.to_owned(), Default::default());

        subscribe_request
            .entry
            .insert(RESERVED_FILTER_NAME.to_owned(), Default::default());

        subscribe_request.commitment = Some(0); // Processed

        let (_sink, source) = self
            .this()
            .subscribe_with_request(Some(subscribe_request))
            .await?;

        let source = source.boxed();
        let sm = BlockSM::default();

        let machine = DragonsmouthBlockMachine {
            minimum_commitment_level: commitment_level,
            block_storage: InMemoryBlockStore::new(),
            sm,
        };

        let (tx, rx) = mpsc::channel(DEFAULT_SUBSCRIBE_BLOCK_CHANNEL_CAPACITY);

        let driver = AsyncDragonsmouthDriver {
            source,
            sink: PollSender::new(tx),
            machine,
        };

        tokio::spawn(async move {
            let _ = driver.run().await;
        });

        Ok(rx)
    }
}

///
/// Errors that can occur in the block machine processing.
///
#[derive(Debug, thiserror::Error)]
pub enum BlockMachineError {
    ///
    /// An error originating from the gRPC stream.
    ///
    #[error(transparent)]
    GrpcError(#[from] tonic::Status),
}

///
/// The different types of outputs produced by the Dragon's mouth block machine.
///
#[derive(Debug, From)]
pub enum BlockMachineOutput {
    ///
    /// A fully reconstructed block, ready for processing.
    ///
    Block(Block),
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
    ///
    DeadBlockDetect(DeadBlockDetected),
}

///
/// The core state machine that processes incoming Geyser events and produces block machine outputs.
/// Mainly a Wrapper around BlockSM and InMemoryBlockStore.
struct DragonsmouthBlockMachine {
    minimum_commitment_level: CommitmentLevel,
    block_storage: InMemoryBlockStore,
    sm: BlockSM,
}

impl From<SubscribeUpdateEntry> for EntryInfo {
    fn from(value: SubscribeUpdateEntry) -> Self {
        Self {
            entry_hash: Hash::new_from_array(value.hash.try_into().expect("entry format")),
            slot: value.slot,
            entry_index: value.index,
            starting_txn_index: value.starting_transaction_index,
            executed_txn_count: value.executed_transaction_count,
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

impl DragonsmouthBlockMachine {
    fn handle_block_entry(&mut self, entry: SubscribeUpdateEntry) {
        let entry_info: EntryInfo = entry.into();
        self.sm.process_event(entry_info.into());
    }

    fn handle_slot_update(&mut self, slot_update: &SubscribeUpdateSlot) {
        let slot_status = slot_update.status();
        const LIFE_CYCLE_STATUS: [SlotStatus; 4] = [
            SlotStatus::SlotFirstShredReceived,
            SlotStatus::SlotCompleted,
            SlotStatus::SlotCreatedBank,
            SlotStatus::SlotDead,
        ];

        if LIFE_CYCLE_STATUS.contains(&slot_status) {
            let lifecycle_update = SlotLifecycleUpdate {
                slot: slot_update.slot,
                parent_slot: slot_update.parent,
                stage: match slot_status {
                    SlotStatus::SlotFirstShredReceived => SlotLifecycle::FirstShredReceived,
                    SlotStatus::SlotCompleted => SlotLifecycle::Completed,
                    SlotStatus::SlotCreatedBank => SlotLifecycle::CreatedBank,
                    SlotStatus::SlotDead => SlotLifecycle::Dead,
                    _ => unreachable!(),
                },
            };
            self.sm.process_event(lifecycle_update.into());
        } else {
            let commitment_level_update = SlotCommitmentStatusUpdate {
                parent_slot: slot_update.parent,
                slot: slot_update.slot,
                commitment: match slot_status {
                    SlotStatus::SlotProcessed => CommitmentLevel::Processed,
                    SlotStatus::SlotConfirmed => CommitmentLevel::Confirmed,
                    SlotStatus::SlotFinalized => CommitmentLevel::Finalized,
                    _ => unreachable!(),
                },
            };

            self.sm.process_event(commitment_level_update.into());
        }
    }

    fn handle_block_meta(&mut self, block_meta: SubscribeUpdateBlockMeta) {
        let bh = bs58::decode(block_meta.blockhash)
            .into_vec()
            .expect("blockhash format");
        let block_summary = BlockSummary {
            slot: block_meta.slot,
            entry_count: block_meta.entries_count,
            executed_transaction_count: block_meta.executed_transaction_count,
            blockhash: Hash::new_from_array(bh.try_into().expect("blockhash length")),
        };
        self.sm.process_event(block_summary.into());
        // Currently not used in block reconstruction
    }

    fn handle_new_geyser_event(&mut self, event: SubscribeUpdate) {
        let SubscribeUpdate {
            filters,
            created_at,
            update_oneof,
        } = event;
        let Some(update_oneof) = update_oneof else {
            return;
        };
        match update_oneof {
            UpdateOneof::Account(acc) => {
                let slot = acc.slot;
                let subscribe_update = SubscribeUpdate {
                    filters,
                    created_at,
                    update_oneof: Some(UpdateOneof::Account(acc)),
                };
                self.block_storage.insert_block_data(slot, subscribe_update);
            }
            UpdateOneof::Slot(subscribe_update_slot) => {
                self.handle_slot_update(&subscribe_update_slot);
            }
            UpdateOneof::Transaction(tx) => {
                let slot = tx.slot;
                let subscribe_update = SubscribeUpdate {
                    filters,
                    created_at,
                    update_oneof: Some(UpdateOneof::Transaction(tx)),
                };
                self.block_storage.insert_block_data(slot, subscribe_update);
            }
            UpdateOneof::BlockMeta(subscribe_update_block_meta) => {
                self.handle_block_meta(subscribe_update_block_meta);
            }
            UpdateOneof::Entry(subscribe_update_entry) => {
                self.handle_block_entry(subscribe_update_entry);
            }
            _ => {
                tracing::trace!("Unsupported update type received: {:?}", update_oneof);
            }
        }
    }

    fn handle_blockstore_output(
        &mut self,
        ev: BlockStateMachineOuput,
        out: &mut Vec<BlockMachineOutput>,
    ) {
        match ev {
            BlockStateMachineOuput::FrozenBlock(info) => {
                self.block_storage.mark_block_as_frozen(info.slot);
            }
            BlockStateMachineOuput::SlotStatus(st) => {
                let ord = compare_commitment(st.commitment, self.minimum_commitment_level);
                if ord == Ordering::Greater || ord == Ordering::Equal {
                    let block = self.block_storage.remove_slot(st.slot);
                    if let Some(block_replay) = block {
                        out.push(block_replay.into());
                    } else {
                        tracing::trace!("No block replay found for slot {}", st.slot);
                    }
                    let commitment_update = SlotCommitmentStatusUpdate {
                        slot: st.slot,
                        parent_slot: st.parent_slot,
                        commitment: st.commitment,
                    };
                    out.push(commitment_update.into());
                }
            }
            BlockStateMachineOuput::ForksDetected(slot) => {
                out.push(BlockMachineOutput::ForkDetected(slot));
            }
            BlockStateMachineOuput::DeadSlotDetected(info) => {
                out.push(BlockMachineOutput::DeadBlockDetect(info));
            }
        }
    }

    fn drain_unprocess_bm_output(&mut self, out: &mut Vec<BlockMachineOutput>) {
        while let Some(ev) = self.sm.pop_next_unprocess_blockstore_update() {
            self.handle_blockstore_output(ev, out);
        }
    }
}

///
/// The driver that connects the Geyser gRPC stream to the DragonsmouthBlockMachine.
///
/// It reads from the gRPC stream, feeds events into the state machine, and sends outputs
/// to the provided sink.
pub struct AsyncDragonsmouthDriver<Sink> {
    source: BoxStream<'static, Result<SubscribeUpdate, tonic::Status>>,
    sink: Sink,
    machine: DragonsmouthBlockMachine,
}

#[derive(Debug, thiserror::Error)]
pub enum DragonsmouthDriverError<SnkErr> {
    #[error(transparent)]
    SinkError(SnkErr),
}

impl<Snk, SnkErr> AsyncDragonsmouthDriver<Snk>
where
    Snk: Sink<Result<BlockMachineOutput, BlockMachineError>, Error = SnkErr>
        + Unpin
        + Send
        + 'static,
    SnkErr: std::error::Error + Send,
{
    pub async fn run(mut self) -> Result<(), DragonsmouthDriverError<SnkErr>> {
        let mut batch = Vec::with_capacity(10);
        loop {
            if !batch.is_empty() {
                while let Some(item) = batch.pop() {
                    self.sink
                        .send(Ok(item))
                        .await
                        .map_err(DragonsmouthDriverError::SinkError)?;
                }
            }

            tokio::select! {
                maybe = self.source.next() => {
                    match maybe {
                        Some(result) => {
                            match result {
                                Ok(ev) => {
                                    self.machine.handle_new_geyser_event(ev);
                                },
                                Err(e) => {
                                    tracing::error!("Geyser stream error: {}", e);
                                    let _ = self.sink.send(Err(BlockMachineError::GrpcError(e))).await;
                                    break;
                                }
                            }
                        }
                        None => {
                            break;
                        }
                    }
                }
            }
            self.machine.drain_unprocess_bm_output(&mut batch);
        }
        self.machine.drain_unprocess_bm_output(&mut batch);
        while let Some(item) = batch.pop() {
            let Ok(_) = self.sink.send(Ok(item)).await else {
                break;
            };
        }
        Ok(())
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
    pub fn new() -> Self {
        Self {
            active_block_map: FxHashMap::default(),
            frozen_block_map: FxHashMap::default(),
        }
    }

    pub fn insert_block_data(&mut self, slot: Slot, update: SubscribeUpdate) {
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
        block.events.push(Arc::new(update));
    }

    pub fn mark_block_as_frozen(&mut self, slot: Slot) {
        let Some(block) = self.active_block_map.remove(&slot) else {
            return;
        };
        self.frozen_block_map.insert(slot, block);
    }

    pub fn remove_slot(&mut self, slot: Slot) -> Option<Block> {
        self.frozen_block_map.remove(&slot).map(|b| b.finish(slot))
    }
}
