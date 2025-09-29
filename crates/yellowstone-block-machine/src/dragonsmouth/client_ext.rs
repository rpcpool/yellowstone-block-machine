use {
    crate::{
        dragonsmouth::wrapper::{BlocksStateMachineWrapper, RESERVED_FILTER_NAME},
        state_machine::{
            BlockStateMachineOutput, DeadBlockDetected, ForkDetected, SlotCommitmentStatusUpdate,
        },
    },
    derive_more::From,
    futures_util::{Sink, SinkExt, Stream, StreamExt, stream::BoxStream},
    rustc_hash::FxHashMap,
    solana_clock::Slot,
    solana_commitment_config::CommitmentLevel,
    std::{
        cmp::Ordering,
        collections::{HashMap, VecDeque},
        sync::Arc,
    },
    tokio::sync::mpsc,
    tokio_stream::wrappers::ReceiverStream,
    tokio_util::sync::PollSender,
    tonic::async_trait,
    yellowstone_grpc_client::{GeyserGrpcClient, GeyserGrpcClientError, Interceptor},
    yellowstone_grpc_proto::geyser::{
        CommitmentLevel as ProtoCommitmentLevel, SubscribeRequest, SubscribeRequestFilterAccounts,
        SubscribeRequestFilterSlots, SubscribeRequestFilterTransactions, SubscribeUpdate,
        subscribe_update::UpdateOneof,
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
    entry_idx_map: Vec<usize>,
}

impl Block {
    pub fn txn_len(&self) -> usize {
        self.transaction_idx_map.len()
    }

    pub fn account_len(&self) -> usize {
        self.account_idx_map.len()
    }

    pub fn entry_len(&self) -> usize {
        self.entry_idx_map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }

    pub fn len(&self) -> usize {
        self.events.len()
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
    ///
    DeadBlockDetect(DeadBlockDetected),
}

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

        assert!(
            subscribe_request.blocks.is_empty(),
            "custom `blocks` filter is not compatible with block machine"
        );

        assert!(
            subscribe_request.slots.is_empty(),
            "custom `slots` filter is not compatible with block machine"
        );

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

        let machine = BlocksStateMachineWrapper::default();

        let (tx, rx) = mpsc::channel(DEFAULT_SUBSCRIBE_BLOCK_CHANNEL_CAPACITY);

        let driver = AsyncDragonsmouthDriver {
            min_commitment_level: commitment_level,
            source,
            sink: PollSender::new(tx),
            machine,
            storage: InMemoryBlockStore::default(),
        };

        tokio::spawn(async move {
            let _ = driver.run().await;
        });

        Ok(rx)
    }
}

///
/// The driver that connects the Geyser gRPC stream to the DragonsmouthBlockMachine.
///
/// It reads from the gRPC stream, feeds events into the state machine, and sends outputs
/// to the provided sink.
pub struct AsyncDragonsmouthDriver<Sink> {
    min_commitment_level: CommitmentLevel,
    source: BoxStream<'static, Result<SubscribeUpdate, tonic::Status>>,
    sink: Sink,
    machine: BlocksStateMachineWrapper,
    storage: InMemoryBlockStore,
}

#[derive(Debug, thiserror::Error)]
pub enum DragonsmouthDriverError<SnkErr> {
    #[error(transparent)]
    SinkError(SnkErr),
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

impl<Snk, SnkErr> AsyncDragonsmouthDriver<Snk>
where
    Snk: Sink<Result<BlockMachineOutput, BlockMachineError>, Error = SnkErr>
        + Unpin
        + Send
        + 'static,
    SnkErr: std::error::Error + Send,
{
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

    async fn process_state_machine_output(
        &mut self,
        outputs: &mut VecDeque<BlockStateMachineOutput>,
    ) -> Result<(), SnkErr> {
        while let Some(output) = outputs.pop_front() {
            match output {
                BlockStateMachineOutput::FrozenBlock(frozen_block) => {
                    let slot = frozen_block.slot;
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
                                self.sink
                                    .send(Ok(BlockMachineOutput::FrozenBlock(block)))
                                    .await?;
                            }

                            self.sink
                                .send(Ok(BlockMachineOutput::SlotCommitmentUpdate(
                                    commitment_level_update,
                                )))
                                .await?;
                        }
                    }
                }
                BlockStateMachineOutput::ForksDetected(fork_detected) => {
                    self.storage.remove_slot(fork_detected.slot);
                    self.sink
                        .send(Ok(BlockMachineOutput::ForkDetected(fork_detected)))
                        .await?;
                }
                BlockStateMachineOutput::DeadSlotDetected(dead_block) => {
                    self.storage.remove_slot(dead_block.slot);
                    self.sink
                        .send(Ok(BlockMachineOutput::DeadBlockDetect(dead_block)))
                        .await?;
                }
            }
        }
        Ok(())
    }

    pub async fn run(mut self) -> Result<(), DragonsmouthDriverError<SnkErr>> {
        let mut batch = VecDeque::with_capacity(10);

        loop {
            if !batch.is_empty() {
                self.process_state_machine_output(&mut batch)
                    .await
                    .map_err(DragonsmouthDriverError::SinkError)?;
            }

            tokio::select! {
                maybe = self.source.next() => {
                    match maybe {
                        Some(result) => {
                            match result {
                                Ok(ev) => {
                                    self.machine.handle_new_geyser_event(&ev);
                                    self.insert_into_storage(ev);
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
            self.machine.drain_unprocess_output(&mut batch);
        }
        self.machine.drain_unprocess_output(&mut batch);

        self.process_state_machine_output(&mut batch)
            .await
            .map_err(DragonsmouthDriverError::SinkError)?;
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
        block.events.push(Arc::new(update));
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
