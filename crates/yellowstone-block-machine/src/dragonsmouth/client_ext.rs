use {
    crate::{
        dragonsmouth::{RESERVED_FILTER_NAME, block_accumulator::DragonsmouthBlockCumulator},
        state_machine::{DeadBlockDetected, ForkDetected, SlotCommitmentStatusUpdate},
        stream::{
            Block, BlockEventStore, BlockMachineOutput, BlockStream, SimpleBlockStore,
            SimpleBlockStoreIter,
        },
    },
    futures_util::Stream,
    solana_clock::Slot,
    solana_commitment_config::CommitmentLevel,
    std::task::ready,
    tonic::async_trait,
    yellowstone_grpc_client::{GeyserGrpcClient, GeyserGrpcClientError, GeyserStream},
    yellowstone_grpc_proto::geyser::{
        CommitmentLevel as ProtoCommitmentLevel, SubscribeRequest, SubscribeRequestFilterSlots,
        SubscribeUpdate,
    },
};

///
/// A stream of [`BlockStreamEvent`] events produced by the block machine, adapted to the `SubscribeUpdate` type used by the gRPC client.
pub struct DragonsmouthBlockStream {
    inner: BlockStream<GeyserStream, SubscribeUpdate, DragonsmouthBlockCumulator>,
}

pub struct DragonsmouthBlock {
    inner: Block<SimpleBlockStore<SubscribeUpdate>>,
}

impl DragonsmouthBlock {
    pub fn slot(&self) -> Slot {
        self.inner.slot
    }

    ///
    /// The entry count reported by the wire's `BlockMeta` itself -- independent of however many
    /// `Entry` events this crate's own sans-io core happened to buffer.
    ///
    pub fn entry_count(&self) -> u64 {
        self.inner.entry_count
    }

    pub fn executed_transaction_count(&self) -> u64 {
        self.inner.executed_transaction_count
    }

    pub fn parent_slot(&self) -> Slot {
        self.inner.parent_slot
    }

    pub fn parent_blockhash(&self) -> Option<[u8; solana_hash::HASH_BYTES]> {
        self.inner.parent_blockhash
    }

    ///
    /// Unix timestamp the block was produced at. `0` if the wire didn't report one.
    ///
    pub fn blocktime_unix_ts(&self) -> u64 {
        self.inner.blocktime_unix_ts
    }
}

impl From<Block<SimpleBlockStore<SubscribeUpdate>>> for DragonsmouthBlock {
    fn from(block: Block<SimpleBlockStore<SubscribeUpdate>>) -> Self {
        Self { inner: block }
    }
}

impl BlockEventStore for DragonsmouthBlock {
    type EventT = SubscribeUpdate;

    type Iter<'a> = SimpleBlockStoreIter<'a, SubscribeUpdate>;

    type IntoIter = std::vec::IntoIter<SubscribeUpdate>;

    fn len(&self) -> usize {
        self.inner.as_ref().len()
    }

    fn iter(&self) -> Self::Iter<'_> {
        self.inner.as_ref().iter()
    }

    fn account_iter(&self) -> Self::Iter<'_> {
        self.inner.as_ref().account_iter()
    }

    fn transaction_iter(&self) -> Self::Iter<'_> {
        self.inner.as_ref().transaction_iter()
    }

    fn entry_iter(&self) -> Self::Iter<'_> {
        self.inner.as_ref().entry_iter()
    }

    fn other_iter(&self) -> Self::Iter<'_> {
        self.inner.as_ref().other_iter()
    }

    fn into_iter(self) -> Self::IntoIter {
        self.inner.events.into_iter()
    }
}

pub enum BlockStreamEvent {
    ///
    /// A fully reconstructed block, ready for processing.
    ///
    FrozenBlock(DragonsmouthBlock),
    ///
    /// An update to the commitment status of a slot, indicating whether it has been confirmed, finalized, or is still in progress.
    SlotCommitmentUpdate(SlotCommitmentStatusUpdate),
    ///
    /// A fork has been detected in the blockchain, indicating that a previously accepted block has been replaced by a different block at the same slot.
    ForkDetected(ForkDetected),
    ///
    /// A dead block has been detected, indicating that a block is no longer part of the canonical chain and should be discarded.
    DeadBlockDetected(DeadBlockDetected),
}

impl Stream for DragonsmouthBlockStream {
    type Item = Result<BlockStreamEvent, BlockMachineError>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let inner = unsafe { self.map_unchecked_mut(|s| &mut s.inner) };
        let poll = ready!(inner.poll_next(cx));
        match poll {
            Some(Ok(output)) => {
                let output2 = match output {
                    BlockMachineOutput::FrozenBlock(block) => {
                        let block2 = DragonsmouthBlock::from(block);
                        BlockStreamEvent::FrozenBlock(block2)
                    }
                    BlockMachineOutput::SlotCommitmentUpdate(slot_commitment_status_update) => {
                        BlockStreamEvent::SlotCommitmentUpdate(slot_commitment_status_update)
                    }
                    BlockMachineOutput::ForkDetected(fork_detected) => {
                        BlockStreamEvent::ForkDetected(fork_detected)
                    }
                    BlockMachineOutput::DeadBlockDetected(dead_block_detected) => {
                        BlockStreamEvent::DeadBlockDetected(dead_block_detected)
                    }
                };
                std::task::Poll::Ready(Some(Ok(output2)))
            }
            Some(Err(e)) => std::task::Poll::Ready(Some(Err(BlockMachineError::GrpcError(e)))),
            None => std::task::Poll::Ready(None),
        }
    }
}

#[async_trait]
pub trait GeyserGrpcExt {
    async fn subscribe_block(
        &mut self,
        subscribe_request: SubscribeRequest,
    ) -> Result<DragonsmouthBlockStream, GeyserGrpcClientError>;
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

#[async_trait]
impl GeyserGrpcExt for GeyserGrpcClient {
    ///
    /// Subscribes to the gRPC stream and returns a stream of block machine outputs.
    /// The provided `SubscribeRequest` will be modified to ensure compatibility with the block machine's
    /// processing logic. Specifically, it will enforce the presence of the reserved filter and set the commitment level to `Processed`.
    ///
    /// The block machine will internally filter and process events based on the minimum commitment level specified in the original `SubscribeRequest`.
    ///
    ///
    async fn subscribe_block(
        &mut self,
        mut subscribe_request: SubscribeRequest,
    ) -> Result<DragonsmouthBlockStream, GeyserGrpcClientError> {
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

        let (_sink, source) = self.subscribe_with_request(Some(subscribe_request)).await?;

        let block_stream = BlockStream::new(source, Default::default(), commitment_level);
        let dragonsmouth_block_stream = DragonsmouthBlockStream {
            inner: block_stream,
        };

        Ok(dragonsmouth_block_stream)
    }
}
