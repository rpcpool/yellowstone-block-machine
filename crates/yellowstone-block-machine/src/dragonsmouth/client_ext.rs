use {
    crate::dragonsmouth::{stream::BlockStream, wrapper::RESERVED_FILTER_NAME},
    solana_commitment_config::CommitmentLevel,
    tonic::async_trait,
    yellowstone_grpc_client::{GeyserGrpcClient, GeyserGrpcClientError, GeyserStream},
    yellowstone_grpc_proto::geyser::{
        CommitmentLevel as ProtoCommitmentLevel, SubscribeRequest, SubscribeRequestFilterSlots,
    },
};

pub type GeyserBlockStream = BlockStream<GeyserStream>;

#[async_trait]
pub trait GeyserGrpcExt {
    async fn subscribe_block(
        &mut self,
        subscribe_request: SubscribeRequest,
    ) -> Result<GeyserBlockStream, GeyserGrpcClientError>;
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
    ) -> Result<GeyserBlockStream, GeyserGrpcClientError> {
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

        Ok(BlockStream::new(source, commitment_level))
    }
}
