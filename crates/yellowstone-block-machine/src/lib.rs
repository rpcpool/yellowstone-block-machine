//! Yellowstone Block Machine
//!
//! Sans-IO state machine utilities for reconstructing Solana blocks from Yellowstone Geyser events.
//!
//! The crate provides:
//! - deterministic per-slot reconstruction from replay + consensus signals,
//! - fork and dead-slot detection,
//! - commitment-level progression updates.
//!
//! # Why this crate exists
//!
//! Raw Geyser streams can arrive in surprising orders. For example, lifecycle and commitment updates
//! may not always line up with block metadata arrival timing. This crate centralizes those rules so
//! downstream consumers can process reconstructed blocks through a stable API.
//!
//! # Core model
//!
//! The state machine consumes:
//! 1. slot lifecycle updates,
//! 2. block entries,
//! 3. block metadata (summary),
//! 4. commitment updates.
//!
//! It emits `BlockStateMachineOutput` values, including frozen blocks, slot status updates, and
//! fork/dead-slot signals.
//!
//! # Wire-format independence
//!
//! [`stream::BlockStream`] and [`wrapper::BlocksStateMachineWrapper`] are generic over any
//! [`event::GeyserEventAdapter`] — they do not depend on any specific version of
//! `yellowstone-grpc-proto`. This crate ships an implementation of [`event::GeyserEventAdapter`] for
//! `yellowstone_grpc_proto::geyser::SubscribeUpdate` behind the `dragonsmouth-thin` feature; if your
//! project is pinned to a different major version of `yellowstone-grpc-proto` (or another Geyser
//! wire format entirely), implement [`event::GeyserEventAdapter`] on your own (possibly zero-sized)
//! marker type instead of enabling that feature — the implementing type doesn't have to be the
//! event type itself, which keeps this legal under Rust's orphan rules even when neither the trait
//! nor the event type is local to your crate — and reuse the reconstruction machinery unchanged.
//!
//! # Dragonsmouth integration
//!
//! With the Dragonsmouth integration enabled, you can consume a typed stream of:
//! - `BlockStreamEvent::FrozenBlock`,
//! - `BlockStreamEvent::SlotCommitmentUpdate`,
//! - `BlockStreamEvent::ForkDetected`,
//! - `BlockStreamEvent::DeadBlockDetected`.
//!
//! High-level example:
//!
//! ```no_run
//! use futures_util::StreamExt;
//! use yellowstone_block_machine::dragonsmouth::client_ext::{BlockStreamEvent, GeyserGrpcExt};
//! use yellowstone_grpc_client::GeyserGrpcBuilder;
//! use yellowstone_grpc_proto::geyser::{CommitmentLevel, SubscribeRequest};
//!
//! async fn run(mut client: yellowstone_grpc_client::GeyserGrpcClient) {
//!     let request = SubscribeRequest {
//!         commitment: Some(CommitmentLevel::Confirmed as i32),
//!         ..Default::default()
//!     };
//!     let mut stream = client.subscribe_block(request).await.expect("subscribe_block");
//!
//!     while let Some(item) = stream.next().await {
//!         match item.expect("stream item") {
//!             BlockStreamEvent::FrozenBlock(block) => {
//!                 let _ = (
//!                     block.slot(),
//!                     block.bank_id(),
//!                     block.entry_count(),
//!                     block.parent_slot(),
//!                     block.parent_blockhash(),
//!                     block.blocktime_unix_ts(),
//!                 );
//!             }
//!             BlockStreamEvent::SlotCommitmentUpdate(update) => {
//!                 let _ = (update.slot, update.commitment);
//!             }
//!             BlockStreamEvent::ForkDetected(fork) => {
//!                 let _ = fork.slot;
//!             }
//!             BlockStreamEvent::DeadBlockDetected(dead) => {
//!                 let _ = dead.slot;
//!             }
//!         }
//!     }
//! }
//! ```
//!
//! # Feature flags
//!
//! - `dragonsmouth-thin`: Enables the bundled [`event::GeyserEventAdapter`] impl for
//!   `yellowstone_grpc_proto::geyser::SubscribeUpdate` and re-exports `yellowstone_grpc_proto`.
//!   Not required if you implement [`event::GeyserEventAdapter`] yourself.
//! - `dragonsmouth`: Enables `client_ext` helpers on top of `dragonsmouth-thin`, including
//!   `GeyserGrpcExt::subscribe_block`, and re-exports `yellowstone_grpc_client`.
//!
//! If you are integrating with Yellowstone gRPC directly, `dragonsmouth` is the easiest starting point.
#[cfg(any(feature = "dragonsmouth", feature = "dragonsmouth-thin"))]
pub mod dragonsmouth;
pub mod event;
pub mod forks;
pub mod state_machine;
pub mod stream;
#[cfg(test)]
pub mod testkit;
pub mod wrapper;

#[cfg(feature = "dragonsmouth")]
pub use yellowstone_grpc_client;
#[cfg(feature = "dragonsmouth-thin")]
pub use yellowstone_grpc_proto;
