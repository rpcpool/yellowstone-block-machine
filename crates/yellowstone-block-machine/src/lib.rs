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
//! # Dragonsmouth integration
//!
//! With the Dragonsmouth integration enabled, you can consume a typed stream of:
//! - `BlockMachineOutput::FrozenBlock`,
//! - `BlockMachineOutput::SlotCommitmentUpdate`,
//! - `BlockMachineOutput::ForkDetected`,
//! - `BlockMachineOutput::DeadBlockDetect`.
//!
//! High-level example:
//!
//! ```ignore
//! use futures_util::StreamExt;
//! use yellowstone_block_machine::dragonsmouth::client_ext::{
//!     BlockMachineOutput, GeyserGrpcExt,
//! };
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
//!             BlockMachineOutput::FrozenBlock(block) => {
//!                 let _ = (block.slot, block.txn_len(), block.account_len(), block.entry_len());
//!             }
//!             BlockMachineOutput::SlotCommitmentUpdate(update) => {
//!                 let _ = (update.slot, update.commitment);
//!             }
//!             BlockMachineOutput::ForkDetected(fork) => {
//!                 let _ = fork.slot;
//!             }
//!             BlockMachineOutput::DeadBlockDetect(dead) => {
//!                 let _ = dead.slot;
//!             }
//!         }
//!     }
//! }
//! ```
//!
//! # Feature flags
//!
//! - `dragonsmouth-thin`: Enables the dragonsmouth module (`stream` + `wrapper`) for custom drivers.
//! - `dragonsmouth`: Enables `client_ext` helpers on top of `dragonsmouth-thin`, including
//!   `GeyserGrpcExt::subscribe_block`.
//!
//! If you are integrating with Yellowstone gRPC directly, `dragonsmouth` is the easiest starting point.
#[cfg(feature = "dragonsmouth-thin")]
pub mod dragonsmouth;
pub mod forks;
pub mod state_machine;
#[cfg(test)]
pub mod testkit;
