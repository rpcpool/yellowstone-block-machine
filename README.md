# Yellowstone Block Machine

State machine utilities for reconstructing Solana blocks from Yellowstone Geyser streams.

This repository contains:

- The `yellowstone-block-machine` crate (core sans-io state machine).
- A working `dragonsmouth` example that connects to a Yellowstone gRPC endpoint and streams reconstructed blocks.

## Why this project exists

Reconstructing a Solana block from Geyser events is tricky because events can arrive in partial order and slot lifecycle updates have important quirks. The block machine centralizes these rules so integrations can:

- Rebuild complete blocks from account, transaction, entry, and block metadata updates.
- Detect forks and dead slots.
- Track slot commitment updates.

## Repository layout

- `crates/yellowstone-block-machine`: core library.
- `examples/dragonsmouth`: runnable example integration using Yellowstone gRPC.

## Core crate overview

The core crate (`crates/yellowstone-block-machine`) is designed as a sans-io state machine:

- You feed it events.
- It emits typed outputs (frozen blocks, commitment updates, fork/dead slot signals).

This makes it easy to embed in different transport layers and runtimes while keeping block reconstruction logic in one place.

See crate-level docs and source for details:

- `crates/yellowstone-block-machine/src/lib.rs`
- `crates/yellowstone-block-machine/src/state_machine.rs`

## Dragonsmouth support

The crate includes optional Dragonsmouth/Yellowstone gRPC integration under feature flags.

When enabled, you can use extensions exposed from:

- `yellowstone_block_machine::dragonsmouth`

The example app in `examples/dragonsmouth` demonstrates:

- Building a gRPC client (`yellowstone-grpc-client`).
- Subscribing to Geyser updates.
- Feeding the stream into block reconstruction.
- Printing reconstructed block stats and lifecycle events.

Main example entrypoint:

- `examples/dragonsmouth/src/main.rs`

## Feature flags

Feature definitions are in `crates/yellowstone-block-machine/Cargo.toml`.

- `dragonsmouth`: full Dragonsmouth support (includes client + tonic).
- `dragonsmouth-thin`: proto-only Dragonsmouth support.
- `tonic`: tonic transport pieces.

### Enable in your `Cargo.toml`

Use the full integration:

```toml
[dependencies]
yellowstone-block-machine = { version = "0.5", features = ["dragonsmouth"] }
```

Use a lighter setup (proto types only):

```toml
[dependencies]
yellowstone-block-machine = { version = "0.5", default-features = false, features = ["dragonsmouth-thin"] }
```

If you need tonic pieces explicitly:

```toml
[dependencies]
yellowstone-block-machine = { version = "0.5", features = ["tonic"] }
```

## Run the Dragonsmouth example

From repository root:

```bash
cargo run -p dragonsmouth -- --config path/to/config.yaml --samples 10
```

Expected config shape:

```yaml
endpoint: "https://your-yellowstone-endpoint"
x_token: "optional-auth-token"
```

The example processes a stream and prints:

- Frozen block summaries.
- Slot commitment updates.
- Fork/dead slot detections.

## Code example

Minimal async example using the `dragonsmouth` feature:

```rust
use futures_util::StreamExt;
use yellowstone_block_machine::dragonsmouth::client_ext::{
	BlockMachineOutput, GeyserGrpcExt,
};
use yellowstone_grpc_client::{ClientTlsConfig, GeyserGrpcBuilder};
use yellowstone_grpc_proto::geyser::{CommitmentLevel, SubscribeRequest};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
	let mut geyser = GeyserGrpcBuilder::from_shared("https://your-yellowstone-endpoint".into())?
		.tls_config(ClientTlsConfig::new().with_native_roots())?
		.connect()
		.await?;

	let request = SubscribeRequest {
		commitment: Some(CommitmentLevel::Confirmed as i32),
		..Default::default()
	};

	let mut block_stream = geyser.subscribe_block(request).await?;

	while let Some(item) = block_stream.next().await {
		match item? {
			BlockMachineOutput::FrozenBlock(block) => {
				println!(
					"slot={} tx={} accounts={} entries={}",
					block.slot,
					block.txn_len(),
					block.account_len(),
					block.entry_len()
				);
			}
			BlockMachineOutput::SlotCommitmentUpdate(update) => {
				println!("slot={} commitment={:?}", update.slot, update.commitment);
			}
			BlockMachineOutput::ForkDetected(fork) => {
				println!("fork detected: slot={}", fork.slot);
			}
			BlockMachineOutput::DeadBlockDetect(dead) => {
				println!("dead block detected: slot={}", dead.slot);
			}
		}
	}

	Ok(())
}
```

## Notes about event semantics

The crate documentation explains important lifecycle quirks, for example:

- `BANK_CREATED` can appear before `FIRST_SHRED_RECEIVED`.
- `DEAD` can appear at any point.
- `COMPLETED` and commitment updates may not arrive in the order you expect.

These are exactly the cases the state machine is meant to normalize.

## Ordering guarantees

The crate is designed to handle out-of-order upstream Geyser events and still provide deterministic output ordering for consumers.

Guaranteed behaviors:

1. For a given slot, `FrozenBlock` is emitted before any slot commitment update (PROCESSED, CONFIRMED, FINALIZED) for that same slot.
2. If a higher commitment arrives before lower levels (for example `Confirmed` before `Processed`), the state machine backfills missing lower commitments before emitting the higher one.
3. In the Dragonsmouth stream adapter, `SlotCommitmentUpdate` is only emitted after the corresponding block has been materialized and emitted as `FrozenBlock`.
4. Commitment filtering is monotonic: with a minimum commitment configured, updates below that threshold are skipped, and updates at or above threshold preserve commitment progression order.

In short: treat upstream order as eventually consistent, and rely on block-machine outputs for per-slot ordering semantics.

## Fork detection

When subscribed to a valid Geyser stream, the crate can detect forks, which are different from dead slots.

Note: all dead slots are forks, but not all forks are dead slots.

Dead slots are slots that contain invalid transaction signatures or invalid entry hashes.

Forked slots can be fully valid and respect protocol rules, but still be discarded by the network during consensus resolution.

## Fill missing confirmed updates

If you subscribe to raw Geyser events, you may notice missing `Confirmed` commitment updates.
Because this crate tracks Solana's fork graph, it can infer and emit missing confirmed updates so downstream consumers do not need to handle that gap manually.

## Further reading

- Crate docs: https://docs.rs/yellowstone-block-machine/latest/yellowstone_block_machine/
- Core crate README: `crates/yellowstone-block-machine/README.md`