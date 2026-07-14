# Yellowstone Block Machine

State machine utilities for reconstructing Solana blocks from Yellowstone Geyser streams.

This repository contains:

- `yellowstone-block-machine`: the core sans-IO crate.
- `examples/dragonsmouth`: a runnable gRPC integration example.

## Why this exists

Reconstructing full Solana blocks from Geyser is subtle because events may arrive out of order and slot lifecycle updates have edge-case behavior. This project centralizes those rules so consumers can:

- reconstruct complete blocks,
- detect forks and dead slots,
- process commitment updates with deterministic ordering.

## Repository layout

- `crates/yellowstone-block-machine`: library crate.
- `examples/dragonsmouth`: CLI example using Yellowstone gRPC.

## Features

Feature definitions live in `crates/yellowstone-block-machine/Cargo.toml`.

- `dragonsmouth-thin`: enables the bundled Yellowstone proto adapter.
- `dragonsmouth`: enables client extensions on top of `dragonsmouth-thin`.

Use the full integration:

```toml
[dependencies]
yellowstone-block-machine = { version = "0.8", features = ["dragonsmouth"] }
```

Use only the proto adapter:

```toml
[dependencies]
yellowstone-block-machine = { version = "0.8", default-features = false, features = ["dragonsmouth-thin"] }
```

## Run the Dragonsmouth example

From repo root:

```bash
cargo run -p dragonsmouth -- --config path/to/config.yaml --samples 10
```

Config example:

```yaml
endpoint: "https://your-yellowstone-endpoint"
x_token: "optional-auth-token"
```

`x-token` is also accepted as an alias.

## Minimal usage example

```rust
use futures_util::StreamExt;
use yellowstone_block_machine::dragonsmouth::client_ext::GeyserGrpcExt;
use yellowstone_block_machine::stream::BlockMachineOutput;
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

    let mut stream = geyser.subscribe_block(request).await?;
    while let Some(item) = stream.next().await {
        match item? {
            BlockMachineOutput::FrozenBlock(block) => {
                println!(
                    "slot={} tx={} accounts={} entries={}",
                    block.slot,
                    block.txn_len(),
                    block.account_len(),
                    block.entry_len(),
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

## Ordering guarantees

For a given slot:

1. `FrozenBlock` is emitted before any commitment update for that slot.
2. Commitment updates below the configured minimum are skipped.
3. Fork/dead-slot outputs prune buffered data for the affected slot.

## Further reading

- https://docs.rs/yellowstone-block-machine/latest/yellowstone_block_machine/
- `crates/yellowstone-block-machine/README.md`