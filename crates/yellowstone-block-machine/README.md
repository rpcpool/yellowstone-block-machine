# Sans-IO Solana Block Reconstruction State Machine

`yellowstone-block-machine` provides a [sans-IO](https://sans-io.readthedocs.io/) state machine to reconstruct Solana blocks from Geyser events.

The state machine encodes ordering and lifecycle rules that are easy to miss when consuming raw Geyser streams directly.

## What it does

- Reconstructs per-slot blocks from replay and metadata signals.
- Emits commitment progression updates.
- Detects forks and dead slots.

## Wire-format agnostic design

The core stream and wrapper are generic over `event::GeyserEventAdapter`, so you can use this crate with:

- the bundled Yellowstone proto adapter (`dragonsmouth-thin` feature), or
- your own adapter for a different event type/version.

This keeps block-reconstruction logic reusable without forcing all consumers onto a specific `yellowstone-grpc-proto` version.

## Feature flags

- `dragonsmouth-thin`: enables the built-in adapter for `yellowstone_grpc_proto::geyser::SubscribeUpdate`.
- `dragonsmouth`: enables gRPC client extensions (`GeyserGrpcExt`) on top of `dragonsmouth-thin`.

## Dragonsmouth integration

See docs.rs for full API docs and examples:

- https://docs.rs/yellowstone-block-machine/latest/yellowstone_block_machine/

You can also run the repository example:

- https://github.com/rpcpool/yellowstone-block-machine/tree/main/examples/dragonsmouth
