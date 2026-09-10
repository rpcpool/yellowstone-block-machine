#[cfg(feature = "dragonsmouth")]
pub mod client_ext;
#[cfg(feature = "dragonsmouth-thin")]
pub mod proto_adapter;

#[cfg(feature = "dragonsmouth-thin")]
pub mod block_accumulator;

#[cfg(feature = "test-tools")]
pub mod simulation;

pub const RESERVED_FILTER_NAME: &str = "_block-machine";
