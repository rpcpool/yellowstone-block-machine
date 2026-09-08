use {solana_clock::Slot, solana_hash::HASH_BYTES};

///
/// The lifecycle/commitment status of a slot, independent of any specific Geyser wire format.
///
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlotStatusKind {
    FirstShredReceived,
    Completed,
    CreatedBank,
    Dead,
    Processed,
    Confirmed,
    Finalized,
    ///
    /// Any status not recognized by this crate. Keeps the abstraction forward-compatible with
    /// new statuses introduced by a Geyser wire format without requiring a breaking change here.
    ///
    Other,
}

///
/// A borrowed view over a slot lifecycle/commitment update.
///
#[derive(Debug, Clone, Copy)]
pub struct SlotUpdateEvInfo {
    pub slot: Slot,
    pub parent: Option<Slot>,
    pub status: SlotStatusKind,
    pub dead_error: bool,
}

///
/// A borrowed view over a block metadata (summary) update.
///
#[derive(Debug, Clone, Copy)]
pub struct BlockMetaEvInfo {
    pub slot: Slot,
    pub parent_slot: Slot,
    pub entries_count: u64,
    pub executed_transaction_count: u64,
    pub blockhash: [u8; HASH_BYTES],
    pub parent_blockhash: Option<[u8; HASH_BYTES]>,
    ///
    /// Unix timestamp the block was produced at. `0` if the wire didn't report one.
    ///
    pub block_time: i64,
}

///
/// A borrowed view over a block entry update.
///
#[derive(Debug, Clone, Copy)]
pub struct EntryEvInfo {
    pub slot: Slot,
    pub index: u64,
    pub starting_transaction_index: u64,
    pub executed_transaction_count: u64,
    pub hash: [u8; HASH_BYTES],
}

///
/// A borrowed, wire-format-agnostic view over a single Geyser event, produced by a
/// [`GeyserEventAdapter`].
///
/// This is the abstraction boundary that lets [`crate::wrapper::BlocksStateMachineWrapper`]
/// and [`crate::stream::BlockStream`] operate over any event source, not just a specific version
/// of `yellowstone_grpc_proto`.
///
#[derive(Debug, Clone)]
pub enum GeyserEventInfo {
    Slot(SlotUpdateEvInfo),
    BlockMeta(BlockMetaEvInfo),
    Entry(EntryEvInfo),
    Transaction {
        slot: Slot,
    },
    Account {
        slot: Slot,
    },
    ///
    /// Any event kind not used by block reconstruction (or not recognized by this crate).
    ///
    Other {
        slot: Slot,
    },
}

impl GeyserEventInfo {
    pub fn slot(&self) -> Slot {
        match self {
            GeyserEventInfo::Slot(ev) => ev.slot,
            GeyserEventInfo::BlockMeta(ev) => ev.slot,
            GeyserEventInfo::Entry(ev) => ev.slot,
            GeyserEventInfo::Transaction { slot } => *slot,
            GeyserEventInfo::Account { slot } => *slot,
            GeyserEventInfo::Other { slot } => *slot,
        }
    }
}

///
/// Adapts a raw Geyser event (of type [`EventT`](GeyserEventAdapter::EventT)) into a
/// [`GeyserEventInfo`], abstracted away from any specific wire format.
///
/// The implementing type does not have to be [`EventT`](GeyserEventAdapter::EventT) itself — it is
/// only the adapter that knows how to view it. This lets you bridge a Geyser event type you don't
/// own (e.g. `yellowstone_grpc_proto::geyser::SubscribeUpdate` from a version pinned by another
/// crate) by implementing this trait on a small local marker type instead, with `EventT` set to
/// that foreign type. That satisfies Rust's orphan rules, since only the implementing type — not
/// `EventT` — needs to be local to your crate.
///
pub trait GeyserEventAdapter {
    ///
    /// The raw Geyser event type this adapter knows how to view.
    ///
    type EventT;

    ///
    /// Extracts a normalized event view for the state machine, or returns `None` if the event
    /// cannot be represented by this crate's abstraction.
    ///
    fn extract_geyser_ev_info(event: &Self::EventT) -> Option<GeyserEventInfo>;
}
