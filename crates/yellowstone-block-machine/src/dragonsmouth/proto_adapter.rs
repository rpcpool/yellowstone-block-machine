use {
    crate::event::{
        BlockMetaEvInfo, EntryEvInfo, GeyserEventAdapter, GeyserEventInfo, SlotStatusKind,
        SlotUpdateEvInfo,
    },
    std::str::FromStr,
    yellowstone_grpc_proto::geyser::{SlotStatus, SubscribeUpdate, subscribe_update::UpdateOneof},
};

impl From<SlotStatus> for SlotStatusKind {
    fn from(value: SlotStatus) -> Self {
        match value {
            SlotStatus::SlotFirstShredReceived => SlotStatusKind::FirstShredReceived,
            SlotStatus::SlotCompleted => SlotStatusKind::Completed,
            SlotStatus::SlotCreatedBank => SlotStatusKind::CreatedBank,
            SlotStatus::SlotDead => SlotStatusKind::Dead,
            SlotStatus::SlotProcessed => SlotStatusKind::Processed,
            SlotStatus::SlotConfirmed => SlotStatusKind::Confirmed,
            SlotStatus::SlotFinalized => SlotStatusKind::Finalized,
        }
    }
}

impl GeyserEventAdapter for SubscribeUpdate {
    type EventT = SubscribeUpdate;

    fn extract_geyser_ev_info(event: &SubscribeUpdate) -> Option<GeyserEventInfo> {
        let update_oneof = event.update_oneof.as_ref()?;
        match update_oneof {
            UpdateOneof::Slot(slot_update) => Some(GeyserEventInfo::Slot(SlotUpdateEvInfo {
                slot: slot_update.slot,
                parent: slot_update.parent,
                status: slot_update.status().into(),
                dead_error: slot_update.dead_error.is_some(),
            })),
            UpdateOneof::BlockMeta(block_meta) => {
                Some(GeyserEventInfo::BlockMeta(BlockMetaEvInfo {
                    slot: block_meta.slot,
                    parent_slot: block_meta.parent_slot,
                    entries_count: block_meta.entries_count,
                    executed_transaction_count: block_meta.executed_transaction_count,
                    blockhash: solana_hash::Hash::from_str(&block_meta.blockhash)
                        .expect("blockhash format")
                        .to_bytes(),
                    // Unlike `blockhash`, `parent_blockhash` may legitimately be absent from the
                    // wire (e.g. genesis, or a producer that doesn't report it).
                    parent_blockhash: solana_hash::Hash::from_str(&block_meta.parent_blockhash)
                        .ok()
                        .map(|h| h.to_bytes()),
                    block_time: block_meta
                        .block_time
                        .as_ref()
                        .map(|t| t.timestamp)
                        .unwrap_or(0),
                }))
            }
            UpdateOneof::Entry(entry) => Some(GeyserEventInfo::Entry(EntryEvInfo {
                slot: entry.slot,
                index: entry.index,
                starting_transaction_index: entry.starting_transaction_index,
                executed_transaction_count: entry.executed_transaction_count,
                hash: entry.hash.as_slice().try_into().expect("entry hash length"),
                // filters: event.filters.as_slice(),
            })),
            UpdateOneof::Transaction(tx) => Some(GeyserEventInfo::Transaction { slot: tx.slot }),
            UpdateOneof::Account(account) => Some(GeyserEventInfo::Account { slot: account.slot }),
            UpdateOneof::TransactionStatus(tx) => {
                Some(GeyserEventInfo::Transaction { slot: tx.slot })
            }
            // ev => Some(GeyserEventInfo::Other { slot: None }),
            UpdateOneof::Block(subscribe_update_block) => Some(GeyserEventInfo::Other {
                slot: subscribe_update_block.slot,
            }),
            UpdateOneof::Ping(_) => None,
            UpdateOneof::Pong(_) => None,
        }
    }
}
