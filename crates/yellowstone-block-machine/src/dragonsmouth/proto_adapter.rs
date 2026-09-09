use {
    crate::{
        dragonsmouth::block_accumulator::SYSVAR_PROGRAM_ID,
        event::{
            BlockMetaEvInfo, EntryEvInfo, GeyserEventAdapter, GeyserEventInfo, SlotStatusKind,
            SlotUpdateEvInfo,
        },
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
                bank_id: slot_update.bank_id,
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
                    // Lenient, unlike `blockhash` above -- genesis's parent_blockhash is empty,
                    // not a valid base58 hash.
                    parent_blockhash: solana_hash::Hash::from_str(&block_meta.parent_blockhash)
                        .map(|h| h.to_bytes())
                        .unwrap_or([0; solana_hash::HASH_BYTES]),
                    block_time: block_meta
                        .block_time
                        .map_or(0, |t| t.timestamp.max(0) as u64),
                    bank_id: block_meta.bank_id,
                }))
            }
            UpdateOneof::Entry(entry) => Some(GeyserEventInfo::Entry(EntryEvInfo {
                slot: entry.slot,
                index: entry.index,
                starting_transaction_index: entry.starting_transaction_index,
                executed_transaction_count: entry.executed_transaction_count,
                hash: entry.hash.as_slice().try_into().expect("entry hash length"),
                bank_id: entry.bank_id,
                // filters: event.filters.as_slice(),
            })),
            UpdateOneof::Transaction(tx) => Some(GeyserEventInfo::BankData {
                slot: tx.slot,
                bank_id: tx.bank_id,
            }),
            UpdateOneof::Account(account) => {
                let bank_id = account.bank_id?;
                // `subscribe_block` forces a subscription to every sysvar-owned account (see
                // `SYSVAR_PROGRAM_ID`), but the caller's own request can also ask for arbitrary
                // non-sysvar accounts -- those must not be treated as sysvars here, or they'd be
                // silently checked against `MUST_HAVE_SYSVAR_ACCOUNTS`'s pubkeys for nothing and,
                // worse, be excluded from `account_idx_map`'s ordinary account-event handling.
                let is_sysvar = account
                    .account
                    .as_ref()
                    .is_some_and(|a| a.owner == SYSVAR_PROGRAM_ID.to_bytes());
                if is_sysvar {
                    Some(GeyserEventInfo::SysvarAccount {
                        slot: account.slot,
                        bank_id,
                        pubkey: account
                            .account
                            .as_ref()
                            .and_then(|a| a.pubkey.as_slice().try_into().ok())
                            .unwrap_or([0; 32]),
                    })
                } else {
                    Some(GeyserEventInfo::BankData {
                        slot: account.slot,
                        bank_id,
                    })
                }
            }
            UpdateOneof::TransactionStatus(tx) => Some(GeyserEventInfo::BankData {
                slot: tx.slot,
                bank_id: tx.bank_id,
            }),
            // ev => Some(GeyserEventInfo::Other { slot: None }),
            UpdateOneof::Block(_) => {
                tracing::warn!("dropping block update");
                None
            }
            UpdateOneof::Ping(_) => None,
            UpdateOneof::Pong(_) => None,
        }
    }
}
