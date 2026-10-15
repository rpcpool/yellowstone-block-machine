use {
    crate::{
        dragonsmouth::block_accumulator::SYSVAR_PROGRAM_ID,
        event::{
            BlockFooterEvInfo, BlockMetaEvInfo, EntryEvInfo, GeyserEventAdapter, GeyserEventInfo,
            SlotStatusKind, SlotUpdateEvInfo,
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
                        .inspect_err(|error| {
                            tracing::error!(
                                "UNEXPECTED: invalid blockhash for slot {}: {error}. Dropping.",
                                block_meta.slot
                            )
                        })
                        .ok()?
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
                hash: entry
                    .hash
                    .as_slice()
                    .try_into()
                    .inspect_err(|error| {
                        tracing::error!(
                            "UNEXPECTED: invalid entry hash for slot {}: {error}. Dropping.",
                            entry.slot
                        )
                    })
                    .ok()?,
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
            UpdateOneof::BlockFooter(footer) => {
                let Ok(bank_hash) = footer.bank_hash.as_slice().try_into() else {
                    tracing::error!(
                        "UNEXPECTED: block footer for bank {} (slot {}) has a {}-byte bank_hash. Dropping.",
                        footer.bank_id,
                        footer.slot,
                        footer.bank_hash.len()
                    );
                    return None;
                };
                Some(GeyserEventInfo::BlockFooter(BlockFooterEvInfo {
                    slot: footer.slot,
                    bank_id: footer.bank_id,
                    bank_hash,
                    block_user_agent: String::from_utf8_lossy(&footer.block_user_agent)
                        .into_owned(),
                }))
            }
            UpdateOneof::EntryUpdateParent(_) => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use yellowstone_grpc_proto::geyser::{SubscribeUpdateBlockMeta, SubscribeUpdateEntry};

    /// Issue #19: reject malformed hashes while retaining valid wire values.
    #[test]
    fn malformed_blockhashes_are_dropped() {
        for blockhash in ["", "invalid", "111"] {
            let event = SubscribeUpdate {
                update_oneof: Some(UpdateOneof::BlockMeta(SubscribeUpdateBlockMeta {
                    blockhash: blockhash.to_string(),
                    ..Default::default()
                })),
                ..Default::default()
            };
            assert!(SubscribeUpdate::extract_geyser_ev_info(&event).is_none());
        }
    }

    /// Issue #19: reject entries whose hash is not exactly 32 bytes.
    #[test]
    fn malformed_entry_hashes_are_dropped() {
        for len in [0, 31, 33] {
            let event = SubscribeUpdate {
                update_oneof: Some(UpdateOneof::Entry(SubscribeUpdateEntry {
                    hash: vec![1; len],
                    ..Default::default()
                })),
                ..Default::default()
            };
            assert!(SubscribeUpdate::extract_geyser_ev_info(&event).is_none());
        }
    }

    /// Issue #19: an empty genesis parent hash remains accepted.
    #[test]
    fn valid_hashes_are_preserved() {
        let hash = solana_hash::Hash::new_unique();
        let event = SubscribeUpdate {
            update_oneof: Some(UpdateOneof::BlockMeta(SubscribeUpdateBlockMeta {
                blockhash: hash.to_string(),
                parent_blockhash: String::new(),
                ..Default::default()
            })),
            ..Default::default()
        };
        let Some(GeyserEventInfo::BlockMeta(meta)) =
            SubscribeUpdate::extract_geyser_ev_info(&event)
        else {
            panic!("valid block metadata must be accepted");
        };
        assert_eq!(meta.blockhash, hash.to_bytes());
        assert_eq!(meta.parent_blockhash, [0; 32]);
        let event = SubscribeUpdate {
            update_oneof: Some(UpdateOneof::Entry(SubscribeUpdateEntry {
                hash: hash.to_bytes().to_vec(),
                ..Default::default()
            })),
            ..Default::default()
        };
        let Some(GeyserEventInfo::Entry(entry)) = SubscribeUpdate::extract_geyser_ev_info(&event)
        else {
            panic!("valid entry must be accepted");
        };
        assert_eq!(entry.hash, hash.to_bytes());
    }
}
