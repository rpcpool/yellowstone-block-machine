use {
    crate::{
        dragonsmouth::RESERVED_FILTER_NAME,
        event::{GeyserEventInfo, SlotStatusKind},
        state_machine::FrozenBlock,
        stream::{Block, BlockAccumulator, BlockEventStore},
    },
    rustc_hash::FxHashMap,
    solana_clock::{BankId, Slot},
    solana_hash::HASH_BYTES,
    solana_pubkey::Pubkey,
    std::collections::VecDeque,
    yellowstone_grpc_proto::geyser::{SubscribeUpdate, subscribe_update::UpdateOneof},
};

// Sysvars a bank must be observed to have written before its block is considered complete.
// Clock and SlotHashes are rewritten inline as part of bank construction -- before `CreatedBank`
// itself fires; SlotHistory and RecentBlockhashes are written later, as the block freezes, which
// is exactly why BlockMeta arriving isn't by itself sufficient evidence that the bank's content
// is all in: BlockMeta and these late Account writes are independent messages that can reorder.
//
// These four are deliberately the ones written on *every* slot. Other sysvars -- e.g.
// SysvarRent/SysvarEpochRewards, which are only rewritten on an epoch boundary -- can arrive
// before `CreatedBank` too (same reordering as Clock/SlotHashes above), but they must never be
// added here: `is_complete` would then never be satisfiable on the vast majority of slots that
// don't rewrite them at all. Auto-vivify (see `add_event`) already handles their arrival at any
// bank_id/order regardless; they just don't count toward this mask.
pub(crate) const MUST_HAVE_SYSVAR_ACCOUNTS: [Pubkey; 4] = [
    Pubkey::from_str_const("SysvarC1ock11111111111111111111111111111111"),
    Pubkey::from_str_const("SysvarS1otHashes111111111111111111111111111"),
    Pubkey::from_str_const("SysvarS1otHistory11111111111111111111111111"),
    Pubkey::from_str_const("SysvarRecentB1ockHashes11111111111111111111"),
];
const MUST_HAVE_SYSVAR_ACCOUNTS_MASK: u8 = (1 << MUST_HAVE_SYSVAR_ACCOUNTS.len()) - 1;

///
/// Owner of every sysvar account, including all of [`MUST_HAVE_SYSVAR_ACCOUNTS`] -- subscribing
/// by owner instead of by individual pubkey (see `subscribe_block`) is simpler and catches any
/// sysvar, not just the specific ones this crate currently requires.
///
pub(crate) const SYSVAR_PROGRAM_ID: Pubkey =
    Pubkey::from_str_const("Sysvar1111111111111111111111111111111111111");

///
/// The BlockMeta-reported blockhash and expected entry count, recorded once BlockMeta arrives
/// but before the block is necessarily ready to seal -- see [`BlockBuffer::is_complete`].
///
#[derive(Debug)]
struct PendingFreeze {
    blockhash: [u8; HASH_BYTES],
    entries_count: u64,
    executed_transaction_count: u64,
    parent_slot: Slot,
    parent_blockhash: [u8; HASH_BYTES],
    block_time: u64,
}

#[derive(Debug)]
pub struct BankBuffer {
    slot: Slot,
    #[allow(dead_code)]
    bank_id: BankId,
    blockhash: [u8; HASH_BYTES],
    events: Vec<SubscribeUpdate>,
    account_idx_map: Vec<usize>,
    transaction_idx_map: Vec<usize>,
    transaction_status_map: Vec<usize>,
    entry_idx_map: Vec<usize>,
    created_bank_seen: bool,
    sysvar_bitmask: u8,
    // Every Entry observed for this bank, regardless of whether the client's own subscription
    // wants entries delivered -- independent of `entry_idx_map`, which only tracks
    // client-visible ones. Needed so the entry-count check below isn't skewed by filtering.
    entries_seen: u64,
    pending_freeze: Option<PendingFreeze>,
    entry_count: u64,
    executed_transaction_count: u64,
    parent_slot: Slot,
    parent_blockhash: [u8; HASH_BYTES],
    blocktime_unix_ts: u64,
}

impl BlockEventStore for BankBuffer {
    type EventT = SubscribeUpdate;

    type Iter<'a> = std::slice::Iter<'a, Self::EventT>;

    type IntoIter = std::vec::IntoIter<Self::EventT>;

    fn len(&self) -> usize {
        self.account_idx_map.len()
            + self.transaction_idx_map.len()
            + self.transaction_status_map.len()
            + self.entry_idx_map.len()
    }

    fn blockhash(&self) -> [u8; HASH_BYTES] {
        self.blockhash
    }

    fn iter(&self) -> Self::Iter<'_> {
        self.events.iter()
    }

    fn into_iter(self) -> Self::IntoIter {
        self.events.into_iter()
    }
}

impl BankBuffer {
    const fn new(bank_id: BankId, slot: Slot) -> Self {
        Self {
            slot,
            bank_id,
            blockhash: [0; HASH_BYTES],
            events: Vec::new(),
            account_idx_map: Vec::new(),
            transaction_idx_map: Vec::new(),
            entry_idx_map: Vec::new(),
            transaction_status_map: Vec::new(),
            created_bank_seen: false,
            sysvar_bitmask: 0,
            entries_seen: 0,
            pending_freeze: None,
            entry_count: 0,
            executed_transaction_count: 0,
            parent_slot: 0,
            parent_blockhash: [0; HASH_BYTES],
            blocktime_unix_ts: 0,
        }
    }

    ///
    /// A block is only considered finished once: `CreatedBank` was observed, every must-have
    /// sysvar account was observed, BlockMeta arrived, and at least as many entries were
    /// observed as BlockMeta itself reports.
    ///
    const fn is_complete(&self) -> bool {
        let Some(pending) = self.pending_freeze.as_ref() else {
            return false;
        };
        self.created_bank_seen
            && self.sysvar_bitmask == MUST_HAVE_SYSVAR_ACCOUNTS_MASK
            && self.entries_seen >= pending.entries_count
    }
}

///
/// An in-memory store for blocks being reconstructed, keyed by `bank_id` — each bank instance
/// gets its own independent buffer, so competing banks for the same slot never clobber each
/// other's accumulated content.
///
/// A block only moves from `active_block_map` to `frozen_block_map` (becoming eligible for
/// [`BlockAccumulator::finish_block`]) once [`BlockBuffer::is_complete`] holds -- BlockMeta
/// arriving is necessary but not sufficient, since some of what it implies (the must-have
/// sysvars in particular) can genuinely still be in flight when it shows up.
#[derive(Default)]
pub struct DragonsmouthBlockCumulator {
    active_bank_map: FxHashMap<BankId, BankBuffer>,
    frozen_bank_map: FxHashMap<BankId, BankBuffer>,
    newly_sealed: VecDeque<BankId>,
}

impl DragonsmouthBlockCumulator {
    ///
    /// Promotes `bank_id` from `active_block_map` to `frozen_block_map` if it just became
    /// complete. Safe to call after any event that could plausibly have completed it.
    ///
    fn try_seal(&mut self, bank_id: BankId) {
        let Some(block) = self.active_bank_map.get(&bank_id) else {
            return;
        };
        if !block.is_complete() {
            return;
        }
        let mut block = self
            .active_bank_map
            .remove(&bank_id)
            .expect("just checked present");
        let pending = block
            .pending_freeze
            .take()
            .expect("is_complete implies pending_freeze is Some");
        block.blockhash = pending.blockhash;
        block.entry_count = pending.entries_count;
        block.executed_transaction_count = pending.executed_transaction_count;
        block.parent_slot = pending.parent_slot;
        block.parent_blockhash = pending.parent_blockhash;
        block.blocktime_unix_ts = pending.block_time;
        self.frozen_bank_map.insert(bank_id, block);
        self.newly_sealed.push_back(bank_id);
    }
}

impl BlockAccumulator for DragonsmouthBlockCumulator {
    type EventT = SubscribeUpdate;
    type EventStore = BankBuffer;

    fn add_event(
        &mut self,
        mut event: SubscribeUpdate,
        bank_id: BankId,
        ev_info: &GeyserEventInfo,
    ) {
        let slot = ev_info.slot();
        let block = self
            .active_bank_map
            .entry(bank_id)
            .or_insert_with(|| BankBuffer::new(bank_id, slot));

        // Tracked unconditionally, regardless of whether the event below ends up being visible
        // to the client -- completeness must not depend on what the client's own subscription
        // happened to ask for.
        match ev_info {
            GeyserEventInfo::Slot(update) if update.status == SlotStatusKind::CreatedBank => {
                block.created_bank_seen = true;
            }
            GeyserEventInfo::SysvarAccount { pubkey, .. } => {
                if let Some(pos) = MUST_HAVE_SYSVAR_ACCOUNTS
                    .iter()
                    .position(|sysvar| sysvar.to_bytes() == *pubkey)
                {
                    block.sysvar_bitmask |= 1 << pos;
                }
            }
            GeyserEventInfo::Entry(_) => {
                block.entries_seen += 1;
            }
            _ => {}
        }

        // We only match on those four since this is where we add the reserved filter to the
        // event -- Slot/BlockMeta/Entry are always force-subscribed (see `subscribe_block`) so
        // this crate can track lifecycle/completeness even if the client never asked for them;
        // Account is force-subscribed too, but only for the must-have sysvars above.
        match ev_info {
            GeyserEventInfo::Slot(_)
            | GeyserEventInfo::BlockMeta(_)
            | GeyserEventInfo::Entry(_)
            | GeyserEventInfo::SysvarAccount { .. } => {
                event.filters.retain(|f| f != RESERVED_FILTER_NAME);

                if event.filters.is_empty() {
                    // Only present because of the crate's internal reserved filter -- the
                    // client didn't ask for it, so don't surface it in the delivered block.
                    self.try_seal(bank_id);
                    return;
                }
            }
            _ => {}
        }

        let idx = block.events.len();
        match ev_info {
            GeyserEventInfo::SysvarAccount { .. } => block.account_idx_map.push(idx),
            GeyserEventInfo::BankData { .. } => {
                let Some(ev) = event.update_oneof.as_ref() else {
                    return;
                };
                match ev {
                    UpdateOneof::Account(_) => {
                        block.account_idx_map.push(idx);
                    }
                    UpdateOneof::Transaction(_) => {
                        block.transaction_idx_map.push(idx);
                    }
                    UpdateOneof::TransactionStatus(_) => {
                        block.transaction_status_map.push(idx);
                    }
                    _ => {
                        let discriminant = std::mem::discriminant(ev);
                        tracing::warn!("dropping event {:?}", discriminant);
                    }
                }
            }
            GeyserEventInfo::Entry(_) => block.entry_idx_map.push(idx),
            _ => {
                //block meta and slot are ignored
                self.try_seal(bank_id);
                return;
            }
        }
        block.events.push(event);
        self.try_seal(bank_id);
    }

    fn freeze_block(&mut self, frozen_block_info: FrozenBlock) {
        let Some(block) = self.active_bank_map.get_mut(&frozen_block_info.bank_id) else {
            // Should always be present -- `add_event` already ran for this same BlockMeta event
            // (and auto-vivified the buffer if needed) before this is ever called. Most likely
            // reachable only for a duplicate/late BlockMeta targeting an already-sealed bank.
            tracing::debug!(
                "BlockMeta for bank {} (slot {}) has no active buffer -- already sealed?",
                frozen_block_info.bank_id,
                frozen_block_info.slot
            );
            return;
        };
        block.pending_freeze = Some(PendingFreeze {
            blockhash: frozen_block_info.blockhash.to_bytes(),
            entries_count: frozen_block_info.entries_count,
            executed_transaction_count: frozen_block_info.executed_transaction_count,
            parent_slot: frozen_block_info.parent_slot,
            parent_blockhash: frozen_block_info.parent_blockhash.to_bytes(),
            block_time: frozen_block_info.block_time,
        });
        self.try_seal(frozen_block_info.bank_id);
    }

    fn finish_block(&mut self, bank_id: BankId) -> Option<Block<Self::EventStore>> {
        let bank = self.frozen_bank_map.remove(&bank_id)?;
        let block = Block {
            slot: bank.slot,
            bank_id,
            blockhash: bank.blockhash,
            entry_count: bank.entry_count,
            executed_transaction_count: bank.executed_transaction_count,
            parent_slot: bank.parent_slot,
            parent_blockhash: bank.parent_blockhash,
            blocktime_unix_ts: bank.blocktime_unix_ts,
            events: bank,
        };
        Some(block)
    }

    fn prune_block(&mut self, bank_id: BankId) {
        self.active_bank_map.remove(&bank_id);
        self.frozen_bank_map.remove(&bank_id);
    }

    fn pop_newly_sealed(&mut self) -> Option<BankId> {
        self.newly_sealed.pop_front()
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::event::GeyserEventAdapter,
        solana_hash::Hash,
        yellowstone_grpc_proto::geyser::{
            SlotStatus, SubscribeUpdateAccount, SubscribeUpdateAccountInfo, SubscribeUpdateEntry,
            SubscribeUpdateSlot, subscribe_update::UpdateOneof,
        },
    };

    fn update(oneof: UpdateOneof, filters: Vec<String>) -> SubscribeUpdate {
        SubscribeUpdate {
            filters,
            created_at: None,
            update_oneof: Some(oneof),
        }
    }

    fn created_bank_update(slot: u64, bank_id: u64) -> SubscribeUpdate {
        update(
            UpdateOneof::Slot(SubscribeUpdateSlot {
                slot,
                parent: None,
                status: SlotStatus::SlotCreatedBank as i32,
                dead_error: None,
                bank_id: Some(bank_id),
            }),
            vec![RESERVED_FILTER_NAME.to_string()],
        )
    }

    fn sysvar_account_update(
        slot: u64,
        bank_id: u64,
        pubkey: Pubkey,
        filters: Vec<String>,
    ) -> SubscribeUpdate {
        update(
            UpdateOneof::Account(SubscribeUpdateAccount {
                account: Some(SubscribeUpdateAccountInfo {
                    pubkey: pubkey.to_bytes().to_vec(),
                    lamports: 1,
                    owner: SYSVAR_PROGRAM_ID.to_bytes().to_vec(),
                    executable: false,
                    rent_epoch: 0,
                    data: vec![],
                    write_version: 1,
                    txn_signature: None,
                }),
                slot,
                is_startup: false,
                bank_id: Some(bank_id),
            }),
            filters,
        )
    }

    fn entry_update(slot: u64, index: u64, bank_id: u64) -> SubscribeUpdate {
        update(
            UpdateOneof::Entry(SubscribeUpdateEntry {
                slot,
                index,
                num_hashes: 0,
                hash: vec![0; 32],
                executed_transaction_count: 0,
                starting_transaction_index: 0,
                bank_id,
            }),
            vec![RESERVED_FILTER_NAME.to_string()],
        )
    }

    fn feed(acc: &mut DragonsmouthBlockCumulator, ev: SubscribeUpdate) {
        let ev_info = SubscribeUpdate::extract_geyser_ev_info(&ev).unwrap();
        let bank_id = ev_info.bank_id().unwrap();
        acc.add_event(ev, bank_id, &ev_info);
    }

    fn freeze(acc: &mut DragonsmouthBlockCumulator, slot: u64, bank_id: u64, entries_count: u64) {
        acc.freeze_block(FrozenBlock {
            slot,
            bank_id,
            parent_slot: 0,
            entries: vec![],
            blockhash: Hash::default(),
            entries_count,
            executed_transaction_count: 0,
            parent_blockhash: Hash::default(),
            block_time: 0,
        });
    }

    #[test]
    fn block_does_not_seal_until_all_sysvars_observed() {
        let mut acc = DragonsmouthBlockCumulator::default();
        let (slot, bank_id) = (10, 1000);

        feed(&mut acc, created_bank_update(slot, bank_id));
        for &sysvar in &MUST_HAVE_SYSVAR_ACCOUNTS[..3] {
            feed(
                &mut acc,
                sysvar_account_update(
                    slot,
                    bank_id,
                    sysvar,
                    vec![RESERVED_FILTER_NAME.to_string()],
                ),
            );
        }
        feed(&mut acc, entry_update(slot, 0, bank_id));
        freeze(&mut acc, slot, bank_id, 1);

        assert!(
            acc.finish_block(bank_id).is_none(),
            "must not seal: one must-have sysvar is still missing"
        );
        assert!(acc.pop_newly_sealed().is_none());

        // The last sysvar arrives after BlockMeta already did -- this is exactly the race the
        // gate exists for.
        feed(
            &mut acc,
            sysvar_account_update(
                slot,
                bank_id,
                MUST_HAVE_SYSVAR_ACCOUNTS[3],
                vec![RESERVED_FILTER_NAME.to_string()],
            ),
        );

        assert_eq!(acc.pop_newly_sealed(), Some(bank_id));
        assert!(acc.finish_block(bank_id).is_some());
    }

    #[test]
    fn block_does_not_seal_until_entry_count_matches_blockmeta() {
        let mut acc = DragonsmouthBlockCumulator::default();
        let (slot, bank_id) = (20, 2000);

        feed(&mut acc, created_bank_update(slot, bank_id));
        for sysvar in MUST_HAVE_SYSVAR_ACCOUNTS {
            feed(
                &mut acc,
                sysvar_account_update(
                    slot,
                    bank_id,
                    sysvar,
                    vec![RESERVED_FILTER_NAME.to_string()],
                ),
            );
        }
        feed(&mut acc, entry_update(slot, 0, bank_id));
        freeze(&mut acc, slot, bank_id, 2); // BlockMeta says 2 entries are expected

        assert!(
            acc.finish_block(bank_id).is_none(),
            "must not seal: only 1 of the 2 entries BlockMeta reports has been observed"
        );

        feed(&mut acc, entry_update(slot, 1, bank_id));

        assert_eq!(acc.pop_newly_sealed(), Some(bank_id));
        assert!(acc.finish_block(bank_id).is_some());
    }

    #[test]
    fn sysvar_accounts_count_toward_completeness_but_are_only_delivered_if_the_client_wanted_them()
    {
        let mut acc = DragonsmouthBlockCumulator::default();
        let (slot, bank_id) = (30, 3000);

        feed(&mut acc, created_bank_update(slot, bank_id));
        // Three sysvars are only present because of the crate's own reserved filter -- the
        // client never asked for accounts. The fourth also carries the client's own filter tag.
        for &sysvar in &MUST_HAVE_SYSVAR_ACCOUNTS[..3] {
            feed(
                &mut acc,
                sysvar_account_update(
                    slot,
                    bank_id,
                    sysvar,
                    vec![RESERVED_FILTER_NAME.to_string()],
                ),
            );
        }
        feed(
            &mut acc,
            sysvar_account_update(
                slot,
                bank_id,
                MUST_HAVE_SYSVAR_ACCOUNTS[3],
                vec![
                    RESERVED_FILTER_NAME.to_string(),
                    "client-account-filter".to_string(),
                ],
            ),
        );
        feed(&mut acc, entry_update(slot, 0, bank_id));
        freeze(&mut acc, slot, bank_id, 1);

        let block = acc
            .finish_block(bank_id)
            .expect("created_bank + all sysvars + matching entry count are all satisfied");
        assert_eq!(
            block.events.account_idx_map.len(),
            1,
            "only the sysvar the client actually subscribed to should be delivered"
        );
    }

    #[test]
    fn non_sysvar_account_does_not_count_toward_the_must_have_mask() {
        let mut acc = DragonsmouthBlockCumulator::default();
        let (slot, bank_id) = (40, 4000);

        feed(&mut acc, created_bank_update(slot, bank_id));
        for sysvar in MUST_HAVE_SYSVAR_ACCOUNTS {
            feed(
                &mut acc,
                sysvar_account_update(
                    slot,
                    bank_id,
                    sysvar,
                    vec![RESERVED_FILTER_NAME.to_string()],
                ),
            );
        }
        feed(&mut acc, entry_update(slot, 0, bank_id));

        // A non-sysvar account the client subscribed to on its own -- must never be treated as
        // one of MUST_HAVE_SYSVAR_ACCOUNTS just because it happens to arrive as an Account
        // update, and must still be delivered like any other client-visible event.
        feed(
            &mut acc,
            update(
                UpdateOneof::Account(SubscribeUpdateAccount {
                    account: Some(SubscribeUpdateAccountInfo {
                        pubkey: Pubkey::new_unique().to_bytes().to_vec(),
                        lamports: 1,
                        owner: Pubkey::new_unique().to_bytes().to_vec(),
                        executable: false,
                        rent_epoch: 0,
                        data: vec![],
                        write_version: 1,
                        txn_signature: None,
                    }),
                    slot,
                    is_startup: false,
                    bank_id: Some(bank_id),
                }),
                vec!["client-account-filter".to_string()],
            ),
        );

        freeze(&mut acc, slot, bank_id, 1);

        let block = acc
            .finish_block(bank_id)
            .expect("must-have sysvars are unaffected by the extra non-sysvar account");
        assert_eq!(
            block.events.account_idx_map.len(),
            1,
            "the non-sysvar account should still be delivered like any other client-visible event"
        );
    }
}
