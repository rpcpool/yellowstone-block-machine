use {
    crate::{
        event::{
            BlockFooterEvInfo, BlockMetaEvInfo, EntryEvInfo, GeyserEventInfo, SlotStatusKind,
            SlotUpdateEvInfo,
        },
        forks::Forks,
        state_machine::{
            BlockStateMachineOutput, BlockSummary, BlocksStateMachine, DeadletterEvent, EntryInfo,
            MAX_UNRESOLVED_SLOT_AGE, SlotCommitmentStatusUpdate, SlotLifecycle,
            SlotLifecycleUpdate, UntrackedSlot,
        },
    },
    rustc_hash::FxHashMap,
    solana_clock::{BankId, Slot},
    solana_commitment_config::CommitmentLevel,
    solana_hash::Hash,
    solana_pubkey::Pubkey,
    std::time::Instant,
};

const STATE_MACHINE_GC_EVERY_COMPLETED_SLOTS: usize = 10;

// Sysvars a bank must be observed to have written before it may freeze. Clock and SlotHashes are
// rewritten inline as part of bank construction -- before `CreatedBank` itself fires;
// RecentBlockhashes at the slot's last tick and SlotHistory inside `freeze()`. All four are
// written before the bank freezes and agave sends BlockMeta only after freeze, so on a live
// stream they always precede BlockMeta. A bank already in flight when the subscription started
// can miss some of them: it never freezes (see AGENTS.md invariant 13).
//
// These four are deliberately the ones written on *every* slot. Other sysvars -- e.g.
// SysvarRent/SysvarEpochRewards, which are only rewritten on an epoch boundary -- must never be
// added here: no bank would ever freeze on the vast majority of slots that don't rewrite them.
pub(crate) const MUST_HAVE_SYSVAR_ACCOUNTS: [Pubkey; 4] = [
    Pubkey::from_str_const("SysvarC1ock11111111111111111111111111111111"),
    Pubkey::from_str_const("SysvarS1otHashes111111111111111111111111111"),
    Pubkey::from_str_const("SysvarS1otHistory11111111111111111111111111"),
    Pubkey::from_str_const("SysvarRecentB1ockHashes11111111111111111111"),
];
const MUST_HAVE_SYSVAR_ACCOUNTS_MASK: u8 = (1 << MUST_HAVE_SYSVAR_ACCOUNTS.len()) - 1;

///
/// Options for [`BlocksStateMachineWrapper`] and the streams built on it.
///
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlockMachineConfig {
    ///
    /// Whether a bank's end of block is its BlockMeta *and* its Alpenglow block footer, rather
    /// than its BlockMeta alone. When set, the `BlockSummary` that freezes a bank is only fed to
    /// the state machine once both have arrived, in either order (and `subscribe_block` also
    /// subscribes to block footers, metadata only). Turn it off for a cluster that doesn't
    /// produce footers (pre-Alpenglow): with it on, no bank would ever freeze there. Defaults to
    /// `true`.
    ///
    pub require_block_footer: bool,
}

impl Default for BlockMachineConfig {
    fn default() -> Self {
        Self {
            require_block_footer: true,
        }
    }
}

///
/// The core state machine that processes incoming Geyser events and produces block machine outputs.
///
/// Mainly a Wrapper to translate Grpc events to State Machine events. It is also the driver that
/// decides when a bank's block has ended: it feeds the state machine a bank's `BlockSummary` only
/// once every end-of-block marker [`BlockMachineConfig`] requires has arrived *and* the bank's
/// content is complete (AGENTS.md invariant 13). The state machine itself never sees footers.
///
///
/// What the driver has seen of a bank that has not frozen yet. `CreatedBank` and entries are
/// tracked by the state machine's own bank buffer, so only the parts it never sees live here.
///
#[derive(Debug)]
struct UnfrozenBank {
    ///
    /// When the first of this bank's sysvars, BlockMeta or footer arrived.
    ///
    first_seen: Instant,
    ///
    /// One bit per [`MUST_HAVE_SYSVAR_ACCOUNTS`] entry observed for this bank.
    ///
    sysvar_mask: u8,
    ///
    /// The bank's BlockMeta, held until every other freeze condition is met.
    ///
    block_meta: Option<BlockMetaEvInfo>,
    footer_seen: bool,
}

impl UnfrozenBank {
    fn new() -> Self {
        Self {
            first_seen: Instant::now(),
            sysvar_mask: 0,
            block_meta: None,
            footer_seen: false,
        }
    }
}

#[derive(Debug, Default)]
pub struct BlocksStateMachineWrapper {
    pub sm: BlocksStateMachine,
    completed_slots_since_last_gc: usize,
    bank_gc_tracer: Option<Vec<BankId>>,
    config: BlockMachineConfig,
    ///
    /// Banks that have not frozen yet. An entry lives until its bank freezes, or is evicted
    /// after [`MAX_UNRESOLVED_SLOT_AGE`] or once its bank is discarded.
    ///
    unfrozen_banks: FxHashMap<BankId, UnfrozenBank>,
}

impl From<EntryEvInfo> for EntryInfo {
    fn from(value: EntryEvInfo) -> Self {
        Self {
            entry_hash: Hash::new_from_array(value.hash),
            slot: value.slot,
            entry_index: value.index,
            starting_txn_index: value.starting_transaction_index,
            executed_txn_count: value.executed_transaction_count,
            bank_id: value.bank_id,
        }
    }
}

impl BlocksStateMachineWrapper {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn new_with_slot_gc_tracing() -> Self {
        Self {
            bank_gc_tracer: Some(Vec::with_capacity(10)),
            ..Self::default()
        }
    }

    ///
    /// Replaces this wrapper's configuration.
    ///
    /// # Arguments
    ///
    /// * `config` - The [`BlockMachineConfig`] to use from now on.
    ///
    /// # Returns
    ///
    /// The wrapper, for chaining after a constructor.
    ///
    pub const fn with_config(mut self, config: BlockMachineConfig) -> Self {
        self.config = config;
        self
    }

    ///
    /// Counts the banks whose BlockMeta arrived but whose block footer did not, so they can't
    /// freeze yet.
    ///
    /// # Returns
    ///
    /// The number of such banks. Always `0` when footers aren't required. A value that keeps
    /// growing means the stream isn't delivering footers: either the cluster isn't on Alpenglow,
    /// or the subscription lacks a `block_footer` filter.
    ///
    pub fn banks_awaiting_footer(&self) -> usize {
        if !self.config.require_block_footer {
            return 0;
        }
        self.unfrozen_banks
            .values()
            .filter(|bank| bank.block_meta.is_some() && !bank.footer_seen)
            .count()
    }

    ///
    /// Counts the banks whose end-of-block markers all arrived but whose content is still
    /// incomplete, so they can't freeze yet.
    ///
    /// # Returns
    ///
    /// The number of such banks. A few right after the subscription starts are expected: banks
    /// already in flight then never complete, and are evicted after [`MAX_UNRESOLVED_SLOT_AGE`].
    /// A value that keeps growing means the stream is missing content, e.g. the subscription
    /// lacks the sysvar accounts filter.
    ///
    pub fn banks_awaiting_content(&self) -> usize {
        self.unfrozen_banks
            .iter()
            .filter(|(bank_id, bank)| {
                bank.block_meta.is_some()
                    && (bank.footer_seen || !self.config.require_block_footer)
                    && !self.is_content_complete(**bank_id, bank)
            })
            .count()
    }

    ///
    /// Whether `bank`'s content is complete: its `CreatedBank`, every
    /// [`MUST_HAVE_SYSVAR_ACCOUNTS`] entry, and as many distinct entries as its BlockMeta reports.
    ///
    /// # Arguments
    ///
    /// * `bank_id` - The bank to check.
    /// * `bank` - What the driver has seen of it.
    ///
    /// # Returns
    ///
    /// `false` if its BlockMeta hasn't arrived yet, since the expected entry count comes from it.
    ///
    fn is_content_complete(&self, bank_id: BankId, bank: &UnfrozenBank) -> bool {
        let Some(block_meta) = bank.block_meta.as_ref() else {
            return false;
        };
        bank.sysvar_mask == MUST_HAVE_SYSVAR_ACCOUNTS_MASK
            && self
                .sm
                .has_created_bank_and_entries(bank_id, block_meta.entries_count)
    }

    ///
    /// Pops the next bank_id that has been garbage collected by the state machine, if GC
    /// tracing is enabled.
    ///
    #[inline]
    pub fn pop_bank_gc_trace(&mut self) -> Option<BankId> {
        let tracer = self.bank_gc_tracer.as_mut()?;
        tracer.pop()
    }

    ///
    /// Drops the tracking of banks that were discarded, or that waited longer than
    /// [`MAX_UNRESOLVED_SLOT_AGE`] to freeze, the same last-resort bound the state machine's `gc`
    /// pass 2 uses. Such a bank never froze, so its state machine state goes through that pass
    /// too.
    ///
    /// # Arguments
    ///
    /// * `now` - The current time.
    ///
    fn evict_stale_unfrozen_banks(&mut self, now: Instant) {
        let stale_bank_ids: Vec<BankId> = self
            .unfrozen_banks
            .iter()
            .filter(|(bank_id, bank)| {
                !self.sm.is_bank_trackable(**bank_id)
                    || now.saturating_duration_since(bank.first_seen) >= MAX_UNRESOLVED_SLOT_AGE
            })
            .map(|(bank_id, _)| *bank_id)
            .collect();
        for bank_id in stale_bank_ids {
            let bank = &self.unfrozen_banks[&bank_id];
            if self.sm.is_bank_trackable(bank_id) {
                if let Some(block_meta) = bank.block_meta.as_ref() {
                    if self.config.require_block_footer && !bank.footer_seen {
                        tracing::warn!(
                            "Bank {bank_id} (slot {}) never froze: its block footer never arrived within {MAX_UNRESOLVED_SLOT_AGE:?} -- is the cluster on Alpenglow, and is the stream subscribed to block footers?",
                            block_meta.slot
                        );
                    } else if !self.is_content_complete(bank_id, bank) {
                        tracing::debug!(
                            "Bank {bank_id} (slot {}) never froze: its content never completed within {MAX_UNRESOLVED_SLOT_AGE:?}, most likely because it was already in flight when the subscription started",
                            block_meta.slot
                        );
                    }
                }
            }
            self.unfrozen_banks.remove(&bank_id);
        }
    }

    fn maybe_run_gc_after_completed_slot(&mut self) {
        self.completed_slots_since_last_gc += 1;
        if self.completed_slots_since_last_gc >= STATE_MACHINE_GC_EVERY_COMPLETED_SLOTS {
            self.evict_stale_unfrozen_banks(Instant::now());
            self.sm.gc(self.bank_gc_tracer.as_mut());
            self.completed_slots_since_last_gc = 0;
        }
    }

    pub fn handle_block_entry(&mut self, entry: EntryEvInfo) -> Result<(), UntrackedSlot> {
        let bank_id = entry.bank_id;
        let entry_info: EntryInfo = entry.into();
        self.sm.process_replay_event(entry_info.into())?;
        self.freeze_on_content(bank_id);
        Ok(())
    }

    ///
    /// Records one of a bank's sysvar account writes, which counts toward its content being
    /// complete if it is one of [`MUST_HAVE_SYSVAR_ACCOUNTS`].
    ///
    /// # Arguments
    ///
    /// * `bank_id` - The bank that wrote the account.
    /// * `pubkey` - The sysvar's pubkey.
    ///
    /// # Errors
    ///
    /// [`UntrackedSlot`] if the bank was discarded.
    ///
    fn handle_sysvar_account(
        &mut self,
        bank_id: BankId,
        pubkey: &[u8; 32],
    ) -> Result<(), UntrackedSlot> {
        if !self.sm.is_bank_trackable(bank_id) {
            return Err(UntrackedSlot);
        }
        if self.sm.is_bank_frozen(bank_id) {
            return Ok(());
        }
        if let Some(pos) = MUST_HAVE_SYSVAR_ACCOUNTS
            .iter()
            .position(|sysvar| sysvar.to_bytes() == *pubkey)
        {
            self.unfrozen_banks
                .entry(bank_id)
                .or_insert_with(UnfrozenBank::new)
                .sysvar_mask |= 1 << pos;
            self.freeze_on_content(bank_id);
        }
        Ok(())
    }

    #[allow(clippy::collapsible_else_if)]
    pub fn handle_slot_update(
        &mut self,
        slot_update: SlotUpdateEvInfo,
    ) -> Result<(), UntrackedSlot> {
        const LIFE_CYCLE_STATUS: [SlotStatusKind; 4] = [
            SlotStatusKind::FirstShredReceived,
            SlotStatusKind::Completed,
            SlotStatusKind::CreatedBank,
            SlotStatusKind::Dead,
        ];

        if LIFE_CYCLE_STATUS.contains(&slot_update.status) {
            let lifecycle_update = SlotLifecycleUpdate {
                slot: slot_update.slot,
                parent_slot: slot_update.parent,
                stage: match slot_update.status {
                    SlotStatusKind::FirstShredReceived => SlotLifecycle::FirstShredReceived,
                    SlotStatusKind::Completed => SlotLifecycle::Completed,
                    SlotStatusKind::CreatedBank => SlotLifecycle::CreatedBank,
                    SlotStatusKind::Dead => SlotLifecycle::Dead,
                    _ => unreachable!(),
                },
                bank_id: slot_update.bank_id,
            };
            self.sm.process_replay_event(lifecycle_update.into())?;
            if let (SlotStatusKind::CreatedBank, Some(bank_id)) =
                (slot_update.status, slot_update.bank_id)
            {
                self.freeze_on_content(bank_id);
            }
        } else {
            if slot_update.dead_error {
                // Downgrade to lifecycle update
                let lifecycle_update = SlotLifecycleUpdate {
                    slot: slot_update.slot,
                    parent_slot: slot_update.parent,
                    stage: SlotLifecycle::Dead,
                    bank_id: None,
                };
                self.sm.process_replay_event(lifecycle_update.into())?;
            } else {
                let Some(bank_id) = slot_update.bank_id else {
                    tracing::warn!(
                        "commitment status for slot {} carries no bank_id; ignoring",
                        slot_update.slot
                    );
                    return Err(UntrackedSlot);
                };
                let commitment_level_update = SlotCommitmentStatusUpdate {
                    parent_slot: slot_update.parent,
                    slot: slot_update.slot,
                    bank_id,
                    commitment: match slot_update.status {
                        SlotStatusKind::Processed => CommitmentLevel::Processed,
                        SlotStatusKind::Confirmed => CommitmentLevel::Confirmed,
                        SlotStatusKind::Finalized => CommitmentLevel::Finalized,
                        _ => unreachable!(),
                    },
                };

                self.sm
                    .process_consensus_event(commitment_level_update.into());
            }
        }
        Ok(())
    }

    ///
    /// Handles a bank's BlockMeta. Freezes the bank if every other freeze condition is already
    /// met (its footer when footers are required, and its content being complete), otherwise
    /// holds the BlockMeta until they are.
    ///
    /// # Arguments
    ///
    /// * `block_meta` - The BlockMeta's wire-agnostic view.
    ///
    /// # Errors
    ///
    /// [`UntrackedSlot`] if the bank was discarded, if it already has a BlockMeta held, or if the
    /// state machine rejects the resulting `BlockSummary`.
    ///
    pub fn handle_block_meta(&mut self, block_meta: BlockMetaEvInfo) -> Result<(), UntrackedSlot> {
        let bank_id = block_meta.bank_id;
        // An already-frozen bank's duplicate goes straight to the state machine, which rejects it.
        if self.sm.is_bank_frozen(bank_id) {
            return self.freeze(block_meta);
        }
        if !self.sm.is_bank_trackable(bank_id) {
            return Err(UntrackedSlot);
        }
        let bank = self
            .unfrozen_banks
            .entry(bank_id)
            .or_insert_with(UnfrozenBank::new);
        if bank.block_meta.is_some() {
            tracing::error!(
                "UNEXPECTED: duplicate BlockMeta for bank {bank_id} (slot {}) that is still waiting to freeze. Dropping.",
                block_meta.slot
            );
            return Err(UntrackedSlot);
        }
        bank.block_meta = Some(block_meta);
        if let Some(result) = self.try_freeze(bank_id) {
            return result;
        }
        // The bank's block did end on the wire, even if it can't freeze yet. Counting it keeps
        // `gc` (and the eviction above) running even if banks never freeze at all.
        self.maybe_run_gc_after_completed_slot();
        Ok(())
    }

    ///
    /// Handles a bank's block footer. With footers required, freezes the bank if every other
    /// freeze condition is already met, otherwise records the footer until they are.
    ///
    /// # Arguments
    ///
    /// * `footer` - The footer's wire-agnostic view.
    ///
    /// # Errors
    ///
    /// [`UntrackedSlot`] if the bank was discarded, if it already has a footer recorded, or if
    /// the state machine rejects the resulting `BlockSummary`.
    ///
    pub fn handle_block_footer(&mut self, footer: &BlockFooterEvInfo) -> Result<(), UntrackedSlot> {
        let bank_id = footer.bank_id;
        if !self.sm.is_bank_trackable(bank_id) {
            // A held BlockMeta will never freeze a discarded bank.
            self.unfrozen_banks.remove(&bank_id);
            return Err(UntrackedSlot);
        }
        // Nothing to drive: the bank already froze (a duplicate footer, which the accumulator
        // reports), or footers aren't part of the end of block.
        if !self.config.require_block_footer || self.sm.is_bank_frozen(bank_id) {
            return Ok(());
        }
        let bank = self
            .unfrozen_banks
            .entry(bank_id)
            .or_insert_with(UnfrozenBank::new);
        if bank.footer_seen {
            tracing::error!(
                "UNEXPECTED: duplicate block footer for bank {bank_id} (slot {}) that is still waiting to freeze. Dropping.",
                footer.slot
            );
            return Err(UntrackedSlot);
        }
        bank.footer_seen = true;
        self.try_freeze(bank_id).unwrap_or(Ok(()))
    }

    ///
    /// Freezes `bank_id` if one of its content events was the last thing it was waiting for.
    /// A failed freeze is the state machine refusing the `BlockSummary`, which it already logs;
    /// the content event itself was still accepted.
    ///
    /// # Arguments
    ///
    /// * `bank_id` - The bank whose content just grew.
    ///
    fn freeze_on_content(&mut self, bank_id: BankId) {
        if let Some(Err(UntrackedSlot)) = self.try_freeze(bank_id) {
            tracing::debug!("Bank {bank_id} completed its content, but its freeze was refused");
        }
    }

    ///
    /// Freezes `bank_id` if it is ready: its BlockMeta arrived, its footer too when footers are
    /// required, and its content is complete (AGENTS.md invariant 13).
    ///
    /// # Arguments
    ///
    /// * `bank_id` - The bank to check.
    ///
    /// # Returns
    ///
    /// `None` if the bank isn't ready yet, otherwise the result of freezing it.
    ///
    fn try_freeze(&mut self, bank_id: BankId) -> Option<Result<(), UntrackedSlot>> {
        let bank = self.unfrozen_banks.get(&bank_id)?;
        bank.block_meta.as_ref()?;
        if self.config.require_block_footer && !bank.footer_seen {
            return None;
        }
        if !self.is_content_complete(bank_id, bank) {
            return None;
        }
        let block_meta = self
            .unfrozen_banks
            .remove(&bank_id)
            .and_then(|bank| bank.block_meta)
            .expect("just checked present");
        Some(self.freeze(block_meta))
    }

    ///
    /// Feeds the state machine the `BlockSummary` built from `block_meta`, freezing its bank.
    ///
    /// # Arguments
    ///
    /// * `block_meta` - The bank's BlockMeta.
    ///
    /// # Errors
    ///
    /// [`UntrackedSlot`] if the state machine rejects the summary.
    ///
    fn freeze(&mut self, block_meta: BlockMetaEvInfo) -> Result<(), UntrackedSlot> {
        let block_summary = BlockSummary {
            slot: block_meta.slot,
            entry_count: block_meta.entries_count,
            parent_slot: block_meta.parent_slot,
            executed_transaction_count: block_meta.executed_transaction_count,
            blockhash: Hash::new_from_array(block_meta.blockhash),
            parent_blockhash: Hash::new_from_array(block_meta.parent_blockhash),
            block_time: block_meta.block_time,
            bank_id: block_meta.bank_id,
        };
        self.sm.process_replay_event(block_summary.into())
        // Currently not used in block reconstruction
    }

    pub fn pop_next_state_machine_output(&mut self) -> Option<BlockStateMachineOutput> {
        let output = self.sm.pop_next_unprocess_blockstore_update()?;
        if matches!(output, BlockStateMachineOutput::FrozenBlock(_)) {
            self.maybe_run_gc_after_completed_slot();
        }
        Some(output)
    }

    pub const fn fork_graph(&self) -> &Forks<Slot> {
        &self.sm.forks
    }

    #[inline]
    pub fn pop_next_dlq(&mut self) -> Option<DeadletterEvent> {
        self.sm.pop_next_dlq()
    }

    pub fn handle_new_geyser_event(&mut self, event: GeyserEventInfo) -> Result<(), UntrackedSlot> {
        match event {
            GeyserEventInfo::Slot(slot_update) => self.handle_slot_update(slot_update),
            GeyserEventInfo::BlockMeta(block_meta) => self.handle_block_meta(block_meta),
            GeyserEventInfo::Entry(entry) => self.handle_block_entry(entry),
            GeyserEventInfo::BlockFooter(footer) => self.handle_block_footer(&footer),
            GeyserEventInfo::SysvarAccount {
                bank_id, pubkey, ..
            } => self.handle_sysvar_account(bank_id, &pubkey),
            GeyserEventInfo::BankData { bank_id, .. } => {
                if self.sm.is_bank_trackable(bank_id) {
                    Ok(())
                } else {
                    Err(UntrackedSlot)
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use {super::*, std::time::Duration};

    fn slot_update(slot: Slot, status: SlotStatusKind, bank_id: Option<BankId>) -> GeyserEventInfo {
        GeyserEventInfo::Slot(SlotUpdateEvInfo {
            slot,
            parent: Some(slot - 1),
            status,
            dead_error: status == SlotStatusKind::Dead,
            bank_id,
        })
    }

    fn entry(slot: Slot, bank_id: BankId) -> GeyserEventInfo {
        entry_at(slot, bank_id, 0)
    }

    fn entry_at(slot: Slot, bank_id: BankId, index: u64) -> GeyserEventInfo {
        GeyserEventInfo::Entry(EntryEvInfo {
            slot,
            index,
            starting_transaction_index: 0,
            executed_transaction_count: 0,
            hash: Hash::new_unique().to_bytes(),
            bank_id,
        })
    }

    fn block_meta(slot: Slot, bank_id: BankId) -> GeyserEventInfo {
        block_meta_with_entries(slot, bank_id, 1)
    }

    fn block_meta_with_entries(slot: Slot, bank_id: BankId, entries_count: u64) -> GeyserEventInfo {
        GeyserEventInfo::BlockMeta(BlockMetaEvInfo {
            slot,
            parent_slot: slot - 1,
            entries_count,
            executed_transaction_count: 0,
            blockhash: Hash::new_unique().to_bytes(),
            parent_blockhash: Hash::new_unique().to_bytes(),
            block_time: 0,
            bank_id,
        })
    }

    fn footer(slot: Slot, bank_id: BankId) -> GeyserEventInfo {
        GeyserEventInfo::BlockFooter(BlockFooterEvInfo {
            slot,
            bank_id,
            bank_hash: [1; 32],
            block_user_agent: String::new(),
        })
    }

    fn sysvar(slot: Slot, bank_id: BankId, pubkey: Pubkey) -> GeyserEventInfo {
        GeyserEventInfo::SysvarAccount {
            slot,
            bank_id,
            pubkey: pubkey.to_bytes(),
        }
    }

    ///
    /// Feeds a bank's whole content: `CreatedBank`, the must-have sysvars and its single entry.
    ///
    fn start_bank(w: &mut BlocksStateMachineWrapper, slot: Slot, bank_id: BankId) {
        w.handle_new_geyser_event(slot_update(
            slot,
            SlotStatusKind::CreatedBank,
            Some(bank_id),
        ))
        .unwrap();
        for pubkey in MUST_HAVE_SYSVAR_ACCOUNTS {
            w.handle_new_geyser_event(sysvar(slot, bank_id, pubkey))
                .unwrap();
        }
        w.handle_new_geyser_event(entry(slot, bank_id)).unwrap();
    }

    ///
    /// Drains the state machine's outputs as `None` for a `FrozenBlock` and `Some(level)` for a
    /// commitment update, ignoring the rest.
    ///
    fn drain(w: &mut BlocksStateMachineWrapper) -> Vec<Option<CommitmentLevel>> {
        let mut outputs = Vec::new();
        while let Some(output) = w.pop_next_state_machine_output() {
            match output {
                BlockStateMachineOutput::FrozenBlock(_) => outputs.push(None),
                BlockStateMachineOutput::SlotStatus(update) => {
                    outputs.push(Some(update.commitment))
                }
                _ => {}
            }
        }
        outputs
    }

    ///
    /// AGENTS.md invariant 11: with footers required, the driver only freezes a bank once both
    /// its BlockMeta and its footer arrived. Here BlockMeta comes first, as on leader slots.
    ///
    /// ```text
    /// slot 9 ── slot 10, bank 1000: BlockMeta (held) ... footer -> frozen
    /// ```
    ///
    #[test]
    fn block_meta_waits_for_its_footer() {
        let mut w = BlocksStateMachineWrapper::new();
        start_bank(&mut w, 10, 1000);

        w.handle_new_geyser_event(block_meta(10, 1000)).unwrap();
        assert_eq!(drain(&mut w), vec![], "no footer yet, no freeze");
        assert_eq!(w.banks_awaiting_footer(), 1);

        w.handle_new_geyser_event(footer(10, 1000)).unwrap();
        assert_eq!(drain(&mut w), vec![None]);
        assert_eq!(w.banks_awaiting_footer(), 0);
    }

    ///
    /// The footer arriving first (as on replayed slots) makes BlockMeta freeze the bank at once.
    ///
    /// ```text
    /// slot 9 ── slot 10, bank 1000: footer ... BlockMeta -> frozen
    /// ```
    ///
    #[test]
    fn footer_first_lets_block_meta_freeze_immediately() {
        let mut w = BlocksStateMachineWrapper::new();
        start_bank(&mut w, 10, 1000);

        w.handle_new_geyser_event(footer(10, 1000)).unwrap();
        assert_eq!(drain(&mut w), vec![]);

        w.handle_new_geyser_event(block_meta(10, 1000)).unwrap();
        assert_eq!(drain(&mut w), vec![None]);
    }

    ///
    /// With footers not required, BlockMeta alone ends the block, as before footers existed.
    ///
    /// ```text
    /// slot 9 ── slot 10, bank 1000: BlockMeta -> frozen
    /// ```
    ///
    #[test]
    fn block_meta_alone_freezes_when_footers_are_not_required() {
        let mut w = BlocksStateMachineWrapper::new().with_config(BlockMachineConfig {
            require_block_footer: false,
        });
        start_bank(&mut w, 10, 1000);

        w.handle_new_geyser_event(block_meta(10, 1000)).unwrap();
        assert_eq!(drain(&mut w), vec![None]);
        assert_eq!(w.banks_awaiting_footer(), 0);
    }

    ///
    /// AGENTS.md invariant 12: the backend never sends this order, but a raw agave source can
    /// (on leader slots the footer can arrive after Processed and even Confirmed). Those
    /// commitment updates are queued by the state machine until the bank freezes, so they still
    /// come out after the block, in order.
    ///
    /// ```text
    /// slot 9 ── slot 10, bank 1000: BlockMeta, Processed, Confirmed ... footer
    ///                               -> frozen, Processed, Confirmed
    /// ```
    ///
    #[test]
    fn commitment_updates_before_the_footer_come_out_after_the_freeze() {
        let mut w = BlocksStateMachineWrapper::new();
        start_bank(&mut w, 10, 1000);
        w.handle_new_geyser_event(block_meta(10, 1000)).unwrap();
        w.handle_new_geyser_event(slot_update(10, SlotStatusKind::Processed, Some(1000)))
            .unwrap();
        w.handle_new_geyser_event(slot_update(10, SlotStatusKind::Confirmed, Some(1000)))
            .unwrap();
        assert_eq!(drain(&mut w), vec![]);

        w.handle_new_geyser_event(footer(10, 1000)).unwrap();

        assert_eq!(
            drain(&mut w),
            vec![
                None,
                Some(CommitmentLevel::Processed),
                Some(CommitmentLevel::Confirmed)
            ]
        );
    }

    ///
    /// A second BlockMeta while the first still waits for the footer is UNEXPECTED: rejected,
    /// and the first one is kept.
    ///
    #[test]
    fn duplicate_block_meta_waiting_for_its_footer_is_rejected() {
        let mut w = BlocksStateMachineWrapper::new();
        start_bank(&mut w, 10, 1000);
        w.handle_new_geyser_event(block_meta(10, 1000)).unwrap();

        assert!(matches!(
            w.handle_new_geyser_event(block_meta(10, 1000)),
            Err(UntrackedSlot)
        ));
        assert_eq!(w.banks_awaiting_footer(), 1);
    }

    ///
    /// A BlockMeta whose footer never comes is dropped after `MAX_UNRESOLVED_SLOT_AGE`, so the
    /// held markers stay bounded on a stream that never delivers footers.
    ///
    #[test]
    fn block_meta_waiting_too_long_for_its_footer_is_evicted() {
        let mut w = BlocksStateMachineWrapper::new();
        start_bank(&mut w, 10, 1000);
        w.handle_new_geyser_event(block_meta(10, 1000)).unwrap();

        w.evict_stale_unfrozen_banks(Instant::now());
        assert_eq!(w.banks_awaiting_footer(), 1, "too young to evict");

        w.evict_stale_unfrozen_banks(
            Instant::now() + MAX_UNRESOLVED_SLOT_AGE + Duration::from_secs(1),
        );
        assert_eq!(w.banks_awaiting_footer(), 0);
    }

    ///
    /// AGENTS.md invariant 5: once a bank is discarded (here its slot went dead), its footer and
    /// BlockMeta are rejected, and a BlockMeta that was waiting for the footer is dropped.
    ///
    /// ```text
    /// slot 9 ── slot 10, bank 1000: BlockMeta (held), Dead ... footer -> rejected
    /// ```
    ///
    #[test]
    fn discarded_bank_drops_its_held_block_meta_and_rejects_its_footer() {
        let mut w = BlocksStateMachineWrapper::new();
        start_bank(&mut w, 10, 1000);
        w.handle_new_geyser_event(block_meta(10, 1000)).unwrap();
        w.handle_new_geyser_event(slot_update(10, SlotStatusKind::Dead, Some(1000)))
            .unwrap();

        assert!(matches!(
            w.handle_new_geyser_event(footer(10, 1000)),
            Err(UntrackedSlot)
        ));
        assert_eq!(w.banks_awaiting_footer(), 0);
        assert!(matches!(
            w.handle_new_geyser_event(block_meta(10, 1000)),
            Err(UntrackedSlot)
        ));
        assert_eq!(drain(&mut w), vec![]);
    }

    ///
    /// AGENTS.md invariant 13: a bank only freezes once its content is complete. Here its last
    /// must-have sysvar arrives after both its BlockMeta and its footer, and is what freezes it.
    ///
    /// ```text
    /// slot 9 ── slot 10, bank 1000: CreatedBank, 3 sysvars, entry, BlockMeta, footer (held)
    ///                               ... last sysvar -> frozen
    /// ```
    ///
    #[test]
    fn bank_waits_for_all_its_sysvars() {
        let mut w = BlocksStateMachineWrapper::new();
        w.handle_new_geyser_event(slot_update(10, SlotStatusKind::CreatedBank, Some(1000)))
            .unwrap();
        for pubkey in &MUST_HAVE_SYSVAR_ACCOUNTS[..3] {
            w.handle_new_geyser_event(sysvar(10, 1000, *pubkey))
                .unwrap();
        }
        w.handle_new_geyser_event(entry(10, 1000)).unwrap();
        w.handle_new_geyser_event(block_meta(10, 1000)).unwrap();
        w.handle_new_geyser_event(footer(10, 1000)).unwrap();
        assert_eq!(drain(&mut w), vec![], "a sysvar is missing, no freeze");
        assert_eq!(w.banks_awaiting_content(), 1);

        w.handle_new_geyser_event(sysvar(10, 1000, MUST_HAVE_SYSVAR_ACCOUNTS[3]))
            .unwrap();
        assert_eq!(drain(&mut w), vec![None]);
        assert_eq!(w.banks_awaiting_content(), 0);
    }

    ///
    /// AGENTS.md invariant 13: a bank only freezes once it has as many entries as its BlockMeta
    /// reports.
    ///
    /// ```text
    /// slot 9 ── slot 10, bank 1000: entry 0, BlockMeta (2 entries), footer (held)
    ///                               ... entry 1 -> frozen
    /// ```
    ///
    #[test]
    fn bank_waits_for_the_entry_count_its_block_meta_reports() {
        let mut w = BlocksStateMachineWrapper::new();
        start_bank(&mut w, 10, 1000);
        w.handle_new_geyser_event(block_meta_with_entries(10, 1000, 2))
            .unwrap();
        w.handle_new_geyser_event(footer(10, 1000)).unwrap();
        assert_eq!(drain(&mut w), vec![], "1 of 2 entries, no freeze");

        w.handle_new_geyser_event(entry_at(10, 1000, 1)).unwrap();
        assert_eq!(drain(&mut w), vec![None]);
    }

    ///
    /// A sysvar that isn't one of the must-have ones (here Rent, rewritten only at an epoch
    /// boundary) doesn't count toward a bank's content.
    ///
    /// ```text
    /// slot 9 ── slot 10, bank 1000: CreatedBank, 3 sysvars + Rent, entry, BlockMeta, footer
    ///                               -> not frozen
    /// ```
    ///
    #[test]
    fn other_sysvars_do_not_count_toward_content() {
        let mut w = BlocksStateMachineWrapper::new();
        w.handle_new_geyser_event(slot_update(10, SlotStatusKind::CreatedBank, Some(1000)))
            .unwrap();
        for pubkey in &MUST_HAVE_SYSVAR_ACCOUNTS[..3] {
            w.handle_new_geyser_event(sysvar(10, 1000, *pubkey))
                .unwrap();
        }
        w.handle_new_geyser_event(sysvar(
            10,
            1000,
            Pubkey::from_str_const("SysvarRent111111111111111111111111111111111"),
        ))
        .unwrap();
        w.handle_new_geyser_event(entry(10, 1000)).unwrap();
        w.handle_new_geyser_event(block_meta(10, 1000)).unwrap();
        w.handle_new_geyser_event(footer(10, 1000)).unwrap();

        assert_eq!(drain(&mut w), vec![]);
        assert_eq!(w.banks_awaiting_content(), 1);
    }

    ///
    /// AGENTS.md invariant 13: a bank already in flight when the subscription started missed its
    /// `CreatedBank`, so it never freezes, and its commitment updates stay queued in the state
    /// machine instead of coming out. The driver stops tracking it after
    /// `MAX_UNRESOLVED_SLOT_AGE`.
    ///
    /// ```text
    /// (subscription starts)
    /// slot 9 ── slot 10, bank 1000: sysvars, entry, BlockMeta, footer, Confirmed -> nothing
    /// ```
    ///
    #[test]
    fn bank_in_flight_at_subscription_start_never_freezes() {
        let mut w = BlocksStateMachineWrapper::new();
        for pubkey in MUST_HAVE_SYSVAR_ACCOUNTS {
            w.handle_new_geyser_event(sysvar(10, 1000, pubkey)).unwrap();
        }
        w.handle_new_geyser_event(entry(10, 1000)).unwrap();
        w.handle_new_geyser_event(block_meta(10, 1000)).unwrap();
        w.handle_new_geyser_event(footer(10, 1000)).unwrap();
        w.handle_new_geyser_event(slot_update(10, SlotStatusKind::Confirmed, Some(1000)))
            .unwrap();

        assert_eq!(drain(&mut w), vec![]);
        assert_eq!(w.banks_awaiting_content(), 1);

        w.evict_stale_unfrozen_banks(
            Instant::now() + MAX_UNRESOLVED_SLOT_AGE + Duration::from_secs(1),
        );
        assert_eq!(w.banks_awaiting_content(), 0);
    }
}
