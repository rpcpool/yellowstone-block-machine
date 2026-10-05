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
    std::time::Instant,
};

const STATE_MACHINE_GC_EVERY_COMPLETED_SLOTS: usize = 10;

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
/// once every end-of-block marker [`BlockMachineConfig`] requires has arrived. The state machine
/// itself never sees footers.
///
#[derive(Debug, Default)]
pub struct BlocksStateMachineWrapper {
    pub sm: BlocksStateMachine,
    completed_slots_since_last_gc: usize,
    bank_gc_tracer: Option<Vec<BankId>>,
    config: BlockMachineConfig,
    ///
    /// BlockMetas waiting for their bank's block footer, with when they arrived. Only used when
    /// footers are required. An entry lives until the footer arrives, or is evicted after
    /// [`MAX_UNRESOLVED_SLOT_AGE`].
    ///
    block_meta_awaiting_footer: FxHashMap<BankId, (BlockMetaEvInfo, Instant)>,
    ///
    /// Banks whose block footer arrived before their BlockMeta, with when it arrived. Only used
    /// when footers are required. Same lifetime as `block_meta_awaiting_footer`.
    ///
    footer_awaiting_block_meta: FxHashMap<BankId, Instant>,
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
        self.block_meta_awaiting_footer.len()
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
    /// Drops end-of-block markers that waited longer than [`MAX_UNRESOLVED_SLOT_AGE`] for their
    /// counterpart, the same last-resort bound the state machine's `gc` pass 2 uses. The bank
    /// never froze, so its state machine state goes through that pass too.
    ///
    /// # Arguments
    ///
    /// * `now` - The current time.
    ///
    fn evict_stale_end_of_block_markers(&mut self, now: Instant) {
        let is_stale =
            |since: &Instant| now.saturating_duration_since(*since) >= MAX_UNRESOLVED_SLOT_AGE;
        self.block_meta_awaiting_footer
            .retain(|bank_id, (block_meta, since)| {
                if !is_stale(since) {
                    return true;
                }
                tracing::warn!(
                    "Bank {bank_id} (slot {}) never froze: its block footer never arrived within {MAX_UNRESOLVED_SLOT_AGE:?} of its BlockMeta -- is the cluster on Alpenglow, and is the stream subscribed to block footers?",
                    block_meta.slot
                );
                false
            });
        self.footer_awaiting_block_meta.retain(|bank_id, since| {
            if !is_stale(since) {
                return true;
            }
            tracing::debug!(
                "Dropping the block footer of bank {bank_id}: its BlockMeta never arrived within {MAX_UNRESOLVED_SLOT_AGE:?}"
            );
            false
        });
    }

    fn maybe_run_gc_after_completed_slot(&mut self) {
        self.completed_slots_since_last_gc += 1;
        if self.completed_slots_since_last_gc >= STATE_MACHINE_GC_EVERY_COMPLETED_SLOTS {
            self.evict_stale_end_of_block_markers(Instant::now());
            self.sm.gc(self.bank_gc_tracer.as_mut());
            self.completed_slots_since_last_gc = 0;
        }
    }

    pub fn handle_block_entry(&mut self, entry: EntryEvInfo) -> Result<(), UntrackedSlot> {
        let entry_info: EntryInfo = entry.into();
        self.sm.process_replay_event(entry_info.into())
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
    /// Handles a bank's BlockMeta. Without footers required it freezes the bank right away;
    /// with them, only if the bank's footer already arrived, otherwise it is held until it does.
    ///
    /// # Arguments
    ///
    /// * `block_meta` - The BlockMeta's wire-agnostic view.
    ///
    /// # Errors
    ///
    /// [`UntrackedSlot`] if the bank was discarded, if it already has a BlockMeta waiting for its
    /// footer, or if the state machine rejects the resulting `BlockSummary`.
    ///
    pub fn handle_block_meta(&mut self, block_meta: BlockMetaEvInfo) -> Result<(), UntrackedSlot> {
        let bank_id = block_meta.bank_id;
        // An already-frozen bank's duplicate goes straight to the state machine, which rejects it.
        if !self.config.require_block_footer || self.sm.is_bank_frozen(bank_id) {
            return self.freeze(block_meta);
        }
        if !self.sm.is_bank_trackable(bank_id) {
            return Err(UntrackedSlot);
        }
        if self.footer_awaiting_block_meta.remove(&bank_id).is_some() {
            return self.freeze(block_meta);
        }
        if self.block_meta_awaiting_footer.contains_key(&bank_id) {
            tracing::error!(
                "UNEXPECTED: duplicate BlockMeta for bank {bank_id} (slot {}) still waiting for its block footer. Dropping.",
                block_meta.slot
            );
            return Err(UntrackedSlot);
        }
        self.block_meta_awaiting_footer
            .insert(bank_id, (block_meta, Instant::now()));
        // The bank's block did end on the wire, even if it can't freeze yet. Counting it keeps
        // `gc` (and the eviction above) running even if footers never arrive at all.
        self.maybe_run_gc_after_completed_slot();
        Ok(())
    }

    ///
    /// Handles a bank's block footer. With footers required, freezes the bank if its BlockMeta
    /// already arrived, otherwise records the footer so the BlockMeta freezes it on arrival.
    ///
    /// # Arguments
    ///
    /// * `footer` - The footer's wire-agnostic view.
    ///
    /// # Errors
    ///
    /// [`UntrackedSlot`] if the bank was discarded, if it already has a footer waiting for its
    /// BlockMeta, or if the state machine rejects the resulting `BlockSummary`.
    ///
    pub fn handle_block_footer(&mut self, footer: &BlockFooterEvInfo) -> Result<(), UntrackedSlot> {
        let bank_id = footer.bank_id;
        if !self.sm.is_bank_trackable(bank_id) {
            // A BlockMeta it was waiting for will never freeze a discarded bank.
            self.block_meta_awaiting_footer.remove(&bank_id);
            return Err(UntrackedSlot);
        }
        // Nothing to drive: the bank already froze (a duplicate footer, which the accumulator
        // reports), or footers aren't part of the end of block.
        if !self.config.require_block_footer || self.sm.is_bank_frozen(bank_id) {
            return Ok(());
        }
        if let Some((block_meta, _)) = self.block_meta_awaiting_footer.remove(&bank_id) {
            return self.freeze(block_meta);
        }
        if self.footer_awaiting_block_meta.contains_key(&bank_id) {
            tracing::error!(
                "UNEXPECTED: duplicate block footer for bank {bank_id} (slot {}) still waiting for its BlockMeta. Dropping.",
                footer.slot
            );
            return Err(UntrackedSlot);
        }
        self.footer_awaiting_block_meta
            .insert(bank_id, Instant::now());
        Ok(())
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
            GeyserEventInfo::BankData { bank_id, .. }
            | GeyserEventInfo::SysvarAccount { bank_id, .. } => {
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
        GeyserEventInfo::Entry(EntryEvInfo {
            slot,
            index: 0,
            starting_transaction_index: 0,
            executed_transaction_count: 0,
            hash: Hash::new_unique().to_bytes(),
            bank_id,
        })
    }

    fn block_meta(slot: Slot, bank_id: BankId) -> GeyserEventInfo {
        GeyserEventInfo::BlockMeta(BlockMetaEvInfo {
            slot,
            parent_slot: slot - 1,
            entries_count: 1,
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

    ///
    /// Feeds `CreatedBank` and the bank's single entry.
    ///
    fn start_bank(w: &mut BlocksStateMachineWrapper, slot: Slot, bank_id: BankId) {
        w.handle_new_geyser_event(slot_update(
            slot,
            SlotStatusKind::CreatedBank,
            Some(bank_id),
        ))
        .unwrap();
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

        w.evict_stale_end_of_block_markers(Instant::now());
        assert_eq!(w.banks_awaiting_footer(), 1, "too young to evict");

        w.evict_stale_end_of_block_markers(
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
}
