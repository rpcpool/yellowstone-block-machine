use {
    crate::{
        event::{BlockMetaEvInfo, EntryEvInfo, GeyserEventInfo, SlotStatusKind, SlotUpdateEvInfo},
        forks::Forks,
        state_machine::{
            BlockStateMachineOutput, BlockSummary, BlocksStateMachine, DeadletterEvent, EntryInfo,
            SlotCommitmentStatusUpdate, SlotLifecycle, SlotLifecycleUpdate, UntrackedSlot,
        },
    },
    solana_clock::Slot,
    solana_commitment_config::CommitmentLevel,
    solana_hash::Hash,
};

const STATE_MACHINE_GC_EVERY_COMPLETED_SLOTS: usize = 10;

///
/// The core state machine that processes incoming Geyser events and produces block machine outputs.
///
/// Mainly a Wrapper to translate Grpc events to State Machine events.
///
#[derive(Debug, Default)]
pub struct BlocksStateMachineWrapper {
    pub sm: BlocksStateMachine,
    completed_slots_since_last_gc: usize,
    slot_gc_tracer: Option<Vec<Slot>>,
}

impl From<EntryEvInfo> for EntryInfo {
    fn from(value: EntryEvInfo) -> Self {
        Self {
            entry_hash: Hash::new_from_array(value.hash),
            slot: value.slot,
            entry_index: value.index,
            starting_txn_index: value.starting_transaction_index,
            executed_txn_count: value.executed_transaction_count,
        }
    }
}

impl BlocksStateMachineWrapper {
    pub fn new() -> Self {
        Self {
            sm: BlocksStateMachine::default(),
            completed_slots_since_last_gc: 0,
            slot_gc_tracer: None,
        }
    }

    pub fn new_with_slot_gc_tracing() -> Self {
        Self {
            sm: BlocksStateMachine::default(),
            completed_slots_since_last_gc: 0,
            slot_gc_tracer: Some(Vec::with_capacity(10)),
        }
    }

    ///
    /// Pops the next slot that has been garbage collected by the state machine, if slot GC tracing is enabled.
    ///
    #[inline]
    pub fn pop_slot_gc_trace(&mut self) -> Option<Slot> {
        let tracer = self.slot_gc_tracer.as_mut()?;
        tracer.pop()
    }

    fn maybe_run_gc_after_completed_slot(&mut self) {
        self.completed_slots_since_last_gc += 1;
        if self.completed_slots_since_last_gc >= STATE_MACHINE_GC_EVERY_COMPLETED_SLOTS {
            self.sm.gc(self.slot_gc_tracer.as_mut());
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
            };
            self.sm.process_replay_event(lifecycle_update.into())?;
        } else {
            if slot_update.dead_error {
                // Downgrade to lifecycle update
                let lifecycle_update = SlotLifecycleUpdate {
                    slot: slot_update.slot,
                    parent_slot: slot_update.parent,
                    stage: SlotLifecycle::Dead,
                };
                self.sm.process_replay_event(lifecycle_update.into())?;
            } else {
                let commitment_level_update = SlotCommitmentStatusUpdate {
                    parent_slot: slot_update.parent,
                    slot: slot_update.slot,
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

    pub fn handle_block_meta(&mut self, block_meta: BlockMetaEvInfo) -> Result<(), UntrackedSlot> {
        let block_summary = BlockSummary {
            slot: block_meta.slot,
            entry_count: block_meta.entries_count,
            parent_slot: block_meta.parent_slot,
            executed_transaction_count: block_meta.executed_transaction_count,
            blockhash: Hash::new_from_array(block_meta.blockhash),
            parent_blockhash: block_meta.parent_blockhash.map(Hash::new_from_array),
            block_time: block_meta.block_time,
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

    pub fn fork_graph(&self) -> &Forks<Slot> {
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
            GeyserEventInfo::Transaction { slot }
            | GeyserEventInfo::Account { slot }
            | GeyserEventInfo::Other { slot } => {
                if !self.sm.is_slot_tracked(slot) {
                    return Err(UntrackedSlot);
                }
                Ok(())
            }
        }
    }
}
