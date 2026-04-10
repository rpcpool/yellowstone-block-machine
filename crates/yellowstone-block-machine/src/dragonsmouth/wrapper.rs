use {
    crate::{
        forks::Forks,
        state_machine::{
            BlockStateMachineOutput, BlockSummary, BlocksStateMachine, DeadletterEvent, EntryInfo,
            SlotCommitmentStatusUpdate, SlotLifecycle, SlotLifecycleUpdate, UntrackedSlot,
        },
    }, solana_clock::Slot, solana_commitment_config::CommitmentLevel, solana_hash::Hash, yellowstone_grpc_proto::geyser::{
        SlotStatus, SubscribeUpdate, SubscribeUpdateBlockMeta, SubscribeUpdateEntry,
        SubscribeUpdateSlot, subscribe_update::UpdateOneof,
    }
};

pub const RESERVED_FILTER_NAME: &str = "_block-machine";
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

impl From<SubscribeUpdateEntry> for EntryInfo {
    fn from(value: SubscribeUpdateEntry) -> Self {
        Self {
            entry_hash: Hash::new_from_array(value.hash.try_into().expect("entry format")),
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

    pub fn handle_block_entry(
        &mut self,
        entry: &SubscribeUpdateEntry,
    ) -> Result<(), UntrackedSlot> {
        let entry_info: EntryInfo = entry.clone().into();
        self.sm.process_replay_event(entry_info.into())
    }

    #[allow(clippy::collapsible_else_if)]
    pub fn handle_slot_update(
        &mut self,
        slot_update: &SubscribeUpdateSlot,
    ) -> Result<(), UntrackedSlot> {
        let slot_status = slot_update.status();
        const LIFE_CYCLE_STATUS: [SlotStatus; 4] = [
            SlotStatus::SlotFirstShredReceived,
            SlotStatus::SlotCompleted,
            SlotStatus::SlotCreatedBank,
            SlotStatus::SlotDead,
        ];

        if LIFE_CYCLE_STATUS.contains(&slot_status) {
            let lifecycle_update = SlotLifecycleUpdate {
                slot: slot_update.slot,
                parent_slot: slot_update.parent,
                stage: match slot_status {
                    SlotStatus::SlotFirstShredReceived => SlotLifecycle::FirstShredReceived,
                    SlotStatus::SlotCompleted => SlotLifecycle::Completed,
                    SlotStatus::SlotCreatedBank => SlotLifecycle::CreatedBank,
                    SlotStatus::SlotDead => SlotLifecycle::Dead,
                    _ => unreachable!(),
                },
            };
            self.sm.process_replay_event(lifecycle_update.into())?;
        } else {
            if slot_update.dead_error.is_some() {
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
                    commitment: match slot_status {
                        SlotStatus::SlotProcessed => CommitmentLevel::Processed,
                        SlotStatus::SlotConfirmed => CommitmentLevel::Confirmed,
                        SlotStatus::SlotFinalized => CommitmentLevel::Finalized,
                        _ => unreachable!(),
                    },
                };

                self.sm
                    .process_consensus_event(commitment_level_update.into());
            }
        }
        Ok(())
    }

    pub fn handle_block_meta(
        &mut self,
        block_meta: &SubscribeUpdateBlockMeta,
    ) -> Result<(), UntrackedSlot> {
        let bh = bs58::decode(block_meta.blockhash.as_str())
            .into_vec()
            .expect("blockhash format");
        let block_summary = BlockSummary {
            slot: block_meta.slot,
            entry_count: block_meta.entries_count,
            parent_slot: block_meta.parent_slot,
            executed_transaction_count: block_meta.executed_transaction_count,
            blockhash: Hash::new_from_array(bh.try_into().expect("blockhash length")),
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

    pub fn fork_graph(&self) -> &Forks {
        &self.sm.forks
    }

    #[inline]
    pub fn pop_next_dlq(&mut self) -> Option<DeadletterEvent> {
        self.sm.pop_next_dlq()
    }

    pub fn handle_new_geyser_event(
        &mut self,
        event: &SubscribeUpdate,
    ) -> Result<(), UntrackedSlot> {
        let SubscribeUpdate {
            filters: _,
            created_at: _,
            update_oneof,
        } = event;
        let Some(update_oneof) = update_oneof else {
            return Ok(());
        };
        let result = match update_oneof {
            UpdateOneof::Slot(subscribe_update_slot) => {
                self.handle_slot_update(subscribe_update_slot)
            }
            UpdateOneof::BlockMeta(subscribe_update_block_meta) => {
                self.handle_block_meta(subscribe_update_block_meta)
            }
            UpdateOneof::Entry(subscribe_update_entry) => {
                self.handle_block_entry(subscribe_update_entry)
            }
            UpdateOneof::Transaction(tx) => {
                let slot = tx.slot;
                if !self.sm.is_slot_tracked(slot) {
                    return Err(UntrackedSlot);
                }
                // Transactions are not currently used in block reconstruction
                Ok(())
            }
            UpdateOneof::Account(account) => {
                let slot = account.slot;
                if !self.sm.is_slot_tracked(slot) {
                    return Err(UntrackedSlot);
                }
                // Accounts are not currently used in block reconstruction
                Ok(())
            }
            UpdateOneof::TransactionStatus(tx) => {
                let slot = tx.slot;
                if !self.sm.is_slot_tracked(slot) {
                    return Err(UntrackedSlot);
                }
                // Transaction statuses are not currently used in block reconstruction
                Ok(())
            }
            _ => {
                // Other event types are not currently used in block reconstruction
                Ok(())
            }
        };

        result
    }
}
