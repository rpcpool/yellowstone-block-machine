use {
    crate::state_machine::{
        BlockStateMachineOutput, BlockSummary, BlocksStateMachine, DeadletterEvent, EntryInfo,
        SlotCommitmentStatusUpdate, SlotLifecycle, SlotLifecycleUpdate, UntrackedSlot,
    },
    solana_commitment_config::CommitmentLevel,
    solana_hash::Hash,
    yellowstone_grpc_proto::geyser::{
        SlotStatus, SubscribeUpdate, SubscribeUpdateBlockMeta, SubscribeUpdateEntry,
        SubscribeUpdateSlot, subscribe_update::UpdateOneof,
    },
};

pub const RESERVED_FILTER_NAME: &str = "_block-machine";

///
/// The core state machine that processes incoming Geyser events and produces block machine outputs.
///
/// Mainly a Wrapper to translate Grpc events to State Machine events.
///
#[derive(Default)]
pub struct BlocksStateMachineWrapper {
    sm: BlocksStateMachine,
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
    pub fn handle_block_entry(
        &mut self,
        entry: &SubscribeUpdateEntry,
    ) -> Result<(), UntrackedSlot> {
        let entry_info: EntryInfo = entry.clone().into();
        self.sm.process_replay_event(entry_info.into())
    }

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
            executed_transaction_count: block_meta.executed_transaction_count,
            blockhash: Hash::new_from_array(bh.try_into().expect("blockhash length")),
        };
        self.sm.process_replay_event(block_summary.into())
        // Currently not used in block reconstruction
    }

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
        match update_oneof {
            UpdateOneof::Slot(subscribe_update_slot) => {
                self.handle_slot_update(subscribe_update_slot)?;
            }
            UpdateOneof::BlockMeta(subscribe_update_block_meta) => {
                self.handle_block_meta(subscribe_update_block_meta)?;
            }
            UpdateOneof::Entry(subscribe_update_entry) => {
                self.handle_block_entry(subscribe_update_entry)?;
            }
            UpdateOneof::Transaction(tx) => {
                let slot = tx.slot;
                if !self.sm.is_slot_tracked(slot) {
                    return Err(UntrackedSlot);
                }
                // Transactions are not currently used in block reconstruction
            }
            UpdateOneof::Account(account) => {
                let slot = account.slot;
                if !self.sm.is_slot_tracked(slot) {
                    return Err(UntrackedSlot);
                }
                // Accounts are not currently used in block reconstruction
            }
            UpdateOneof::TransactionStatus(tx) => {
                let slot = tx.slot;
                if !self.sm.is_slot_tracked(slot) {
                    return Err(UntrackedSlot);
                }
                // Transaction statuses are not currently used in block reconstruction
            }
            _ => {
                // Other event types are not currently used in block reconstruction
            }
        }
        Ok(())
    }

    pub fn drain_unprocess_output<Ext, O>(&mut self, out: &mut Ext)
    where
        O: From<BlockStateMachineOutput>,
        Ext: Extend<O>,
    {
        while let Some(ev) = self.sm.pop_next_unprocess_blockstore_update() {
            out.extend([ev.into()]);
        }
    }
}
