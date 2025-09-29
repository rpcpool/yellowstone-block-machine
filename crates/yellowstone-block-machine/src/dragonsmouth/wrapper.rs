use {
    crate::state_machine::{
        BlockStateMachineOutput, BlockSummary, BlocksStateMachine, EntryInfo,
        SlotCommitmentStatusUpdate, SlotLifecycle, SlotLifecycleUpdate,
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
    pub fn handle_block_entry(&mut self, entry: &SubscribeUpdateEntry) {
        let entry_info: EntryInfo = entry.clone().into();
        self.sm.process_event(entry_info.into());
    }

    pub fn handle_slot_update(&mut self, slot_update: &SubscribeUpdateSlot) {
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
            self.sm.process_event(lifecycle_update.into());
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

            self.sm.process_event(commitment_level_update.into());
        }
    }

    pub fn handle_block_meta(&mut self, block_meta: &SubscribeUpdateBlockMeta) {
        let bh = bs58::decode(block_meta.blockhash.as_str())
            .into_vec()
            .expect("blockhash format");
        let block_summary = BlockSummary {
            slot: block_meta.slot,
            entry_count: block_meta.entries_count,
            executed_transaction_count: block_meta.executed_transaction_count,
            blockhash: Hash::new_from_array(bh.try_into().expect("blockhash length")),
        };
        self.sm.process_event(block_summary.into());
        // Currently not used in block reconstruction
    }

    pub fn handle_new_geyser_event(&mut self, event: &SubscribeUpdate) {
        let SubscribeUpdate {
            filters: _,
            created_at: _,
            update_oneof,
        } = event;
        let Some(update_oneof) = update_oneof else {
            return;
        };
        match update_oneof {
            UpdateOneof::Slot(subscribe_update_slot) => {
                self.handle_slot_update(subscribe_update_slot);
            }
            UpdateOneof::BlockMeta(subscribe_update_block_meta) => {
                self.handle_block_meta(subscribe_update_block_meta);
            }
            UpdateOneof::Entry(subscribe_update_entry) => {
                self.handle_block_entry(subscribe_update_entry);
            }
            _ => {
                tracing::trace!("Unsupported update type received: {:?}", update_oneof);
            }
        }
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
