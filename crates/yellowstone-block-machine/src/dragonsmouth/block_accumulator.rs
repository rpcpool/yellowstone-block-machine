use {
    crate::{
        dragonsmouth::RESERVED_FILTER_NAME,
        event::GeyserEventInfo,
        state_machine::FrozenBlock,
        stream::{Block, BlockAccumulator, SimpleBlockStore},
    },
    rustc_hash::FxHashMap,
    solana_clock::Slot,
    solana_hash::HASH_BYTES,
    yellowstone_grpc_proto::geyser::SubscribeUpdate,
};

#[derive(Debug)]
struct BlockBuffer<E> {
    blockhash: [u8; HASH_BYTES],
    events: Vec<E>,
    account_idx_map: Vec<usize>,
    transaction_idx_map: Vec<usize>,
    entry_idx_map: Vec<usize>,
    other_idx_map: Vec<usize>,
}

impl<E> Default for BlockBuffer<E> {
    fn default() -> Self {
        Self {
            blockhash: [0; HASH_BYTES],
            events: Vec::new(),
            account_idx_map: Vec::new(),
            transaction_idx_map: Vec::new(),
            entry_idx_map: Vec::new(),
            other_idx_map: Vec::new(),
        }
    }
}

impl<E> BlockBuffer<E> {
    fn finish(self, slot: Slot) -> Block<SimpleBlockStore<E>> {
        Block {
            slot,
            blockhash: self.blockhash,
            events: SimpleBlockStore {
                events: self.events,
                account_idx_map: self.account_idx_map,
                transaction_idx_map: self.transaction_idx_map,
                entry_idx_map: self.entry_idx_map,
                other_idx_map: self.other_idx_map,
                slot,
            },
        }
    }
}

///
/// An in-memory store for blocks being reconstructed.
///
/// It maintains active blocks (currently being reconstructed) and frozen blocks (fully reconstructed).
#[derive(Default)]
pub struct DragonsmouthBlockCumulator {
    active_block_map: FxHashMap<Slot, BlockBuffer<SubscribeUpdate>>,
    frozen_block_map: FxHashMap<Slot, BlockBuffer<SubscribeUpdate>>,
}

impl BlockAccumulator for DragonsmouthBlockCumulator {
    type EventT = SubscribeUpdate;
    type EventStore = SimpleBlockStore<SubscribeUpdate>;

    fn add_event(&mut self, mut event: SubscribeUpdate, slot: Slot, ev_info: &GeyserEventInfo) {
        let block = self.active_block_map.entry(slot).or_default();
        let idx = block.events.len();

        // We only match on those three since this is where we add the reserved filter to the event.
        match ev_info {
            GeyserEventInfo::Slot(_)
            | GeyserEventInfo::BlockMeta(_)
            | GeyserEventInfo::Entry(_) => {
                event.filters.retain(|f| f != RESERVED_FILTER_NAME);

                if event.filters.is_empty() {
                    // Skip events that are only present because of the crate's internal reserved filter.
                    return;
                }
            }
            _ => {}
        }

        match ev_info {
            GeyserEventInfo::Account { .. } => block.account_idx_map.push(idx),
            GeyserEventInfo::Transaction { .. } => block.transaction_idx_map.push(idx),
            GeyserEventInfo::Entry(_) => {
                // Skip entries that are only present because of the crate's internal reserved filter.
                block.entry_idx_map.push(idx);
            }
            GeyserEventInfo::Other { .. } => block.other_idx_map.push(idx),
            _ => {
                //block meta and slot are ignored
                return;
            }
        }
        block.events.push(event);
    }

    fn freeze_block(&mut self, frozen_block_info: FrozenBlock) {
        let Some(mut block) = self.active_block_map.remove(&frozen_block_info.slot) else {
            return;
        };
        block.blockhash = frozen_block_info.blockhash.to_bytes();
        self.frozen_block_map.insert(frozen_block_info.slot, block);
    }

    fn finish_block(&mut self, slot: Slot) -> Option<Block<Self::EventStore>> {
        let acc = self.frozen_block_map.remove(&slot)?;
        Some(acc.finish(slot))
    }

    fn prune_block(&mut self, slot: Slot) {
        self.active_block_map.remove(&slot);
        self.frozen_block_map.remove(&slot);
    }
}
