use {
    crate::state_machine::{BlockstoreSM, EntryInfo},
    futures_util::{Sink, StreamExt, stream::BoxStream},
    rustc_hash::FxHashMap,
    solana_sdk::{clock::Slot, hash::Hash},
    std::{collections::HashMap, ops::Sub, sync::Arc},
    tokio::sync::mpsc,
    tonic::async_trait,
    yellowstone_grpc_client::{GeyserGrpcClient, GeyserGrpcClientError, Interceptor},
    yellowstone_grpc_proto::geyser::{
        CommitmentLevel, SubscribeRequest, SubscribeRequestFilterAccounts,
        SubscribeRequestFilterSlots, SubscribeRequestFilterTransactions, SubscribeUpdate,
        SubscribeUpdateAccount, SubscribeUpdateEntry, SubscribeUpdateTransaction,
        geyser_client::GeyserClient, subscribe_update::UpdateOneof,
    },
};

#[derive(Default)]
pub struct DragonsmouthBlockMachine {
    accounts: HashMap<String, SubscribeRequestFilterAccounts>,
    transactions: HashMap<String, SubscribeRequestFilterAccounts>,
}

#[async_trait]
pub trait GeyserGrpcExt<F> {
    fn this(&mut self) -> &mut GeyserGrpcClient<F>;

    async fn subscribe_block(
        &mut self,
        subscribe_request: SimplifiedSubscribeRequest,
        commitment_level: CommitmentLevel,
    ) -> Result<BlockStream, GeyserGrpcClientError>;
}

pub struct SimplifiedSubscribeRequest {
    pub accounts: HashMap<String, SubscribeRequestFilterAccounts>,
    pub transactions: HashMap<String, SubscribeRequestFilterTransactions>,
}

#[derive(Debug, Default)]
struct Block {
    accounts: Vec<Arc<SubscribeUpdateAccount>>,
    transactions: Vec<Arc<SubscribeUpdateTransaction>>,
}

struct BlockStream {
    receiver: BoxStream<'static, Result<Block, tonic::Status>>,
}

#[async_trait]
impl<F> GeyserGrpcExt<F> for GeyserGrpcClient<F>
where
    F: Interceptor + Send + Sync + 'static,
{
    fn this(&mut self) -> &mut GeyserGrpcClient<F> {
        self
    }

    async fn subscribe_block(
        &mut self,
        subscribe_request: SimplifiedSubscribeRequest,
        commitment_level: CommitmentLevel,
    ) -> Result<BlockStream, GeyserGrpcClientError> {
        let this = self.this();
        let request = SubscribeRequest {
            accounts: subscribe_request.accounts,
            transactions: subscribe_request.transactions,
            slots: HashMap::from([(
                "_block-machine".to_owned(),
                SubscribeRequestFilterSlots {
                    interslot_updates: Some(true),
                    ..Default::default()
                },
            )]),
            blocks_meta: HashMap::from([("_block-machine".to_owned(), Default::default())]),
            entry: HashMap::from([("_block-machine".to_owned(), Default::default())]),
            commitment: Some(0), // Processed
            ..Default::default()
        };
        let (sink, source) = self.this().subscribe_with_request(Some(request)).await?;

        let source = source.boxed();
        let sm = BlockstoreSM::default();

        let rt = DragonsmouthBlockMachineRuntime {
            source,
            blockstore: InMemoryBlockStore::new(),
            sm,
        };

        todo!()
    }
}

struct DragonsmouthBlockMachineRuntime<Src> {
    source: Src,
    blockstore: InMemoryBlockStore,
    sm: BlockstoreSM,
}

impl<Src> DragonsmouthBlockMachineRuntime<Src>
where
    Src: StreamExt<Item = Result<SubscribeUpdate, tonic::Status>> + Unpin,
{
    async fn handle_new_geyser_event(&mut self, event: SubscribeUpdate) {
        let SubscribeUpdate {
            filters,
            created_at,
            update_oneof,
        } = event;
        let Some(update_oneof) = update_oneof else {
            return;
        };
        match update_oneof {
            UpdateOneof::Account(subscribe_update_account) => {
                self.blockstore.insert_account(subscribe_update_account);
            }
            UpdateOneof::Slot(subscribe_update_slot) => todo!(),
            UpdateOneof::Transaction(subscribe_update_transaction) => {
                self.blockstore.insert_tx(subscribe_update_transaction);
            }
            UpdateOneof::BlockMeta(subscribe_update_block_meta) => todo!(),
            UpdateOneof::Entry(subscribe_update_entry) => {
                let sm_ev = EntryInfo {};
            }
            _ => {
                tracing::trace!("Unsupported update type received: {:?}", update_oneof);
            }
        }
    }

    pub async fn run(mut self) {
        loop {
            tokio::select! {
                maybe = self.source.next() => {
                    match maybe {
                        Some(data) => {

                        }
                        None => {
                            break;
                        }
                    }
                }
            }
        }
    }
}

impl From<SubscribeUpdateEntry> for EntryInfo {
    fn from(entry: SubscribeUpdateEntry) -> Self {
        // Convert SubscribeUpdateEntry to EntryInfo
        // This is a placeholder implementation, adjust as needed
        Self {
            slot: entry.slot,
            entry_index: entry.index,
            starting_txn_index: entry.starting_transaction_index,
            entry_hash: Hash::entry.hash,
            executed_txn_count: todo!(),
        }
    }
}

struct InMemoryBlockStore {
    blocks: FxHashMap<Slot, Block>,
}

impl InMemoryBlockStore {
    pub fn new() -> Self {
        Self {
            blocks: FxHashMap::default(),
        }
    }

    pub fn insert_account(&mut self, account: SubscribeUpdateAccount) {
        let slot = account.slot;
        self.blocks
            .entry(slot)
            .or_default()
            .accounts
            .push(Arc::new(account));
    }

    pub fn insert_tx(&mut self, transaction: SubscribeUpdateTransaction) {
        let slot = transaction.slot;
        self.blocks
            .entry(slot)
            .or_default()
            .transactions
            .push(Arc::new(transaction));
    }
}
