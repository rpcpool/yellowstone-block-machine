//! Yellowstone Block Machine
//!
//! This crate provides a block machine implementation that processes Solana Geyser events such that block reconstruction
//! is done right!
//!
//! The block machine processes Geyser events and produces blocks that contain all transactions and account states
//! that were part of the block when it was originally produced by the Solana node.
//!
//! The block machine also detects forks and dead blocks, and provides slot commitment updates.
//!
//! The block machine can be used as a library or as a standalone application (dragonsmouth).
//!
//! # Challenge rebuilding blocks from Geyser events
//!
//! Trying to rebuild a block from Geyser events can be challenging due to the multiple rules and edge cases
//! surrounding slot lifecycle events and commitment level updates.
//!
//! The bootstrapping problem: typically you receive geyser event via Dragonsmouth's gRPC interface.
//! However, the first couple of events you receive may be in the middle of a slot, or even at the end of a slot.
//! You would have to discard these events since you don't know if you missed any events at the beginning of the slot.
//!
//! To make sure you have received all events for a slot, you must make sure your program detected
//! either SlotStatus::FIRST_SHRED_RECEIVED or SlotStatus::BANK_CREATED for the slot, otherwise
//! you may have missed some events at the beginning of the slot.
//!
//! To better understand the slot lifecycle, the following section provides a detailed overview of the various
//! slot status events and their significance.
//!
//! # Intra-slot Update
//!
//! ```ignore
//! enum SlotStatus {
//!   ...
//!   SLOT_FIRST_SHRED_RECEIVED = 3;
//!   SLOT_COMPLETED = 4;
//!   SLOT_CREATED_BANK = 5;
//!   SLOT_DEAD = 6;
//! }
//! ```
//!
//!
//! - SLOT_FIRST_SHRED_RECEIVED: The remote RPC node you're connected to has received the first shred of a given slot. This does not indicate it has been replayed yet. This event occurs during the retransmit stage in the TVU.
//! - SLOT_CREATED_BANK: A bank for the given slot has been created on the remote RPC node you're connected to. Within a validator, a Bank acts as an isolated execution environment during the replay stage (which follows the retransmit stage). Due to the decentralized nature of blockchains, forks are inevitable, meaning a slot can have multiple descendants.
//!   To handle this, validators must be capable of replaying multiple slots that share the same ancestor without their execution interfering with one another. Each slot is assigned its own Bank instance, and these Banks form a fork graph, where each edge represents a parent-child relationship between two banks.
//!   Banks serve as self-contained execution contexts, maintaining replay results and essential metadata about the slot and its lineage. Importantly, a Bank is instantiated once per slot.
//! - SLOT_COMPLETED: All the shreds for the given slot have been received by the RPC node you're connected to. However, this does not necessarily mean that the slot has been fully replayed yet.
//! - SLOT_DEAD: Dead slots are slots that have been rejected by the validator for various reasons, such as invalid transaction signatures in the leader's shreds, incorrect entry hashes during Proof of History (PoH) verification, or an unexpected number of entries in the slot. When a slot is marked as dead, it is discarded by the network as a whole and effectively skipped. This can occur at any point during the replay process, even after the slot has been marked as 'completed'.
//!
//! Here's a "simplfied" overview of the expected lifecycle of a slot:\
//!                                                                                                                                             
//!                                                                                                                                       
//!                                                                                                                                       
//!                                     TIME ->                                                                                           
//! ──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────►   
//! ┌───────────────────────────────────────────────────────┐                                                                             
//! │ Slot download                                         │                                                                             
//! │ ┌───────────┐┌──────┐         ┌───────┐┌───────────┐  │                                                                             
//! │ │FIRST_SHRED││SHRED2│  ...    │SHRED N││ COMPLETED │  │                                                                             
//! │ │ RECEIVED  │└──────┘         └───────┘└───────────┘  │                                                                             
//! │ └───────────                                          │                                                                             
//! └──────────────┌───────────────────────────────────────────────────────────────────────────────┐                                      
//!                │ REPLAY STAGE                                                                  │                                      
//!                │┌─────────────┐ ┌──────────────┐ ┌───┌───┐┌──────┐    ┌──────────┐ ┌─────────┐ │                                      
//!                ││BANK_CREATED │ │ACCOUNT UPDATE│ │TX1│TX2││ENTRY1│... │BLOCK_META│ │PROCESSED│ │                                      
//!                │└─────────────┘ └──────────────┘ └───└───┘└──────┘    └──────────┘ └─────────┘ │                                      
//!                │                                                                               │                                      
//!                └───────────────────────────────────────────────────────────────────────────────┘                                      
//!                                                                                       ┌──────────────────────────────────┐    
//!                                                                                       │ CONSENSUS                        │    
//!                                                                                       │ ┌──────────┐      ┌───────────┐  │    
//!                                                                                       │ │CONFIRMED │      │FINALIZED  │  │    
//!                                                                                       │ └──────────┘      └───────────┘  │    
//!                                                                                       │                                  │    
//!                                                                                       └──────────────────────────────────┘    
//!                                                                                                                                       
//!
//!
//!
//! ## IMPORTANT QUIRKS
//!
//! - Sometimes, BANK_CREATED is received before FIRST_SHRED_RECEIVED. This is because of internal Agave logic which sends
//!   FIRST_SHRED_RECEIVED on a queue and BANK_CREATED directly via a callback.
//! - DEAD slots can be received at any time, even after COMPLETED.
//! - COMPLETED can be received after BlockMeta and any Commitment Level status update.
//! - A slot can receive PROCESSED or CONFIRMED before BlockMeta.
//!
//!
//! # Dragonsmouth Integration Examples
//!
//! The easiest way to use the block machine is via the dragonsmouth module, which provides
//! integration with the dragonsmouth gRPC interface.
//!
//!
//! ```ignore
//! #[derive(Debug, Clone, serde::Deserialize)]
//! struct Config {
//!     endpoint: String,
//!     x_token: Option<String>,
//! }
//!
//! async fn process_block<W>(
//!     mut block_recv: mpsc::Receiver<Result<BlockMachineOutput, BlockMachineError>>,
//!     sample: usize,
//!     mut out: W,
//! ) where
//!     W: std::io::Write,
//! {
//!     let mut i = 0;
//!     while let Some(result) = block_recv.recv().await {
//!         match result {
//!             Ok(output) => match output {
//!                 BlockMachineOutput::Block(block) => {
//!                     let n = block.len();
//!                     let slot = block.slot;
//!                     let account_cnt = block.account_len();
//!                     let txn_cnt = block.txn_len();
//!                     let entry_cnt = block.entry_len();
//!                     writeln!(out, "Block {slot} len: {n}, {txn_cnt} tx, {account_cnt} accounts, {entry_cnt} entries").expect("write");
//!                     i += 1;
//!                 }
//!                 BlockMachineOutput::SlotCommitmentUpdate(slot_commitment_status_update) => {
//!                     writeln!(
//!                         out,
//!                         "SlotCommitmentUpdate: {:?}",
//!                         slot_commitment_status_update
//!                     )
//!                     .expect("write");
//!                 }
//!                 BlockMachineOutput::ForkDetected(fork_detected) => {
//!                     writeln!(out, "ForkDetected: {}", fork_detected.slot).expect("write");
//!                 }
//!                 BlockMachineOutput::DeadBlockDetect(dead_block_detected) => {
//!                     writeln!(out, "DeadBlockDetect: {}", dead_block_detected.slot).expect("write");
//!                 }
//!             },
//!             Err(e) => {
//!                 writeln!(out, "BlockMachineError: {:?}", e).expect("write");
//!                 break;
//!             }
//!         }
//!         if i >= sample {
//!             break;
//!         }
//!     }
//! }
//!
//! #[tokio::main]
//! async fn main() {
//!     init_tracing();
//!     let args = Args::parse();
//!     let config: Config =
//!         serde_yaml::from_reader(std::fs::File::open(args.config).unwrap()).expect("open config");
//!     let endpoint = config.endpoint;
//!     let x_token = config.x_token;
//!     let mut geyser = GeyserGrpcBuilder::from_shared(endpoint)
//!         .expect("Failed to parse endpoint")
//!         .x_token(x_token)
//!         .expect("x_token")
//!         .tls_config(ClientTlsConfig::new().with_native_roots())
//!         .expect("tls_config")
//!         .accept_compressed(tonic::codec::CompressionEncoding::Zstd)
//!         .connect()
//!         .await
//!         .expect("Failed to connect to geyser");
//!
//!     // This request listen for all account updates and transaction updates
//!     let request = SubscribeRequest {
//!         accounts: hash_map! {
//!             "test".to_string() => Default::default(),
//!         },
//!         transactions: hash_map! {
//!             "test".to_string() => Default::default(),
//!         },
//!         entry: hash_map! {
//!             "test".to_string() => Default::default(),
//!         },
//!         commitment: Some(CommitmentLevel::Confirmed as i32),
//!         ..Default::default()
//!     };
//!
//!     let block_machine_rx = geyser
//!         .subscribe_block(request)
//!         .await
//!         .expect("subscribe_block");
//!     process_block(block_machine_rx, args.samples, std::io::stdout()).await;
//! }
//! ```
//!
//! # How to build your custom block machine integration
//!
//! You can build your own block machine integration by building a driver that feeds [`yellowstone_block_machine::state_machine::BlockSM`] instance.
//!
//! You must feed the state machine with every:
//! 1. Block entries
//! 2. Block meta summary (FINAL message to make the block freeze)
//! 3. Slot lifecycle events (SLOT_FIRST_SHRED_RECEIVED, SLOT_COMPLETED, SLOT_CREATED_BANK, SLOT_DEAD)
//! 4. Commitment level updates (PROCESSED, CONFIRMED, FINALIZED)
//!
//!
//! ```ignore
//! struct DragonsmouthBlockMachine {
//!     minimum_commitment_level: CommitmentLevel,
//!     block_storage: InMemoryBlockStore,
//!     sm: BlockSM,
//! }
//!
//! impl From<SubscribeUpdateEntry> for EntryInfo {
//!     fn from(value: SubscribeUpdateEntry) -> Self {
//!         Self {
//!             entry_hash: Hash::new_from_array(value.hash.try_into().expect("entry format")),
//!             slot: value.slot,
//!             entry_index: value.index,
//!             starting_txn_index: value.starting_transaction_index,
//!             executed_txn_count: value.executed_transaction_count,
//!         }
//!     }
//! }
//!
//! fn compare_commitment(cl1: CommitmentLevel, cl2: CommitmentLevel) -> Ordering {
//!     match (cl1, cl2) {
//!         (CommitmentLevel::Processed, CommitmentLevel::Processed) => Ordering::Equal,
//!         (CommitmentLevel::Confirmed, CommitmentLevel::Confirmed) => Ordering::Equal,
//!         (CommitmentLevel::Finalized, CommitmentLevel::Finalized) => Ordering::Equal,
//!         (CommitmentLevel::Processed, _) => Ordering::Less,
//!         (CommitmentLevel::Confirmed, CommitmentLevel::Processed) => Ordering::Greater,
//!         (CommitmentLevel::Finalized, CommitmentLevel::Processed) => Ordering::Greater,
//!         (CommitmentLevel::Finalized, CommitmentLevel::Confirmed) => Ordering::Greater,
//!         (CommitmentLevel::Confirmed, CommitmentLevel::Finalized) => Ordering::Less,
//!     }
//! }
//!
//! impl DragonsmouthBlockMachine {
//!     fn handle_block_entry(&mut self, entry: SubscribeUpdateEntry) {
//!         let entry_info: EntryInfo = entry.into();
//!         self.sm.process_event(entry_info.into());
//!     }
//!
//!     fn handle_slot_update(&mut self, slot_update: &SubscribeUpdateSlot) {
//!         let slot_status = slot_update.status();
//!         const LIFE_CYCLE_STATUS: [SlotStatus; 4] = [
//!             SlotStatus::SlotFirstShredReceived,
//!             SlotStatus::SlotCompleted,
//!             SlotStatus::SlotCreatedBank,
//!             SlotStatus::SlotDead,
//!         ];
//!
//!         if LIFE_CYCLE_STATUS.contains(&slot_status) {
//!             let lifecycle_update = SlotLifecycleUpdate {
//!                 slot: slot_update.slot,
//!                 parent_slot: slot_update.parent,
//!                 stage: match slot_status {
//!                     SlotStatus::SlotFirstShredReceived => SlotLifecycle::FirstShredReceived,
//!                     SlotStatus::SlotCompleted => SlotLifecycle::Completed,
//!                     SlotStatus::SlotCreatedBank => SlotLifecycle::CreatedBank,
//!                     SlotStatus::SlotDead => SlotLifecycle::Dead,
//!                     _ => unreachable!(),
//!                 },
//!             };
//!             self.sm.process_event(lifecycle_update.into());
//!         } else {
//!             let commitment_level_update = SlotCommitmentStatusUpdate {
//!                 parent_slot: slot_update.parent,
//!                 slot: slot_update.slot,
//!                 commitment: match slot_status {
//!                     SlotStatus::SlotProcessed => CommitmentLevel::Processed,
//!                     SlotStatus::SlotConfirmed => CommitmentLevel::Confirmed,
//!                     SlotStatus::SlotFinalized => CommitmentLevel::Finalized,
//!                     _ => unreachable!(),
//!                 },
//!             };
//!
//!             self.sm.process_event(commitment_level_update.into());
//!         }
//!     }
//!
//!     fn handle_block_meta(&mut self, block_meta: SubscribeUpdateBlockMeta) {
//!         let bh = bs58::decode(block_meta.blockhash)
//!             .into_vec()
//!             .expect("blockhash format");
//!         let block_summary = BlockSummary {
//!             slot: block_meta.slot,
//!             entry_count: block_meta.entries_count,
//!             executed_transaction_count: block_meta.executed_transaction_count,
//!             blockhash: Hash::new_from_array(bh.try_into().expect("blockhash length")),
//!         };
//!         self.sm.process_event(block_summary.into());
//!         // Currently not used in block reconstruction
//!     }
//!     
//!     ///
//!     /// Main Logic to handle incoming geyser events and feed the block state machine
//!     ///
//!     fn handle_new_geyser_event(&mut self, event: SubscribeUpdate) {
//!         let SubscribeUpdate {
//!             filters,
//!             created_at,
//!             update_oneof,
//!         } = event;
//!         let Some(update_oneof) = update_oneof else {
//!             return;
//!         };
//!         match update_oneof {
//!             UpdateOneof::Account(acc) => {
//!                 let slot = acc.slot;
//!                 let subscribe_update = SubscribeUpdate {
//!                     filters,
//!                     created_at,
//!                     update_oneof: Some(UpdateOneof::Account(acc)),
//!                 };
//!                 self.block_storage.insert_block_data(slot, subscribe_update);
//!             }
//!             UpdateOneof::Slot(subscribe_update_slot) => {
//!                 self.handle_slot_update(&subscribe_update_slot);
//!             }
//!             UpdateOneof::Transaction(tx) => {
//!                 let slot = tx.slot;
//!                 let subscribe_update = SubscribeUpdate {
//!                     filters,
//!                     created_at,
//!                     update_oneof: Some(UpdateOneof::Transaction(tx)),
//!                 };
//!                 self.block_storage.insert_block_data(slot, subscribe_update);
//!             }
//!             UpdateOneof::BlockMeta(subscribe_update_block_meta) => {
//!                 self.handle_block_meta(subscribe_update_block_meta);
//!             }
//!             UpdateOneof::Entry(subscribe_update_entry) => {
//!                 let slot = subscribe_update_entry.slot;
//!                 self.handle_block_entry(subscribe_update_entry.clone());
//!                 if filters.iter().any(|f| f.as_str() != RESERVED_FILTER_NAME) {
//!                     let subscribe_update = SubscribeUpdate {
//!                         filters,
//!                         created_at,
//!                         update_oneof: Some(UpdateOneof::Entry(subscribe_update_entry)),
//!                     };
//!                     self.block_storage.insert_block_data(slot, subscribe_update);
//!                 }
//!             }
//!             _ => {
//!                 tracing::trace!("Unsupported update type received: {:?}", update_oneof);
//!             }
//!         }
//!     }
//!     
//!     ///
//!     /// Handle a block machine output event from the block state machine
//!     /// This function will handle the event and produce BlockMachineOutput events
//!     ///
//!     fn handle_blockstore_output<Ext>(&mut self, ev: BlockStateMachineOuput, out: &mut Ext)
//!     where
//!         Ext: Extend<BlockMachineOutput>,
//!     {
//!         match ev {
//!             BlockStateMachineOuput::FrozenBlock(info) => {
//!                 self.block_storage.mark_block_as_frozen(info.slot);
//!             }
//!             BlockStateMachineOuput::SlotStatus(st) => {
//!                 let ord = compare_commitment(st.commitment, self.minimum_commitment_level);
//!                 if ord == Ordering::Greater || ord == Ordering::Equal {
//!                     let block = self.block_storage.remove_slot(st.slot);
//!                     if let Some(block_replay) = block {
//!                         out.extend([block_replay.into()]);
//!                     } else {
//!                         tracing::trace!("No block replay found for slot {}", st.slot);
//!                     }
//!                     let commitment_update = SlotCommitmentStatusUpdate {
//!                         slot: st.slot,
//!                         parent_slot: st.parent_slot,
//!                         commitment: st.commitment,
//!                     };
//!                     out.extend([commitment_update.into()]);
//!                 }
//!             }
//!             BlockStateMachineOuput::ForksDetected(slot) => {
//!                 out.extend([BlockMachineOutput::ForkDetected(slot)]);
//!             }
//!             BlockStateMachineOuput::DeadSlotDetected(info) => {
//!                 out.extend([BlockMachineOutput::DeadBlockDetect(info)]);
//!             }
//!         }
//!     }
//!
//!     fn drain_unprocess_bm_output<Ext>(&mut self, out: &mut Ext)
//!     where
//!         Ext: Extend<BlockMachineOutput>,
//!     {
//!         while let Some(ev) = self.sm.pop_next_unprocess_blockstore_update() {
//!             self.handle_blockstore_output(ev, out);
//!         }
//!     }
//! }
//! ```
//!
//! # Feature flags
//! - `dragonsmouth`: Enables the dragonsmouth subscription of geyser events as the blockmachine input source.
//!
//!
#[cfg(feature = "dragonsmouth")]
pub mod dragonsmouth;
pub mod forks;
pub mod state_machine;
#[cfg(test)]
pub mod testkit;
