//! A mock Geyser stream for exercising this crate's Dragonsmouth integration without a live
//! validator or gRPC connection.
//!
//! [`SimulationBuilder`] programs a sequence of wire-shaped [`SubscribeUpdate`] events -- slot
//! lifecycle transitions, sysvar/account writes, entries, block metadata, block footers -- and [`build`] turns
//! it into a [`SimulatedGeyserStream`], a plain [`Stream`] of `Result<SubscribeUpdate, Infallible>`
//! that can be fed directly to [`crate::stream::BlockStream`] (or anything else generic over a
//! `TryStream<Ok = SubscribeUpdate>`) in place of a real `GeyserStream`.
//!
//! This is the tool for exercising scenarios such as: a slot producing anywhere from 1 to several
//! entries before freezing, several competing banks for the same slot racing to be the one that
//! ends up `Confirmed`/`Finalized` (Alpenglow's Corollary 50 bounds a correct node to storing at
//! most 7 distinct blocks for a single slot -- [`SimulationBuilder::bank`] can be called that many
//! times with the same `slot` and different `bank_id`s to simulate the worst case), or a bank
//! going `Dead` after partially streaming.
//!
//! ```
//! use futures_util::StreamExt;
//! use solana_hash::Hash;
//! use yellowstone_block_machine::dragonsmouth::{
//!     block_accumulator::DragonsmouthBlockCumulator,
//!     simulation::{SimulatedBank, SimulationBuilder},
//! };
//! use yellowstone_block_machine::stream::BlockStream;
//! use solana_commitment_config::CommitmentLevel;
//!
//! # async fn run() {
//! let bank = SimulatedBank::new(10, 1000).with_entry_count(3);
//! let stream = SimulationBuilder::new()
//!     .bank(&bank)
//!     .confirmed(bank.slot, bank.bank_id)
//!     .build();
//!
//! let mut block_stream = BlockStream::<
//!     _,
//!     yellowstone_block_machine::yellowstone_grpc_proto::geyser::SubscribeUpdate,
//!     _,
//! >::new(
//!     stream,
//!     DragonsmouthBlockCumulator::default(),
//!     CommitmentLevel::Confirmed,
//! );
//! while block_stream.next().await.is_some() {}
//! # }
//! ```

///
/// A seedable RNG for [`SimulationBuilder::random_bank`] -- re-exported so callers don't need to
/// depend on `rand` themselves just to get one. The same seed always produces the same sequence
/// of simulated events.
///
pub use rand::rngs::StdRng;
use {
    crate::dragonsmouth::block_accumulator::{MUST_HAVE_SYSVAR_ACCOUNTS, SYSVAR_PROGRAM_ID},
    futures_util::Stream,
    rand::{Rng, RngCore, SeedableRng, seq::SliceRandom},
    solana_clock::{BankId, Slot},
    solana_commitment_config::CommitmentLevel,
    solana_hash::Hash,
    solana_pubkey::Pubkey,
    std::{
        collections::VecDeque,
        convert::Infallible,
        pin::Pin,
        task::{Context, Poll},
    },
    yellowstone_grpc_proto::geyser::{
        SlotStatus, SubscribeUpdate, SubscribeUpdateAccount, SubscribeUpdateAccountInfo,
        SubscribeUpdateBlockFooter, SubscribeUpdateBlockMeta, SubscribeUpdateEntry,
        SubscribeUpdateSlot, SubscribeUpdateTransaction, SubscribeUpdateTransactionInfo,
        SubscribeUpdateTransactionStatus, subscribe_update::UpdateOneof,
    },
};

pub fn seeded_rng(seed: u64) -> StdRng {
    StdRng::seed_from_u64(seed)
}

const fn wrap(oneof: UpdateOneof, filters: Vec<String>) -> SubscribeUpdate {
    SubscribeUpdate {
        filters,
        created_at: None,
        update_oneof: Some(oneof),
    }
}

fn slot_status_update(
    slot: Slot,
    parent: Option<Slot>,
    status: SlotStatus,
    bank_id: Option<BankId>,
    dead_error: Option<String>,
) -> SubscribeUpdate {
    wrap(
        UpdateOneof::Slot(SubscribeUpdateSlot {
            slot,
            parent,
            status: status as i32,
            dead_error,
            bank_id,
        }),
        vec![crate::dragonsmouth::RESERVED_FILTER_NAME.to_string()],
    )
}

///
/// An account write. `owner` decides whether `proto_adapter.rs` classifies it as a sysvar or
/// plain `BankData` (see finding 19 in `AUDIT.md`); `txn_signature`, when present, must be one of
/// the block's own transaction signatures to be wire-valid.
///
fn account_update(
    slot: Slot,
    bank_id: BankId,
    pubkey: Pubkey,
    owner: Pubkey,
    txn_signature: Option<[u8; 64]>,
) -> SubscribeUpdate {
    wrap(
        UpdateOneof::Account(SubscribeUpdateAccount {
            account: Some(SubscribeUpdateAccountInfo {
                pubkey: pubkey.to_bytes().to_vec(),
                lamports: 1,
                owner: owner.to_bytes().to_vec(),
                executable: false,
                rent_epoch: 0,
                data: vec![],
                write_version: 1,
                txn_signature: txn_signature.map(|sig| sig.to_vec()),
            }),
            slot,
            is_startup: false,
            bank_id: Some(bank_id),
        }),
        vec![crate::dragonsmouth::RESERVED_FILTER_NAME.to_string()],
    )
}

fn sysvar_account_update(slot: Slot, bank_id: BankId, pubkey: Pubkey) -> SubscribeUpdate {
    account_update(slot, bank_id, pubkey, SYSVAR_PROGRAM_ID, None)
}

fn entry_update(slot: Slot, index: u64, bank_id: BankId) -> SubscribeUpdate {
    entry_update_with_count(slot, index, bank_id, 0)
}

fn entry_update_with_count(
    slot: Slot,
    index: u64,
    bank_id: BankId,
    executed_transaction_count: u64,
) -> SubscribeUpdate {
    wrap(
        UpdateOneof::Entry(SubscribeUpdateEntry {
            slot,
            index,
            num_hashes: 0,
            hash: vec![0; solana_hash::HASH_BYTES],
            executed_transaction_count,
            starting_transaction_index: 0,
            bank_id,
        }),
        vec![crate::dragonsmouth::RESERVED_FILTER_NAME.to_string()],
    )
}

fn transaction_update(
    slot: Slot,
    bank_id: BankId,
    signature: [u8; 64],
    index: u64,
    is_vote: bool,
) -> SubscribeUpdate {
    wrap(
        UpdateOneof::Transaction(SubscribeUpdateTransaction {
            transaction: Some(SubscribeUpdateTransactionInfo {
                signature: signature.to_vec(),
                is_vote,
                transaction: None,
                meta: None,
                index,
            }),
            slot,
            bank_id,
        }),
        vec![crate::dragonsmouth::RESERVED_FILTER_NAME.to_string()],
    )
}

fn transaction_status_update(
    slot: Slot,
    bank_id: BankId,
    signature: [u8; 64],
    index: u64,
    is_vote: bool,
) -> SubscribeUpdate {
    wrap(
        UpdateOneof::TransactionStatus(SubscribeUpdateTransactionStatus {
            slot,
            signature: signature.to_vec(),
            is_vote,
            index,
            err: None,
            bank_id,
        }),
        vec![crate::dragonsmouth::RESERVED_FILTER_NAME.to_string()],
    )
}

#[allow(clippy::too_many_arguments)]
fn block_meta_update(
    slot: Slot,
    bank_id: BankId,
    parent_slot: Slot,
    parent_blockhash: Hash,
    blockhash: Hash,
    entries_count: u64,
    executed_transaction_count: u64,
    block_time: i64,
) -> SubscribeUpdate {
    wrap(
        UpdateOneof::BlockMeta(SubscribeUpdateBlockMeta {
            slot,
            blockhash: blockhash.to_string(),
            rewards: None,
            block_time: Some(
                yellowstone_grpc_proto::solana::storage::confirmed_block::UnixTimestamp {
                    timestamp: block_time,
                },
            ),
            block_height: None,
            parent_slot,
            parent_blockhash: parent_blockhash.to_string(),
            executed_transaction_count,
            entries_count,
            bank_id,
        }),
        vec![crate::dragonsmouth::RESERVED_FILTER_NAME.to_string()],
    )
}

///
/// A bank's Alpenglow block footer. `bank_hash` is derived from `blockhash` purely so each
/// simulated bank gets a distinct, recognizable value.
///
fn block_footer_update(slot: Slot, bank_id: BankId, bank_hash: Hash) -> SubscribeUpdate {
    wrap(
        UpdateOneof::BlockFooter(SubscribeUpdateBlockFooter {
            slot,
            bank_id,
            bank_hash: bank_hash.to_bytes().to_vec(),
            block_producer_time_nanos: 0,
            block_user_agent: vec![],
            block_final_cert: None,
            skip_reward_cert: None,
            notar_reward_cert: None,
        }),
        vec![crate::dragonsmouth::RESERVED_FILTER_NAME.to_string()],
    )
}

///
/// The wire-level `SlotStatus` progression a bank passes through on its way to `level` --
/// `Processed` always comes first (a real validator never emits `Confirmed`/`Finalized` for a
/// bank_id it hasn't already reported `Processed` for).
///
const fn commitment_progression(level: CommitmentLevel) -> &'static [SlotStatus] {
    match level {
        CommitmentLevel::Processed => &[SlotStatus::SlotProcessed],
        CommitmentLevel::Confirmed => &[SlotStatus::SlotProcessed, SlotStatus::SlotConfirmed],
        CommitmentLevel::Finalized => &[
            SlotStatus::SlotProcessed,
            SlotStatus::SlotConfirmed,
            SlotStatus::SlotFinalized,
        ],
    }
}

///
/// A single candidate block for a slot -- everything [`SimulationBuilder::bank`] needs to
/// generate that bank's full happy-path event sequence: `CreatedBank`, the four must-have sysvar
/// accounts (see [`MUST_HAVE_SYSVAR_ACCOUNTS`]), `entry_count` `Entry` events, the block footer,
/// and `BlockMeta`.
///
#[derive(Debug, Clone)]
pub struct SimulatedBank {
    pub slot: Slot,
    pub bank_id: BankId,
    pub parent_slot: Slot,
    pub parent_blockhash: Hash,
    pub blockhash: Hash,
    ///
    /// How many `Entry` events this bank produces before its `BlockMeta`. Solana blocks
    /// typically carry a handful of entries per slot; any value is accepted here.
    ///
    pub entry_count: u64,
    ///
    /// Unix timestamp reported in this bank's `BlockMeta`. `0` if unset.
    ///
    pub block_time: i64,
    ///
    /// If set, this bank's block footer is never delivered, as on a pre-Alpenglow cluster.
    ///
    pub skip_footer: bool,
}

impl SimulatedBank {
    pub fn new(slot: Slot, bank_id: BankId) -> Self {
        Self {
            slot,
            bank_id,
            parent_slot: slot.saturating_sub(1),
            parent_blockhash: Hash::default(),
            blockhash: Hash::new_unique(),
            entry_count: 1,
            block_time: 0,
            skip_footer: false,
        }
    }

    pub const fn with_parent(mut self, parent_slot: Slot, parent_blockhash: Hash) -> Self {
        self.parent_slot = parent_slot;
        self.parent_blockhash = parent_blockhash;
        self
    }

    pub const fn with_blockhash(mut self, blockhash: Hash) -> Self {
        self.blockhash = blockhash;
        self
    }

    pub const fn with_entry_count(mut self, entry_count: u64) -> Self {
        self.entry_count = entry_count;
        self
    }

    pub const fn with_block_time(mut self, block_time: i64) -> Self {
        self.block_time = block_time;
        self
    }

    pub const fn with_skip_footer(mut self) -> Self {
        self.skip_footer = true;
        self
    }
}

///
/// A randomized candidate block for [`SimulationBuilder::random_bank`]. Unlike [`SimulatedBank`],
/// this carries real transactions and extra account writes, and its `commitment` field says
/// exactly which `SlotStatus` progression (if any) this specific bank should reach -- so a
/// multi-bank simulation can program "bank A reaches `Finalized`, bank B is left an unresolved
/// sibling, bank C goes `Dead`" explicitly, one [`RandomBlockPlan`] per bank.
///
#[derive(Debug, Clone)]
pub struct RandomBlockPlan {
    pub slot: Slot,
    pub bank_id: BankId,
    pub parent_slot: Slot,
    pub parent_blockhash: Hash,
    pub blockhash: Hash,
    pub transaction_count: u64,
    ///
    /// Entries this block's transactions are split across. Their `executed_transaction_count`
    /// always sums to exactly `transaction_count`, however they're split.
    ///
    pub entry_count: u64,
    ///
    /// Extra non-sysvar account writes to sprinkle in, on top of the four must-have sysvars.
    /// Each one has an even chance of carrying a `txn_signature` referencing one of this block's
    /// own transactions (never a fabricated one).
    ///
    pub account_update_count: u64,
    ///
    /// The `SlotStatus` progression this bank should reach on the wire -- `None` leaves it an
    /// unresolved candidate with no commitment update at all.
    ///
    pub commitment: Option<CommitmentLevel>,
    ///
    /// If set, this bank never gets a `BlockMeta` or a commitment progression (`commitment` is
    /// ignored) -- only its `CreatedBank`/sysvars/transactions/entries are delivered, followed by
    /// `Dead`, matching a leader that gave up partway through instead of one that finished
    /// replaying and was marked dead by some later, rarer reordering.
    ///
    pub dead: bool,
    pub block_time: i64,
    ///
    /// If set, this bank's block footer is never delivered, as on a pre-Alpenglow cluster.
    ///
    pub skip_footer: bool,
}

impl RandomBlockPlan {
    pub fn new(slot: Slot, bank_id: BankId) -> Self {
        Self {
            slot,
            bank_id,
            parent_slot: slot.saturating_sub(1),
            parent_blockhash: Hash::default(),
            blockhash: Hash::new_unique(),
            transaction_count: 0,
            entry_count: 1,
            account_update_count: 0,
            commitment: None,
            dead: false,
            block_time: 0,
            skip_footer: false,
        }
    }

    pub const fn with_parent(mut self, parent_slot: Slot, parent_blockhash: Hash) -> Self {
        self.parent_slot = parent_slot;
        self.parent_blockhash = parent_blockhash;
        self
    }

    pub const fn with_blockhash(mut self, blockhash: Hash) -> Self {
        self.blockhash = blockhash;
        self
    }

    pub const fn with_transaction_count(mut self, transaction_count: u64) -> Self {
        self.transaction_count = transaction_count;
        self
    }

    pub const fn with_entry_count(mut self, entry_count: u64) -> Self {
        self.entry_count = entry_count;
        self
    }

    pub const fn with_account_update_count(mut self, account_update_count: u64) -> Self {
        self.account_update_count = account_update_count;
        self
    }

    pub const fn with_commitment(mut self, commitment: CommitmentLevel) -> Self {
        self.commitment = Some(commitment);
        self
    }

    pub const fn with_dead(mut self) -> Self {
        self.dead = true;
        self
    }

    pub const fn with_block_time(mut self, block_time: i64) -> Self {
        self.block_time = block_time;
        self
    }

    pub const fn with_skip_footer(mut self) -> Self {
        self.skip_footer = true;
        self
    }
}

///
/// Programs a sequence of [`SubscribeUpdate`] events, in the order they'll be replayed by the
/// [`SimulatedGeyserStream`] built from it.
///
#[derive(Debug, Default)]
pub struct SimulationBuilder {
    events: VecDeque<SubscribeUpdate>,
}

impl SimulationBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    ///
    /// Appends `bank`'s full happy-path event sequence: `CreatedBank`, the must-have sysvar
    /// accounts, `bank.entry_count` `Entry` events, the block footer (unless
    /// `bank.skip_footer`), then `BlockMeta`. Call this once per
    /// candidate block -- e.g. up to 7 times with the same `slot` and different `bank_id`s to
    /// simulate the maximum number of distinct blocks Alpenglow allows a correct node to hold
    /// for one slot -- and follow up with [`Self::confirmed`]/[`Self::finalized`]/[`Self::dead`]
    /// to resolve which one wins.
    ///
    pub fn bank(mut self, bank: &SimulatedBank) -> Self {
        self.events.push_back(slot_status_update(
            bank.slot,
            None,
            SlotStatus::SlotCreatedBank,
            Some(bank.bank_id),
            None,
        ));
        for &sysvar in &MUST_HAVE_SYSVAR_ACCOUNTS {
            self.events
                .push_back(sysvar_account_update(bank.slot, bank.bank_id, sysvar));
        }
        for index in 0..bank.entry_count {
            self.events
                .push_back(entry_update(bank.slot, index, bank.bank_id));
        }
        if !bank.skip_footer {
            self.events
                .push_back(block_footer_update(bank.slot, bank.bank_id, bank.blockhash));
        }
        self.events.push_back(block_meta_update(
            bank.slot,
            bank.bank_id,
            bank.parent_slot,
            bank.parent_blockhash,
            bank.blockhash,
            bank.entry_count,
            bank.entry_count,
            bank.block_time,
        ));
        self
    }

    ///
    /// Appends a randomized bank generated from `plan`. The same `rng` state (so, ultimately, the
    /// same seed -- see [`seeded_rng`]) always produces the same sequence of events. It follows
    /// the backend's per-bank ordering guarantee:
    ///
    /// - the bank's content -- `CreatedBank`, the four must-have sysvar accounts, `Entry` events,
    ///   transactions, transaction statuses and extra account writes -- arrives first, in a
    ///   random order;
    /// - then the block footer (unless `plan.skip_footer`), then `BlockMeta`, then the bank's
    ///   `commitment` progression, always in that order.
    ///
    /// Within the content:
    ///
    /// - `plan.entry_count` `Entry` events whose `executed_transaction_count` sum to exactly
    ///   `plan.transaction_count`, however randomly split across them;
    /// - every extra account write that carries a `txn_signature` references one of this block's
    ///   own randomly generated transaction signatures, never a fabricated one.
    ///
    /// A `plan.dead` bank gets only a prefix of its content, then `Dead`.
    ///
    pub fn random_bank(mut self, rng: &mut impl RngCore, plan: &RandomBlockPlan) -> Self {
        let entry_count = plan.entry_count.max(1);

        let mut body: Vec<SubscribeUpdate> = MUST_HAVE_SYSVAR_ACCOUNTS
            .iter()
            .map(|&sysvar| sysvar_account_update(plan.slot, plan.bank_id, sysvar))
            .collect();
        body.push(slot_status_update(
            plan.slot,
            None,
            SlotStatus::SlotCreatedBank,
            Some(plan.bank_id),
            None,
        ));

        let signatures: Vec<[u8; 64]> = (0..plan.transaction_count)
            .map(|_| {
                let mut bytes = [0u8; 64];
                rng.fill_bytes(&mut bytes);
                bytes
            })
            .collect();

        // Randomly split the transactions across entries: whatever the split,
        // `executed_transaction_count` sums to exactly `plan.transaction_count`.
        let mut per_entry = vec![0u64; entry_count as usize];
        for _ in 0..plan.transaction_count {
            let slot_index = rng.random_range(0..per_entry.len());
            per_entry[slot_index] += 1;
        }
        for (index, count) in per_entry.into_iter().enumerate() {
            body.push(entry_update_with_count(
                plan.slot,
                index as u64,
                plan.bank_id,
                count,
            ));
        }

        for (index, &signature) in signatures.iter().enumerate() {
            let is_vote = rng.random_bool(0.1);
            let index = index as u64;
            body.push(transaction_update(
                plan.slot,
                plan.bank_id,
                signature,
                index,
                is_vote,
            ));
            body.push(transaction_status_update(
                plan.slot,
                plan.bank_id,
                signature,
                index,
                is_vote,
            ));
        }
        for _ in 0..plan.account_update_count {
            let mut pubkey_bytes = [0u8; 32];
            rng.fill_bytes(&mut pubkey_bytes);
            let mut owner_bytes = [0u8; 32];
            rng.fill_bytes(&mut owner_bytes);
            let txn_signature = (!signatures.is_empty() && rng.random_bool(0.5))
                .then(|| signatures[rng.random_range(0..signatures.len())]);
            body.push(account_update(
                plan.slot,
                plan.bank_id,
                Pubkey::from(pubkey_bytes),
                Pubkey::from(owner_bytes),
                txn_signature,
            ));
        }
        body.shuffle(rng);

        if plan.dead {
            // `Dead` means the leader gave up partway through: only part of the content arrives,
            // then `Dead`, and no footer, `BlockMeta` or commitment progression. (A bank marked
            // dead after its `BlockMeta` is covered by building the events by hand; see
            // `dead_bank_never_freezes_even_if_its_block_meta_arrives_late`.)
            let delivered = rng.random_range(0..=body.len());
            body.truncate(delivered);
            self.events.extend(body);
            self.events.push_back(slot_status_update(
                plan.slot,
                None,
                SlotStatus::SlotDead,
                Some(plan.bank_id),
                Some("simulated dead bank".to_string()),
            ));
            return self;
        }

        self.events.extend(body);
        if !plan.skip_footer {
            self.events
                .push_back(block_footer_update(plan.slot, plan.bank_id, plan.blockhash));
        }
        self.events.push_back(block_meta_update(
            plan.slot,
            plan.bank_id,
            plan.parent_slot,
            plan.parent_blockhash,
            plan.blockhash,
            entry_count,
            plan.transaction_count,
            plan.block_time,
        ));
        if let Some(level) = plan.commitment {
            for &status in commitment_progression(level) {
                self.events.push_back(slot_status_update(
                    plan.slot,
                    None,
                    status,
                    Some(plan.bank_id),
                    None,
                ));
            }
        }

        self
    }

    ///
    /// Appends an arbitrary already-built event, for scenarios the convenience methods here
    /// don't cover (a custom filter set, a malformed message, etc).
    ///
    pub fn push(mut self, event: SubscribeUpdate) -> Self {
        self.events.push_back(event);
        self
    }

    ///
    /// Appends a batch of already-built events, e.g. one returned by
    /// [`SimulationBuilder::into_events`] or [`interleave`].
    ///
    pub fn extend(mut self, events: impl IntoIterator<Item = SubscribeUpdate>) -> Self {
        self.events.extend(events);
        self
    }

    ///
    /// Appends a block footer for `bank_id`, e.g. to deliver one late for a bank built with
    /// `with_skip_footer`.
    ///
    pub fn block_footer(mut self, slot: Slot, bank_id: BankId, bank_hash: Hash) -> Self {
        self.events
            .push_back(block_footer_update(slot, bank_id, bank_hash));
        self
    }

    pub fn first_shred_received(mut self, slot: Slot, parent: Option<Slot>) -> Self {
        self.events.push_back(slot_status_update(
            slot,
            parent,
            SlotStatus::SlotFirstShredReceived,
            None,
            None,
        ));
        self
    }

    pub fn completed(mut self, slot: Slot, parent: Option<Slot>) -> Self {
        self.events.push_back(slot_status_update(
            slot,
            parent,
            SlotStatus::SlotCompleted,
            None,
            None,
        ));
        self
    }

    pub fn processed(mut self, slot: Slot, bank_id: BankId) -> Self {
        self.events.push_back(slot_status_update(
            slot,
            None,
            SlotStatus::SlotProcessed,
            Some(bank_id),
            None,
        ));
        self
    }

    pub fn confirmed(mut self, slot: Slot, bank_id: BankId) -> Self {
        self.events.push_back(slot_status_update(
            slot,
            None,
            SlotStatus::SlotConfirmed,
            Some(bank_id),
            None,
        ));
        self
    }

    pub fn finalized(mut self, slot: Slot, bank_id: BankId) -> Self {
        self.events.push_back(slot_status_update(
            slot,
            None,
            SlotStatus::SlotFinalized,
            Some(bank_id),
            None,
        ));
        self
    }

    ///
    /// Marks `bank_id` dead. `bank_id` is `None` when the bank was never created (the leader
    /// never got that far) -- matching how a real Geyser stream reports it.
    ///
    pub fn dead(mut self, slot: Slot, bank_id: Option<BankId>, error: impl Into<String>) -> Self {
        self.events.push_back(slot_status_update(
            slot,
            None,
            SlotStatus::SlotDead,
            bank_id,
            Some(error.into()),
        ));
        self
    }

    ///
    /// Consumes the builder, returning the programmed events without wrapping them in a
    /// [`Stream`] -- useful for [`interleave`]ing several banks' sequences instead of replaying
    /// them back-to-back.
    ///
    pub fn into_events(self) -> Vec<SubscribeUpdate> {
        self.events.into_iter().collect()
    }

    pub fn build(self) -> SimulatedGeyserStream {
        SimulatedGeyserStream {
            events: self.events,
        }
    }
}

///
/// Round-robins several already-built event sequences into one, so independent banks' events
/// appear to arrive concurrently rather than one bank's whole sequence at a time -- e.g. to
/// simulate several competing blocks for the same slot racing each other on the wire.
///
pub fn interleave(sequences: Vec<Vec<SubscribeUpdate>>) -> Vec<SubscribeUpdate> {
    let mut queues: Vec<VecDeque<SubscribeUpdate>> =
        sequences.into_iter().map(VecDeque::from).collect();
    let mut out = Vec::new();
    loop {
        let mut progressed = false;
        for queue in &mut queues {
            if let Some(event) = queue.pop_front() {
                out.push(event);
                progressed = true;
            }
        }
        if !progressed {
            break;
        }
    }
    out
}

///
/// A pre-programmed [`Stream`] of `SubscribeUpdate`, built by [`SimulationBuilder::build`].
/// Never errors -- its `Item` is `Result<SubscribeUpdate, Infallible>` purely so it satisfies the
/// same `TryStream<Ok = SubscribeUpdate>` bound a real `GeyserStream` does.
///
#[derive(Debug, Default)]
pub struct SimulatedGeyserStream {
    events: VecDeque<SubscribeUpdate>,
}

impl SimulatedGeyserStream {
    pub fn new(events: Vec<SubscribeUpdate>) -> Self {
        Self {
            events: events.into(),
        }
    }
}

impl Stream for SimulatedGeyserStream {
    type Item = Result<SubscribeUpdate, Infallible>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(self.events.pop_front().map(Ok))
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            dragonsmouth::block_accumulator::DragonsmouthBlockCumulator,
            event::GeyserEventAdapter,
            stream::{BlockMachineOutput, BlockStream},
            wrapper::BlockMachineConfig,
        },
        std::pin::Pin,
    };

    // The simulated source is always `Poll::Ready`, so `BlockStream` never actually needs to
    // suspend -- driving it with a `noop_waker` (same pattern as `stream.rs`'s own tests) avoids
    // depending on a real async runtime just for these tests.
    fn drain<S, A, Acc>(
        stream: &mut BlockStream<S, A, Acc>,
    ) -> Vec<BlockMachineOutput<Acc::EventStore>>
    where
        S: futures_util::TryStream<Ok = A::EventT> + Unpin,
        A: crate::event::GeyserEventAdapter,
        A::EventT: Unpin,
        Acc: crate::stream::BlockAccumulator<EventT = A::EventT> + Unpin,
    {
        let waker = futures_util::task::noop_waker();
        let mut cx = std::task::Context::from_waker(&waker);
        let mut outputs = Vec::new();
        loop {
            match Pin::new(&mut *stream).poll_next(&mut cx) {
                std::task::Poll::Ready(Some(Ok(output))) => outputs.push(output),
                std::task::Poll::Ready(Some(Err(_))) => unreachable!("simulation never errors"),
                std::task::Poll::Ready(None) => break,
                std::task::Poll::Pending => break,
            }
        }
        outputs
    }

    #[test]
    fn bank_generates_expected_event_shape() {
        let bank = SimulatedBank::new(10, 1000).with_entry_count(3);
        let events = SimulationBuilder::new().bank(&bank).into_events();

        // CreatedBank + 4 must-have sysvars + 3 entries + BlockFooter + BlockMeta.
        assert_eq!(events.len(), 1 + 4 + 3 + 1 + 1);
        for event in &events {
            let ev_info = SubscribeUpdate::extract_geyser_ev_info(event)
                .expect("every simulated event must be recognized by the crate's own adapter");
            assert_eq!(ev_info.bank_id(), Some(1000));
        }
    }

    #[test]
    fn seven_competing_banks_can_be_simulated_for_one_slot() {
        let slot = 77;
        let banks: Vec<SimulatedBank> = (0..7)
            .map(|i| SimulatedBank::new(slot, 7_000 + i).with_entry_count(1))
            .collect();

        let mut builder = SimulationBuilder::new();
        for bank in &banks {
            builder = builder.bank(bank);
        }
        // Only the last bank ends up confirmed; the rest are left unresolved siblings.
        let winner = banks.last().unwrap();
        let stream = builder.confirmed(slot, winner.bank_id).build();

        let mut block_stream = BlockStream::<_, SubscribeUpdate, _>::new(
            stream,
            DragonsmouthBlockCumulator::default(),
            CommitmentLevel::Confirmed,
        );

        let frozen_bank_ids: Vec<_> = drain(&mut block_stream)
            .into_iter()
            .filter_map(|output| match output {
                BlockMachineOutput::FrozenBlock(block) => Some(block.bank_id),
                _ => None,
            })
            .collect();
        assert!(frozen_bank_ids.contains(&winner.bank_id));
    }

    #[test]
    fn dead_bank_never_freezes_even_if_its_block_meta_arrives_late() {
        let slot = 88;
        let bank = SimulatedBank::new(slot, 8800).with_entry_count(1);

        // Everything but the trailing `BlockMeta` -- `Dead` is delivered in between, so the
        // straggler `BlockMeta` arrives for a bank the machine has already discarded.
        let mut events = SimulationBuilder::new().bank(&bank).into_events();
        let block_meta = events.pop().expect("bank() always ends with BlockMeta");

        let stream = SimulationBuilder::new()
            .extend(events)
            .dead(slot, Some(bank.bank_id), "erroneous transaction")
            .push(block_meta)
            .build();

        let mut block_stream = BlockStream::<_, SubscribeUpdate, _>::new(
            stream,
            DragonsmouthBlockCumulator::default(),
            CommitmentLevel::Processed,
        );

        let dead_bank_id = bank.bank_id;
        for output in drain(&mut block_stream) {
            if let BlockMachineOutput::FrozenBlock(block) = output {
                assert_ne!(block.bank_id, dead_bank_id, "a dead bank must never freeze");
            }
        }
    }

    fn is_block_meta(event: &SubscribeUpdate) -> bool {
        matches!(event.update_oneof, Some(UpdateOneof::BlockMeta(_)))
    }

    fn slot_status(event: &SubscribeUpdate) -> Option<i32> {
        match &event.update_oneof {
            Some(UpdateOneof::Slot(slot)) => Some(slot.status),
            _ => None,
        }
    }

    #[test]
    fn random_bank_respects_signature_entry_and_ordering_invariants() {
        let mut rng = seeded_rng(42);
        let plan = RandomBlockPlan::new(50, 5000)
            .with_transaction_count(9)
            .with_entry_count(3)
            .with_account_update_count(6);
        let events = SimulationBuilder::new()
            .random_bank(&mut rng, &plan)
            .into_events();

        let signatures: std::collections::HashSet<Vec<u8>> = events
            .iter()
            .filter_map(|event| match &event.update_oneof {
                Some(UpdateOneof::Transaction(tx)) => {
                    Some(tx.transaction.as_ref().unwrap().signature.clone())
                }
                _ => None,
            })
            .collect();
        assert_eq!(signatures.len(), plan.transaction_count as usize);

        let mut executed_sum = 0u64;
        let mut created_bank_index = None;
        let mut sysvar_indices = Vec::new();
        for (index, event) in events.iter().enumerate() {
            match &event.update_oneof {
                Some(UpdateOneof::Entry(entry)) => executed_sum += entry.executed_transaction_count,
                Some(UpdateOneof::Slot(slot))
                    if slot.status == SlotStatus::SlotCreatedBank as i32 =>
                {
                    created_bank_index = Some(index);
                }
                Some(UpdateOneof::Account(account)) => {
                    let info = account.account.as_ref().unwrap();
                    if info.owner == SYSVAR_PROGRAM_ID.to_bytes() {
                        sysvar_indices.push(index);
                    }
                    if let Some(sig) = &info.txn_signature {
                        assert!(
                            signatures.contains(sig),
                            "an account update's txn_signature must match a real transaction \
                             signature from this block"
                        );
                    }
                }
                _ => {}
            }
        }

        assert_eq!(
            executed_sum, plan.transaction_count,
            "the sum of executed_transaction_count across all entries must equal the block's \
             total transaction count"
        );

        assert!(
            created_bank_index.is_some(),
            "CreatedBank must appear in the generated sequence"
        );
        assert_eq!(sysvar_indices.len(), MUST_HAVE_SYSVAR_ACCOUNTS.len());
    }

    #[test]
    fn random_bank_is_deterministic_given_a_seed() {
        let plan = RandomBlockPlan::new(60, 6000)
            .with_transaction_count(5)
            .with_entry_count(2)
            .with_account_update_count(3)
            .with_commitment(CommitmentLevel::Finalized);

        let events_a = SimulationBuilder::new()
            .random_bank(&mut seeded_rng(7), &plan)
            .into_events();
        let events_b = SimulationBuilder::new()
            .random_bank(&mut seeded_rng(7), &plan)
            .into_events();

        assert_eq!(
            events_a, events_b,
            "the same seed must replay the same sequence"
        );
    }

    ///
    /// The backend's per-bank ordering: content in any order, then the footer, then `BlockMeta`,
    /// then the commitment progression.
    ///
    #[test]
    fn random_bank_follows_the_backend_order() {
        let mut saw_created_bank_after_an_entry = false;

        for seed in 0..40u64 {
            let plan = RandomBlockPlan::new(70, 7_000 + seed)
                .with_transaction_count(3)
                .with_entry_count(2)
                .with_account_update_count(2)
                .with_commitment(CommitmentLevel::Finalized);
            let events = SimulationBuilder::new()
                .random_bank(&mut seeded_rng(seed), &plan)
                .into_events();

            let position = |status: SlotStatus| {
                events
                    .iter()
                    .position(|ev| slot_status(ev) == Some(status as i32))
                    .unwrap()
            };
            let footer_index = events.iter().position(is_block_footer).unwrap();
            let block_meta_index = events.iter().position(is_block_meta).unwrap();
            let processed_index = position(SlotStatus::SlotProcessed);
            let confirmed_index = position(SlotStatus::SlotConfirmed);
            let finalized_index = position(SlotStatus::SlotFinalized);
            assert_eq!(
                (
                    block_meta_index,
                    processed_index,
                    confirmed_index,
                    finalized_index
                ),
                (
                    footer_index + 1,
                    footer_index + 2,
                    footer_index + 3,
                    footer_index + 4
                ),
                "seed {seed}: footer, BlockMeta, then the commitments, back to back"
            );
            assert_eq!(
                finalized_index + 1,
                events.len(),
                "seed {seed}: nothing after the commitments"
            );

            let created_bank_index = position(SlotStatus::SlotCreatedBank);
            let first_entry_index = events
                .iter()
                .position(|ev| matches!(ev.update_oneof, Some(UpdateOneof::Entry(_))))
                .unwrap();
            saw_created_bank_after_an_entry |= created_bank_index > first_entry_index;
        }

        assert!(
            saw_created_bank_after_an_entry,
            "across enough seeds, content (CreatedBank included) must arrive in a random order"
        );
    }

    #[test]
    fn random_bank_reaches_the_programmed_commitment_level() {
        let slot = 90;
        let winner = RandomBlockPlan::new(slot, 9001)
            .with_transaction_count(4)
            .with_entry_count(2)
            .with_account_update_count(2)
            .with_commitment(CommitmentLevel::Finalized);
        // No `commitment` set: stays an unresolved candidate sibling.
        let sibling = RandomBlockPlan::new(slot, 9002)
            .with_transaction_count(1)
            .with_entry_count(1);

        let mut rng = seeded_rng(123);
        let stream = SimulationBuilder::new()
            .random_bank(&mut rng, &winner)
            .random_bank(&mut rng, &sibling)
            .build();

        let mut block_stream = BlockStream::<_, SubscribeUpdate, _>::new(
            stream,
            DragonsmouthBlockCumulator::default(),
            CommitmentLevel::Processed,
        );

        let mut reached: std::collections::HashMap<BankId, Vec<CommitmentLevel>> =
            std::collections::HashMap::new();
        for output in drain(&mut block_stream) {
            if let BlockMachineOutput::SlotCommitmentUpdate(update) = output {
                reached
                    .entry(update.bank_id)
                    .or_default()
                    .push(update.commitment);
            }
        }

        assert!(
            reached
                .get(&winner.bank_id)
                .is_some_and(|levels| levels.contains(&CommitmentLevel::Finalized)),
            "the bank programmed with `with_commitment(Finalized)` must reach Finalized"
        );
        assert!(
            !reached.contains_key(&sibling.bank_id),
            "a sibling given no commitment must never itself be reported as resolved"
        );
    }

    fn is_block_footer(event: &SubscribeUpdate) -> bool {
        matches!(event.update_oneof, Some(UpdateOneof::BlockFooter(_)))
    }

    #[test]
    fn skip_footer_and_dead_banks_emit_no_footer() {
        let bank = SimulatedBank::new(81, 8100).with_skip_footer();
        let events = SimulationBuilder::new().bank(&bank).into_events();
        assert!(!events.iter().any(is_block_footer));

        let mut rng = seeded_rng(1);
        for plan in [
            RandomBlockPlan::new(82, 8200).with_skip_footer(),
            RandomBlockPlan::new(83, 8300).with_dead(),
        ] {
            let events = SimulationBuilder::new()
                .random_bank(&mut rng, &plan)
                .into_events();
            assert!(!events.iter().any(is_block_footer));
        }
    }

    ///
    /// A bank whose footer never arrives never freezes by default (footers required), and is
    /// counted as awaiting its footer; with `require_block_footer` off it is delivered.
    ///
    #[test]
    fn bank_without_footer_is_only_delivered_when_footers_are_not_required() {
        let bank = SimulatedBank::new(84, 8400).with_skip_footer();
        let frozen = |require_block_footer: bool| {
            let stream = SimulationBuilder::new()
                .bank(&bank)
                .confirmed(bank.slot, bank.bank_id)
                .build();
            let mut block_stream = BlockStream::<_, SubscribeUpdate, _>::new_with_config(
                stream,
                DragonsmouthBlockCumulator::default(),
                CommitmentLevel::Confirmed,
                BlockMachineConfig {
                    require_block_footer,
                },
            );
            let outputs = drain(&mut block_stream);
            let awaiting = block_stream.banks_awaiting_footer();
            let frozen = outputs
                .iter()
                .any(|output| matches!(output, BlockMachineOutput::FrozenBlock(_)));
            (frozen, awaiting)
        };

        assert_eq!(frozen(true), (false, 1));
        assert_eq!(frozen(false), (true, 0));
    }

    ///
    /// A bank whose footer arrives after its slot's commitment updates is still delivered first,
    /// followed by every commitment update in order (the driver only freezes it once the footer
    /// arrives, and the state machine queues commitment updates until then).
    ///
    #[test]
    fn commitment_updates_wait_for_a_late_sealing_block() {
        let bank = SimulatedBank::new(85, 8500).with_skip_footer();
        let stream = SimulationBuilder::new()
            .bank(&bank)
            .processed(bank.slot, bank.bank_id)
            .confirmed(bank.slot, bank.bank_id)
            .block_footer(bank.slot, bank.bank_id, bank.blockhash)
            .build();
        let mut block_stream = BlockStream::<_, SubscribeUpdate, _>::new(
            stream,
            DragonsmouthBlockCumulator::default(),
            CommitmentLevel::Processed,
        );

        let order: Vec<Option<CommitmentLevel>> = drain(&mut block_stream)
            .into_iter()
            .filter_map(|output| match output {
                BlockMachineOutput::FrozenBlock(block) if block.bank_id == bank.bank_id => {
                    Some(None)
                }
                BlockMachineOutput::SlotCommitmentUpdate(update)
                    if update.bank_id == bank.bank_id =>
                {
                    Some(Some(update.commitment))
                }
                _ => None,
            })
            .collect();

        assert_eq!(
            order,
            vec![
                None,
                Some(CommitmentLevel::Processed),
                Some(CommitmentLevel::Confirmed)
            ],
            "the block first, then its commitment updates in arrival order"
        );
    }
}
