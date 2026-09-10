//! End-to-end scenarios built on `yellowstone_block_machine::dragonsmouth::simulation`.
//!
//! Unlike the crate's own unit tests (which drive `BlockStream` with a `noop_waker` since they
//! live inside the crate and want to stay runtime-agnostic), this crate is an external consumer:
//! it drives the stream the way a real integration would, with `futures_util::StreamExt` under a
//! `tokio` runtime.

use {
    futures_util::StreamExt,
    solana_clock::BankId,
    solana_commitment_config::CommitmentLevel,
    std::collections::HashMap,
    yellowstone_block_machine::{
        dragonsmouth::{
            block_accumulator::DragonsmouthBlockCumulator,
            simulation::{
                RandomBlockPlan, SimulatedBank, SimulationBuilder, interleave, seeded_rng,
            },
        },
        stream::{BlockMachineOutput, BlockStream},
        yellowstone_grpc_proto::geyser::SubscribeUpdate,
    },
};

type Stream = BlockStream<
    yellowstone_block_machine::dragonsmouth::simulation::SimulatedGeyserStream,
    SubscribeUpdate,
    DragonsmouthBlockCumulator,
>;

async fn collect(mut stream: Stream) -> Vec<BlockMachineOutput<<DragonsmouthBlockCumulator as yellowstone_block_machine::stream::BlockAccumulator>::EventStore>>{
    let mut outputs = Vec::new();
    while let Some(output) = stream.next().await {
        outputs.push(output.expect("simulation never produces a stream error"));
    }
    outputs
}

///
/// The bank_id a commitment update named as resolved -- unlike `FrozenBlock`, which any
/// data-complete candidate can produce independently of whether it ever wins its slot.
///
const fn resolved_bank_id(
    output: &BlockMachineOutput<impl yellowstone_block_machine::stream::BlockEventStore>,
) -> Option<BankId> {
    match output {
        BlockMachineOutput::SlotCommitmentUpdate(update) => Some(update.bank_id),
        _ => None,
    }
}

const fn frozen_bank_id(
    output: &BlockMachineOutput<impl yellowstone_block_machine::stream::BlockEventStore>,
) -> Option<BankId> {
    match output {
        BlockMachineOutput::FrozenBlock(block) => Some(block.bank_id),
        _ => None,
    }
}

///
/// The bank_id a `BankDiscarded` output named as having lost its slot to a sibling -- block-level,
/// unlike `ForkDetected`, which is about a slot diverging from the canonical chain.
///
const fn discarded_bank_id(
    output: &BlockMachineOutput<impl yellowstone_block_machine::stream::BlockEventStore>,
) -> Option<BankId> {
    match output {
        BlockMachineOutput::BankDiscarded(discarded) => Some(discarded.bank_id),
        _ => None,
    }
}

#[tokio::test]
async fn entry_counts_one_through_seven_all_freeze_correctly() {
    for entry_count in 1..=7u64 {
        let slot = 1_000 + entry_count;
        let bank_id = 100_000 + entry_count;
        let bank = SimulatedBank::new(slot, bank_id).with_entry_count(entry_count);

        let stream = SimulationBuilder::new()
            .bank(&bank)
            .confirmed(slot, bank_id)
            .build();

        let block_stream = BlockStream::<_, SubscribeUpdate, _>::new(
            stream,
            DragonsmouthBlockCumulator::default(),
            CommitmentLevel::Confirmed,
        );

        let outputs = collect(block_stream).await;
        let frozen = outputs
            .into_iter()
            .find_map(|output| match output {
                BlockMachineOutput::FrozenBlock(block) if block.bank_id == bank_id => Some(block),
                _ => None,
            })
            .unwrap_or_else(|| panic!("block with {entry_count} entries never froze"));

        assert_eq!(
            frozen.entry_count, entry_count,
            "a block with {entry_count} entries must report that count in its frozen summary"
        );
    }
}

#[tokio::test]
async fn seven_competing_banks_only_the_finalized_one_survives() {
    let slot = 5_000;
    let banks: Vec<SimulatedBank> = (0..7)
        .map(|i| SimulatedBank::new(slot, 500_000 + i).with_entry_count(1 + i))
        .collect();
    let winner = banks[3].bank_id;

    let mut builder = SimulationBuilder::new();
    for bank in &banks {
        // Every candidate genuinely gets replayed and reaches Processed on its own -- that's
        // true of any bank a validator creates, win or lose -- only Confirmed/Finalized ever
        // singles one out.
        builder = builder.bank(bank).processed(slot, bank.bank_id);
    }
    let stream = builder.finalized(slot, winner).build();

    let block_stream = BlockStream::<_, SubscribeUpdate, _>::new(
        stream,
        DragonsmouthBlockCumulator::default(),
        CommitmentLevel::Processed,
    );

    let outputs = collect(block_stream).await;
    let mut commitments: HashMap<BankId, Vec<CommitmentLevel>> = HashMap::new();
    for output in &outputs {
        if let BlockMachineOutput::SlotCommitmentUpdate(update) = output {
            commitments
                .entry(update.bank_id)
                .or_default()
                .push(update.commitment);
        }
    }

    for bank in &banks {
        assert!(
            commitments
                .get(&bank.bank_id)
                .is_some_and(|levels| levels.contains(&CommitmentLevel::Processed)),
            "bank {} is one of the 7 candidates for the slot and must be reported at least at \
             Processed",
            bank.bank_id
        );
    }

    let finalized_bank_ids: std::collections::HashSet<BankId> = commitments
        .iter()
        .filter(|(_, levels)| levels.contains(&CommitmentLevel::Finalized))
        .map(|(&bank_id, _)| bank_id)
        .collect();
    assert_eq!(
        finalized_bank_ids,
        std::collections::HashSet::from([winner]),
        "only the bank named by the Finalized commitment update may ever reach Finalized, out \
         of all 7 candidates for the slot"
    );
}

///
/// A fresh seed per test run (not per assertion) -- printed in every panic message below so a CI
/// failure is reproducible by pinning `seeded_rng` to the exact value that failed, without
/// needing a whole seed range to be re-run to hit it again.
///
fn random_seed() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is before the epoch")
        .as_nanos() as u64
}

#[tokio::test]
async fn random_block_reconstructs_regardless_of_block_meta_timing() {
    let seed = random_seed();
    let slot = 9_000;
    let plan = RandomBlockPlan::new(slot, 900_000)
        .with_transaction_count(6)
        .with_entry_count(3)
        .with_account_update_count(4)
        .with_commitment(CommitmentLevel::Finalized);

    let stream = SimulationBuilder::new()
        .random_bank(&mut seeded_rng(seed), &plan)
        .build();

    let block_stream = BlockStream::<_, SubscribeUpdate, _>::new(
        stream,
        DragonsmouthBlockCumulator::default(),
        CommitmentLevel::Finalized,
    );

    let outputs = collect(block_stream).await;

    let frozen = outputs
        .iter()
        .find_map(|output| match output {
            BlockMachineOutput::FrozenBlock(block) if block.bank_id == plan.bank_id => Some(block),
            _ => None,
        })
        .unwrap_or_else(|| panic!("seed {seed}: block never froze regardless of BlockMeta timing"));
    assert_eq!(
        frozen.entry_count, plan.entry_count,
        "seed {seed}: entry count mismatch"
    );

    let reached_finalized = outputs.iter().any(|output| {
        matches!(
            output,
            BlockMachineOutput::SlotCommitmentUpdate(update)
                if update.bank_id == plan.bank_id && update.commitment == CommitmentLevel::Finalized
        )
    });
    assert!(
        reached_finalized,
        "seed {seed}: block never reached Finalized"
    );
}

#[tokio::test]
async fn dead_bank_never_finalizes() {
    let slot = 12_000;
    let plan = RandomBlockPlan::new(slot, 1_200_000)
        .with_transaction_count(3)
        .with_entry_count(2)
        .with_dead();

    let stream = SimulationBuilder::new()
        .random_bank(&mut seeded_rng(99), &plan)
        .build();

    let block_stream = BlockStream::<_, SubscribeUpdate, _>::new(
        stream,
        DragonsmouthBlockCumulator::default(),
        CommitmentLevel::Processed,
    );

    let outputs = collect(block_stream).await;
    assert!(
        outputs
            .iter()
            .all(|output| frozen_bank_id(output) != Some(plan.bank_id)
                && resolved_bank_id(output) != Some(plan.bank_id)),
        "a bank marked dead must never freeze or reach any commitment level"
    );
}

#[tokio::test]
async fn interleaved_forks_still_resolve_the_confirmed_winner() {
    let slot = 15_000;
    let winner = SimulatedBank::new(slot, 1_500_001).with_entry_count(2);
    let loser = SimulatedBank::new(slot, 1_500_002).with_entry_count(4);

    let winner_events = SimulationBuilder::new().bank(&winner).into_events();
    let loser_events = SimulationBuilder::new().bank(&loser).into_events();

    // Race the two banks' events against each other instead of replaying one after the other,
    // then resolve the slot once both have had a chance to arrive.
    let stream = SimulationBuilder::new()
        .extend(interleave(vec![winner_events, loser_events]))
        .confirmed(slot, winner.bank_id)
        .build();

    let block_stream = BlockStream::<_, SubscribeUpdate, _>::new(
        stream,
        DragonsmouthBlockCumulator::default(),
        CommitmentLevel::Confirmed,
    );

    let outputs = collect(block_stream).await;
    let resolved: HashMap<BankId, CommitmentLevel> = outputs
        .into_iter()
        .filter_map(|output| match output {
            BlockMachineOutput::SlotCommitmentUpdate(update) => {
                Some((update.bank_id, update.commitment))
            }
            _ => None,
        })
        .collect();

    assert_eq!(
        resolved.get(&winner.bank_id),
        Some(&CommitmentLevel::Confirmed)
    );
    assert!(!resolved.contains_key(&loser.bank_id));
}

///
/// A 4-bank ancestry tree:
///
/// ```text
///        A
///       / \
///      B   D
///      |
///      C
/// ```
///
/// B and D are both children of A, at consecutive slots. C and D then both claim the *next*
/// slot after that -- but with different parents (C says its parent is B, D says its parent is
/// A) -- exactly the "two banks competing for the same slot, disagreeing about their parent
/// slot" case `state_machine.rs`'s own
/// `two_banks_for_same_slot_can_disagree_on_parent_and_only_the_resolved_bank_feeds_forks` covers
/// in isolation. This is the same scenario end-to-end: real ancestor banks (not just raw parent
/// slot numbers) reconstructed through the full wire simulation, with D (the distant-parent
/// claim, chained off A directly) the one that ends up Confirmed.
#[tokio::test]
async fn competing_banks_for_the_same_slot_can_have_different_parents() {
    let a = SimulatedBank::new(10, 10_001).with_entry_count(1);
    let b = SimulatedBank::new(11, 10_002)
        .with_entry_count(1)
        .with_parent(a.slot, a.blockhash);
    // C and D both claim slot 12 -- C as B's child, D as A's child directly (skipping B).
    let c = SimulatedBank::new(12, 10_003)
        .with_entry_count(1)
        .with_parent(b.slot, b.blockhash);
    let d = SimulatedBank::new(12, 10_004)
        .with_entry_count(1)
        .with_parent(a.slot, a.blockhash);

    let stream = SimulationBuilder::new()
        .bank(&a)
        .bank(&b)
        .bank(&c)
        .bank(&d)
        .processed(a.slot, a.bank_id)
        .processed(b.slot, b.bank_id)
        .processed(d.slot, d.bank_id)
        // D, not C, is the one that gets confirmed for slot 12.
        .confirmed(d.slot, d.bank_id)
        .build();

    let block_stream = BlockStream::<_, SubscribeUpdate, _>::new(
        stream,
        DragonsmouthBlockCumulator::default(),
        CommitmentLevel::Processed,
    );

    let outputs = collect(block_stream).await;
    let mut commitments: HashMap<BankId, Vec<CommitmentLevel>> = HashMap::new();
    let mut frozen_bank_ids: std::collections::HashSet<BankId> = std::collections::HashSet::new();
    let mut discarded_bank_ids: Vec<BankId> = Vec::new();
    for output in &outputs {
        if let BlockMachineOutput::SlotCommitmentUpdate(update) = output {
            commitments
                .entry(update.bank_id)
                .or_default()
                .push(update.commitment);
        }
        if let Some(bank_id) = frozen_bank_id(output) {
            frozen_bank_ids.insert(bank_id);
        }
        if let Some(bank_id) = discarded_bank_id(output) {
            discarded_bank_ids.push(bank_id);
        }
    }

    // A and B are each the sole candidate for their own slot, so they resolve independently of
    // the fork at slot 12 entirely.
    assert!(
        commitments
            .get(&a.bank_id)
            .is_some_and(|levels| levels.contains(&CommitmentLevel::Processed)),
        "A must resolve at Processed on its own"
    );
    assert!(
        commitments
            .get(&b.bank_id)
            .is_some_and(|levels| levels.contains(&CommitmentLevel::Processed)),
        "B must resolve at Processed on its own, independently of the fork at slot 12"
    );

    // Only D -- the bank the Confirmed update actually named -- wins slot 12.
    assert!(
        commitments
            .get(&d.bank_id)
            .is_some_and(|levels| levels.contains(&CommitmentLevel::Confirmed)),
        "D must reach Confirmed"
    );
    assert!(
        !commitments.contains_key(&c.bank_id),
        "C lost the fork at slot 12 to D and must never reach any commitment level"
    );

    // Freezing is purely data-driven (see `seven_competing_banks_only_the_finalized_one_survives`
    // above) -- C is just as data-complete as its siblings, so it freezes too, same as any
    // losing candidate would. What actually marks it as having lost the fork is that it never
    // reaches a commitment level, asserted above.
    assert!(frozen_bank_ids.contains(&a.bank_id));
    assert!(frozen_bank_ids.contains(&b.bank_id));
    assert!(frozen_bank_ids.contains(&c.bank_id));
    assert!(frozen_bank_ids.contains(&d.bank_id));

    // C's loss is also reported directly, block-level, via `BankDiscarded` -- exactly once, and
    // naming no one but C (A/B never competed with a sibling, and D is the winner, not a loser).
    assert_eq!(
        discarded_bank_ids,
        vec![c.bank_id],
        "BankDiscarded must name C exactly once, and no other bank"
    );
}

///
/// The same A/B/C/D tree as `competing_banks_for_the_same_slot_can_have_different_parents`, one
/// generation deeper:
///
/// ```text
///        A
///       / \
///      B   D
///      |    \
///      C     E
/// ```
///
/// Nothing ever gets a *direct* commitment update except E, at the tip -- A, B, C and D rely
/// entirely on retroactive rooting (or, for the losing side, on never resolving at all) once E
/// reaches `Finalized`.
///
/// This test caught a real bug the first time it was written: with `SimulationBuilder::bank`
/// replaying one candidate's *entire* lifecycle before the next's begins, C's `BlockMeta` (and
/// thus its freeze, which triggers `try_infer_sole_candidate_winner`) arrived while D had not yet
/// even had its `CreatedBank` observed -- so slot 12 looked like it had a sole candidate (C) at
/// that instant, and got silently resolved to it. When D showed up moments later, nothing ever
/// revisited that decision, so E's later `Finalized` retroactively rooted the wrong sibling (C)
/// all the way to `Finalized`, while D -- E's real parent -- was silently orphaned.
///
/// Fixed in `process_retroactively_rooted_slots` (`state_machine.rs`): `resolved_bank_per_slot`
/// can hold a bank purely from that earlier sole-candidate guess, which is only ever provisional
/// (see `two_banks_for_same_slot_stay_peers_until_one_is_confirmed`'s own "tentative... not a real
/// discard" case) unless a direct Confirmed/Finalized update backed it -- tracked via
/// `slot_min_commitment`. Before trusting it here, the fix re-checks whether the slot is *still*
/// unambiguous (or was directly confirmed); if a genuine second candidate has since appeared and
/// nothing ever resolved the tie directly, the guess is treated as exactly what it is -- no
/// resolution at all -- rather than promoted straight to `Finalized`.
///
/// With that fixed, this is what actually happens: slot 12 has two genuine candidates and no
/// direct commitment ever names either one, so -- correctly -- **neither C nor D ever resolves**;
/// nothing can safely retroactively root *past* an ambiguous slot without a direct signal saying
/// which bank is canonical (see the comment in `process_retroactively_rooted_slots`). A and B
/// still resolve, but on their own merits (each is the sole candidate for its own slot), not by
/// riding through the ambiguity at 12.
#[tokio::test]
async fn ambiguous_intermediate_ancestor_blocks_retroactive_rooting_of_its_slot() {
    let a = SimulatedBank::new(10, 40_001).with_entry_count(1);
    let b = SimulatedBank::new(11, 40_002)
        .with_entry_count(1)
        .with_parent(a.slot, a.blockhash);
    let c = SimulatedBank::new(12, 40_003)
        .with_entry_count(1)
        .with_parent(b.slot, b.blockhash);
    let d = SimulatedBank::new(12, 40_004)
        .with_entry_count(1)
        .with_parent(a.slot, a.blockhash);
    let e = SimulatedBank::new(13, 40_005)
        .with_entry_count(1)
        .with_parent(d.slot, d.blockhash);

    let stream = SimulationBuilder::new()
        .bank(&a)
        .bank(&b)
        .bank(&c)
        .bank(&d)
        .bank(&e)
        // Only E ever gets a direct commitment update.
        .finalized(e.slot, e.bank_id)
        .build();

    let block_stream = BlockStream::<_, SubscribeUpdate, _>::new(
        stream,
        DragonsmouthBlockCumulator::default(),
        CommitmentLevel::Processed,
    );

    let outputs = collect(block_stream).await;
    let mut commitments: HashMap<BankId, Vec<CommitmentLevel>> = HashMap::new();
    for output in &outputs {
        if let BlockMachineOutput::SlotCommitmentUpdate(update) = output {
            commitments
                .entry(update.bank_id)
                .or_default()
                .push(update.commitment);
        }
    }

    for (name, bank) in [("A", &a), ("B", &b), ("E", &e)] {
        assert!(
            commitments
                .get(&bank.bank_id)
                .is_some_and(|levels| levels.contains(&CommitmentLevel::Finalized)),
            "{name} must reach Finalized"
        );
    }
    // Neither of slot 12's two genuine candidates ever gets to resolve -- no direct commitment
    // ever named either one, and retroactive rooting must not guess between them.
    for (name, bank) in [("C", &c), ("D", &d)] {
        assert!(
            !commitments.contains_key(&bank.bank_id),
            "{name} is one of two genuinely competing, never directly resolved banks for slot \
             12 and must never reach any commitment level"
        );
    }
}

///
/// `bank_a` resolves as slot 40's sole candidate purely via inference (no direct commitment yet,
/// so this is still supersedable). `bank_b` then registers and is confirmed directly, superseding
/// `bank_a` -- a genuine, one-time discard. The same `Confirmed` update for `bank_b` is then
/// delivered a second time (as a duplicate wire delivery would), which must be a pure no-op: it
/// must not discard `bank_a` all over again, and `BankDiscarded` must never fire twice for the
/// same bank_id.
#[tokio::test]
async fn bank_discarded_never_fires_twice_for_the_same_bank_id() {
    let slot = 20_000;
    let bank_a = SimulatedBank::new(slot, 2_000_001).with_entry_count(1);
    let bank_b = SimulatedBank::new(slot, 2_000_002).with_entry_count(1);

    let stream = SimulationBuilder::new()
        .bank(&bank_a)
        .bank(&bank_b)
        .confirmed(slot, bank_b.bank_id)
        // A duplicate delivery of the very same commitment update -- must be a no-op.
        .confirmed(slot, bank_b.bank_id)
        .build();

    let block_stream = BlockStream::<_, SubscribeUpdate, _>::new(
        stream,
        DragonsmouthBlockCumulator::default(),
        CommitmentLevel::Processed,
    );

    let outputs = collect(block_stream).await;
    let discarded_bank_ids: Vec<BankId> = outputs.iter().filter_map(discarded_bank_id).collect();

    assert_eq!(
        discarded_bank_ids,
        vec![bank_a.bank_id],
        "bank_a must be discarded exactly once, even though the Confirmed update that \
         superseded it was itself delivered twice"
    );
}
