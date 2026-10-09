use {
    futures_util::StreamExt,
    solana_commitment_config::CommitmentLevel,
    yellowstone_block_machine::{
        dragonsmouth::{
            block_accumulator::DragonsmouthBlockCumulator,
            simulation::{SimulatedBank, SimulationBuilder},
        },
        stream::{BlockMachineOutput, BlockStream},
        yellowstone_grpc_proto::geyser::SubscribeUpdate,
    },
};

/// Issue #15: complete competing banks must not bypass the requested commitment.
/// ```text
/// slot 9 -> slot 10: bank 1 (Processed), bank 2 (Processed)
/// ```
#[tokio::test]
async fn complete_banks_wait_for_minimum_commitment() {
    for commitment in [CommitmentLevel::Confirmed, CommitmentLevel::Finalized] {
        let source = SimulationBuilder::new()
            .bank(&SimulatedBank::new(10, 1))
            .processed(10, 1)
            .bank(&SimulatedBank::new(10, 2))
            .processed(10, 2)
            .build();
        let mut stream = BlockStream::<_, SubscribeUpdate, _>::new(
            source,
            DragonsmouthBlockCumulator::default(),
            commitment,
        );
        assert!(stream.next().await.is_none());
    }
}

/// Issue #15: late sysvars must not let commitment updates overtake their block.
/// ```text
/// slot 10 -> slot 11: bank 3 (late sysvar)
/// ```
#[tokio::test]
async fn late_sysvar_delivers_block_before_commitments() {
    for commitment in [
        CommitmentLevel::Processed,
        CommitmentLevel::Confirmed,
        CommitmentLevel::Finalized,
    ] {
        let mut events = SimulationBuilder::new()
            .bank(&SimulatedBank::new(11, 3))
            .processed(11, 3)
            .confirmed(11, 3)
            .finalized(11, 3)
            .into_events();
        let late_sysvar = events.remove(4);
        events.push(late_sysvar);
        let source = SimulationBuilder::new().extend(events).build();
        let mut stream = BlockStream::<_, SubscribeUpdate, _>::new(
            source,
            DragonsmouthBlockCumulator::default(),
            commitment,
        );
        let first = stream.next().await.unwrap().unwrap();
        assert!(matches!(first, BlockMachineOutput::FrozenBlock(block) if block.bank_id == 3));
        let levels: Vec<_> = [
            CommitmentLevel::Processed,
            CommitmentLevel::Confirmed,
            CommitmentLevel::Finalized,
        ]
        .into_iter()
        .skip_while(|level| *level != commitment)
        .collect();
        for level in levels {
            let output = stream.next().await.unwrap().unwrap();
            assert!(
                matches!(output, BlockMachineOutput::SlotCommitmentUpdate(update)
                if update.bank_id == 3 && update.commitment == level)
            );
        }
        assert!(stream.next().await.is_none());
    }
}

/// Issue #15: a discarded bank cannot release deferred updates when its sysvar arrives.
/// ```text
/// slot 9 -> slot 10: bank 1 (late sysvar, discarded), bank 2 (Confirmed)
/// ```
#[tokio::test]
async fn discarded_bank_drops_deferred_commitments() {
    let mut events = SimulationBuilder::new()
        .bank(&SimulatedBank::new(10, 1))
        .processed(10, 1)
        .into_events();
    let late_sysvar = events.remove(4);
    events.extend(
        SimulationBuilder::new()
            .bank(&SimulatedBank::new(10, 2))
            .confirmed(10, 2)
            .into_events(),
    );
    events.push(late_sysvar);
    let source = SimulationBuilder::new().extend(events).build();
    let mut stream = BlockStream::<_, SubscribeUpdate, _>::new(
        source,
        DragonsmouthBlockCumulator::default(),
        CommitmentLevel::Processed,
    );
    let mut discards = 0;
    while let Some(output) = stream.next().await {
        match output.unwrap() {
            BlockMachineOutput::FrozenBlock(block) => assert_eq!(block.bank_id, 2),
            BlockMachineOutput::SlotCommitmentUpdate(update) => assert_eq!(update.bank_id, 2),
            BlockMachineOutput::BankDiscarded(discarded) if discarded.bank_id == 1 => discards += 1,
            _ => {}
        }
    }
    assert_eq!(discards, 1);
}
