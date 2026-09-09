use {
    clap::Parser,
    common_macros::hash_map,
    futures_util::StreamExt,
    solana_signature::Signature,
    std::{
        collections::{HashMap, HashSet},
        path::PathBuf,
    },
    tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt},
    yellowstone_block_machine::dragonsmouth::client_ext::{
        BlockStreamEvent, DragonsmouthBlock, DragonsmouthBlockStream, GeyserGrpcExt,
    },
    yellowstone_grpc_client::{ClientTlsConfig, GeyserGrpcBuilder},
    yellowstone_grpc_proto::geyser::{
        CommitmentLevel, SubscribeRequest, subscribe_update::UpdateOneof,
    },
};

/// Installs the process-global tracing subscriber for this example binary: ANSI-colored,
/// line-numbered output filtered by the `RUST_LOG` environment variable (or its default filter
/// if unset).
///
/// # Panics
///
/// Panics if a global tracing subscriber has already been installed.
pub fn init_tracing() {
    let io_layer = tracing_subscriber::fmt::layer()
        .with_ansi(true)
        .with_line_number(true);

    let level_layer = EnvFilter::builder().from_env_lossy();
    tracing_subscriber::registry()
        .with(io_layer)
        .with(level_layer)
        .try_init()
        .expect("tracing init");
}

/// Cross-checks that every `Account` update's transaction signature (when it has one) also
/// shows up among the `Transaction` updates in the same block.
///
/// # Arguments
///
/// * `block` - The [`DragonsmouthBlock`] to scan. Taken by value since matching every event
///   inside it requires owning them.
///
/// # Panics
///
/// Panics if an `Account` update carries no
/// [`SubscribeUpdateAccountInfo`](yellowstone_grpc_proto::geyser::SubscribeUpdateAccountInfo),
/// if an account's or a transaction's signature bytes don't form a valid [`Signature`], or if an
/// account update's transaction signature never appears among the block's transaction updates.
fn cross_check_account_txn_join(block: DragonsmouthBlock) {
    let mut account_txn_sig_set: HashSet<Signature> = HashSet::new();
    let mut txn_sig_index_map: HashMap<Signature, u64> = HashMap::new();
    for ev in block.into_iter() {
        let Some(update) = ev.update_oneof else {
            continue;
        };
        match update {
            UpdateOneof::Account(subscribe_update_account) => {
                let Some(sig) = subscribe_update_account.account.unwrap().txn_signature else {
                    continue;
                };
                let sig = Signature::try_from(sig.as_slice()).expect("signature");
                account_txn_sig_set.insert(sig);
            }
            UpdateOneof::Transaction(subscribe_update_transaction) => {
                let Some(txn) = subscribe_update_transaction.transaction else {
                    continue;
                };
                let sig = Signature::try_from(txn.signature.as_slice()).expect("signature");
                txn_sig_index_map.insert(sig, txn.index);
            }
            UpdateOneof::TransactionStatus(txn) => {
                let sig = Signature::try_from(txn.signature.as_slice()).expect("signature");
                txn_sig_index_map.insert(sig, txn.index);
            }
            _ => {}
        }
    }

    for sig in account_txn_sig_set {
        if !txn_sig_index_map.contains_key(&sig) {
            panic!("Missing txn for account update sig: {}", sig);
        }
    }
}

/// Command-line arguments for this example binary.
#[derive(Debug, clap::Parser)]
#[clap(
    author,
    version,
    about = "Yellowstone Block Machine with Dragonsmouth Extension Example"
)]
struct Args {
    /// Path to a YAML file deserializable as [`Config`].
    #[clap(long)]
    config: PathBuf,
    /// How many [`BlockStreamEvent::FrozenBlock`] events to print before [`process_block`]
    /// stops.
    #[clap(short, long, default_value_t = 10)]
    samples: usize,
    /// If set, suppresses printing [`BlockStreamEvent::SlotCommitmentUpdate`] events.
    #[clap(long)]
    no_slot_commitment_updates: bool,
}

/// Geyser gRPC endpoint configuration, loaded from the YAML file named by [`Args::config`].
#[derive(Debug, Clone, serde::Deserialize)]
struct Config {
    /// The gRPC endpoint URL to connect to.
    endpoint: String,
    /// Optional `x-token` metadata value used to authenticate with `endpoint`.
    #[serde(alias = "x-token")]
    x_token: Option<String>,
}

/// Drives a [`DragonsmouthBlockStream`] to completion, writing one human-readable line per
/// event it yields, until `sample` [`BlockStreamEvent::FrozenBlock`] events have been printed,
/// the stream ends, or it yields an error.
///
/// # Arguments
///
/// * `block_stream` - The [`DragonsmouthBlockStream`] to poll for events.
/// * `sample` - How many [`BlockStreamEvent::FrozenBlock`] events to print before stopping.
/// * `slot_commitment_updates` - Whether to also print [`BlockStreamEvent::SlotCommitmentUpdate`]
///   events.
/// * `out` - Destination for the printed output.
///
/// # Panics
///
/// Panics if writing to `out` fails, or if a frozen block's transaction count (summed across its
/// entries) doesn't match the number of `Transaction` updates actually observed for it -- see
/// [`cross_check_account_txn_join`] for the other consistency check run on each block.
async fn process_block<W>(
    mut block_stream: DragonsmouthBlockStream,
    sample: usize,
    slot_commitment_updates: bool,
    mut out: W,
) where
    W: std::io::Write,
{
    let mut i = 0;
    while let Some(result) = block_stream.next().await {
        match result {
            Ok(output) => match output {
                BlockStreamEvent::FrozenBlock(block) => {
                    let slot = block.slot();
                    let bank_id = block.bank_id();

                    let mut account_cnt = 0u64;
                    let mut entry_cnt = 0u64;
                    let mut entry_txn_cnt = 0u64;
                    let mut unique_sig_set = HashSet::new();
                    for ev in block.iter() {
                        match ev.update_oneof.as_ref() {
                            Some(UpdateOneof::Account(_)) => account_cnt += 1,
                            Some(UpdateOneof::Transaction(txn)) => {
                                let sig = txn.transaction.as_ref().unwrap().signature.clone();
                                let sig = Signature::try_from(sig).expect("sig");
                                unique_sig_set.insert(sig);
                            }
                            Some(UpdateOneof::TransactionStatus(txn)) => {
                                let sig = txn.signature.as_ref();
                                let sig = Signature::try_from(sig).expect("sig");
                                unique_sig_set.insert(sig);
                            }
                            Some(UpdateOneof::Entry(entry)) => {
                                entry_cnt += 1;
                                entry_txn_cnt += entry.executed_transaction_count;
                            }
                            _ => {}
                        }
                    }
                    assert_eq!(
                        entry_txn_cnt as usize,
                        unique_sig_set.len(),
                        "slot {}: sum of transaction count across entries ({}) must equal transactions received ({})",
                        slot,
                        entry_txn_cnt as usize,
                        unique_sig_set.len()
                    );
                    let parent_slot = block.parent_slot();
                    let parent_blockhash = bs58::encode(block.parent_blockhash()).into_string();

                    writeln!(out, "Block ({i}) {slot}, bank_id: {bank_id}, txn: {}, account: {account_cnt}, entry: {entry_cnt}, parent_slot: {parent_slot}, parent hash: {parent_blockhash}", unique_sig_set.len()).expect("write");
                    cross_check_account_txn_join(block);
                    i += 1;
                }
                BlockStreamEvent::SlotCommitmentUpdate(slot_commitment_status_update) => {
                    if slot_commitment_updates {
                        writeln!(
                            out,
                            "slot: {}, bank_id: {},  commtiment: {}",
                            slot_commitment_status_update.slot,
                            slot_commitment_status_update.bank_id,
                            slot_commitment_status_update.commitment,
                        )
                        .expect("write");
                    }
                }
                BlockStreamEvent::ForkDetected(fork_detected) => {
                    writeln!(out, "ForkDetected: {}", fork_detected.slot).expect("write");
                }
                BlockStreamEvent::DeadBlockDetected(dead_block_detected) => {
                    writeln!(out, "DeadBlockDetect: {}", dead_block_detected.slot).expect("write");
                }
            },
            Err(e) => {
                writeln!(out, "BlockMachineError: {:?}", e).expect("write");
                break;
            }
        }
        if i >= sample {
            writeln!(out, "Sample limit reached: {sample}").expect("write");
            break;
        }
    }
}

/// Entry point: parses [`Args`], loads the [`Config`] it points to, connects to the configured
/// Geyser endpoint, subscribes to blocks via
/// [`GeyserGrpcExt::subscribe_block`],
/// and hands the resulting stream to [`process_block`].
///
/// # Panics
///
/// Panics if the config file named by [`Args::config`] can't be opened or parsed as [`Config`],
/// if the endpoint can't be reached, TLS/auth can't be configured, or the block subscription
/// fails, or (transitively) on any of the panic conditions documented on [`process_block`].
#[tokio::main]
async fn main() {
    init_tracing();
    let args = Args::parse();
    let config: Config =
        serde_yaml::from_reader(std::fs::File::open(args.config).unwrap()).expect("open config");
    let endpoint = config.endpoint;
    let x_token = config.x_token;
    let mut geyser = GeyserGrpcBuilder::from_shared(endpoint)
        .expect("Failed to parse endpoint")
        .x_token(x_token)
        .expect("x_token")
        .tls_config(ClientTlsConfig::new().with_native_roots())
        .expect("tls_config")
        .max_decoding_message_size(50 * 1024 * 1024) // 50MB
        .accept_compressed(tonic::codec::CompressionEncoding::Zstd)
        .http2_adaptive_window(true)
        .connect()
        .await
        .expect("Failed to connect to geyser");

    // This request listen for all account updates and transaction updates
    let request = SubscribeRequest {
        accounts: hash_map! {
            "test".to_string() => Default::default(),
        },
        transactions_status: hash_map! {
            "test".to_string() => Default::default(),
        },
        entry: hash_map! {
            "test".to_string() => Default::default(),
        },
        commitment: Some(CommitmentLevel::Processed as i32),
        ..Default::default()
    };

    let block_machine_rx = geyser
        .subscribe_block(request)
        .await
        .expect("subscribe_block");
    process_block(
        block_machine_rx,
        args.samples,
        !args.no_slot_commitment_updates,
        std::io::stdout(),
    )
    .await;
}
