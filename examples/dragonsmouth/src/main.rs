use {
    clap::Parser, common_macros::hash_map, solana_signature::Signature, std::{collections::{HashMap, HashSet}, path::PathBuf, sync::Arc}, tokio::sync::mpsc, tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt}, yellowstone_block_machine::dragonsmouth::client_ext::{
        Block, BlockMachineError, BlockMachineOutput, GeyserGrpcExt
    }, yellowstone_grpc_client::{ClientTlsConfig, GeyserGrpcBuilder}, yellowstone_grpc_proto::geyser::{CommitmentLevel, SubscribeRequest, subscribe_update::UpdateOneof}
};

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


fn cross_check_account_txn_join(block: Block) {
    let mut account_txn_sig_set: HashSet<Signature> = HashSet::new();
    let mut txn_sig_index_map: HashMap<Signature, u64> = HashMap::new();
    for event in block.events.into_iter() {
        let ev = Arc::unwrap_or_clone(event);

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
            _ => {}
        }
    }

    for sig in account_txn_sig_set {
        if !txn_sig_index_map.contains_key(&sig) {
            panic!("Missing txn for account update sig: {}", sig);
        }
    }
}


#[derive(Debug, clap::Parser)]
#[clap(
    author,
    version,
    about = "Yellowstone Block Machine with Dragonsmouth Extension Example"
)]
struct Args {
    #[clap(long)]
    config: PathBuf,
    #[clap(short, long, default_value_t = 10)]
    samples: usize,
}

#[derive(Debug, Clone, serde::Deserialize)]
struct Config {
    endpoint: String,
    x_token: Option<String>,
}

async fn process_block<W>(
    mut block_recv: mpsc::Receiver<Result<BlockMachineOutput, BlockMachineError>>,
    sample: usize,
    mut out: W,
) where
    W: std::io::Write,
{
    let mut i = 0;
    while let Some(result) = block_recv.recv().await {
        match result {
            Ok(output) => match output {
                BlockMachineOutput::FrozenBlock(block) => {
                    let n = block.len();
                    let slot = block.slot;
                    let account_cnt = block.account_len();
                    let txn_cnt = block.txn_len();
                    let entry_cnt = block.entry_len();
                    writeln!(out, "Block ({i}) {slot} len: {n}, {txn_cnt} tx, {account_cnt} accounts, {entry_cnt} entries").expect("write");
                    cross_check_account_txn_join(block);
                    i += 1;
                }
                BlockMachineOutput::SlotCommitmentUpdate(slot_commitment_status_update) => {
                    writeln!(
                        out,
                        "SlotCommitmentUpdate: {:?}",
                        slot_commitment_status_update
                    )
                    .expect("write");
                }
                BlockMachineOutput::ForkDetected(fork_detected) => {
                    writeln!(out, "ForkDetected: {}", fork_detected.slot).expect("write");
                }
                BlockMachineOutput::DeadBlockDetect(dead_block_detected) => {
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
        .connect()
        .await
        .expect("Failed to connect to geyser");

    // This request listen for all account updates and transaction updates
    let request = SubscribeRequest {
        accounts: hash_map! {
            "test".to_string() => Default::default(),
        },
        transactions: hash_map! {
            "test".to_string() => Default::default(),
        },
        commitment: Some(CommitmentLevel::Confirmed as i32),
        ..Default::default()
    };

    let block_machine_rx = geyser
        .subscribe_block(request)
        .await
        .expect("subscribe_block");
    process_block(block_machine_rx, args.samples, std::io::stdout()).await;
}
