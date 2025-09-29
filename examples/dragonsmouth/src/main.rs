use {
    clap::Parser,
    common_macros::hash_map,
    std::path::PathBuf,
    tokio::sync::mpsc,
    tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt},
    yellowstone_block_machine::dragonsmouth::client_ext::{
        BlockMachineError, BlockMachineOutput, GeyserGrpcExt,
    },
    yellowstone_grpc_client::{ClientTlsConfig, GeyserGrpcBuilder},
    yellowstone_grpc_proto::geyser::{CommitmentLevel, SubscribeRequest},
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

#[derive(Debug, clap::Parser)]
#[clap(
    author,
    version,
    about = "Yellowstone Block Machine with Dragonsmouth Extension Example"
)]
struct Args {
    #[clap(long)]
    config: PathBuf,
    #[clap(long, default_value_t = 10)]
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
                    writeln!(out, "Block {slot} len: {n}, {txn_cnt} tx, {account_cnt} accounts, {entry_cnt} entries").expect("write");
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
        entry: hash_map! {
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
