use {
    std::collections::HashMap,
    yellowstone_grpc_client::{ClientTlsConfig, GeyserGrpcBuilder},
    yellowstone_grpc_proto::geyser::{
        CommitmentLevel, SubscribeRequest, SubscribeRequestFilterAccounts,
        SubscribeRequestFilterSlots,
    },
};

#[tokio::main]
async fn main() {
    let endpoint = "http://127.0.0.1:10000";
    let mut geyser = GeyserGrpcBuilder::from_shared(endpoint)
        .expect("Failed to parse endpoint")
        .tls_config(ClientTlsConfig::new().with_native_roots())
        .expect("tls_config")
        .accept_compressed(tonic::codec::CompressionEncoding::Gzip)
        .send_compressed(tonic::codec::CompressionEncoding::Gzip)
        .connect()
        .await
        .expect("Failed to connect to geyser");

    // This request listen for all account updates and transaction updates
    let request = SubscribeRequest {
        slots: HashMap::from([(
            "f1".to_owned(),
            SubscribeRequestFilterSlots {
                interslot_updates: Some(true),
                ..Default::default()
            },
        )]),
        blocks_meta: HashMap::from([("f1".to_owned(), Default::default())]),
        entry: HashMap::from([("f1".to_owned(), Default::default())]),
        commitment: Some(CommitmentLevel::Processed as i32),
        ..Default::default()
    };

    // You can perform asynchronous operations here.
    // For example, you could await an asynchronous function:
    // let result = some_async_function().await;

    // println!("Result: {:?}", result);
}
