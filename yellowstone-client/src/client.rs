use chrono::{DateTime, TimeZone, Utc};
use clap::Parser;
use futures::StreamExt;
use prost_types::Timestamp;
use std::{
    collections::HashMap,
    str::FromStr,
    time::{Duration, Instant},
};

use tonic::{metadata::MetadataValue, transport::Channel, Request};
use yellowstone_grpc_proto::geyser::{
    geyser_client::GeyserClient, CommitmentLevel, SubscribeRequest, SubscribeRequestFilterAccounts,
};

#[derive(Debug, Clone, Parser)]
#[clap(author, version, about)]
struct Args {
    #[clap(short, long, default_value_t = String::from("http://127.0.0.1:10000"))]
    endpoint: String,

    #[clap(long)]
    x_token: Option<String>,
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let channel = Channel::from_shared(args.endpoint.clone())?
        .connect()
        .await?;

    let token = args.x_token.clone();
let interceptor = move |mut req: Request<()>| {
        if let Some(t) = &token {
            if let Ok(meta_val) = MetadataValue::from_str(t) {
                req.metadata_mut().insert("x-token", meta_val);
            }
        }
        Ok(req)
    };
    let mut client = GeyserClient::with_interceptor(channel, interceptor);

    let request = SubscribeRequest {
        slots: HashMap::new(),
        accounts: {
            let mut accounts = HashMap::new();
            accounts.insert(
                "slot_account_updates".to_string(),
                SubscribeRequestFilterAccounts {
                    account: vec![],
                    owner: vec![],
                    filters: vec![],
                    nonempty_txn_signature: None,
                },
            );
            accounts
        },
        transactions: HashMap::new(),
        transactions_status: HashMap::new(),
        entry: HashMap::new(),
        blocks: HashMap::new(),
        blocks_meta: HashMap::new(),
        commitment: Some(CommitmentLevel::Processed as i32),
        accounts_data_slice: vec![],
        ping: None,
        from_slot: None,
    };

    let request_stream = futures::stream::iter(vec![request]);
    let mut response_stream = client.subscribe(Request::new(request_stream)).await?.into_inner();

    println!("🔁 Streaming time responses from server...");

    let mut count = 0;
    let mut last_print = Instant::now();
    let print_interval = Duration::from_secs(1);

    while let Some(update) = response_stream.next().await {
        match update {
            Ok(resp) => {
                count += 1;
                let now_ms = Utc::now().timestamp_millis();
                // let now_ms = SystemTime::now()
                //     .duration_since(UNIX_EPOCH)?
                //     .as_millis();

                let created_at = resp.created_at.unwrap_or(Timestamp {
                    seconds: 0,
                    nanos: 0,
                });

                let datetime: DateTime<Utc> = Utc
                    .timestamp_opt(created_at.seconds, created_at.nanos as u32)
                    .unwrap();
                let message_ms = datetime.timestamp_millis();
                // let message_ms = (resp.seconds.parse::<u128>().unwrap() * 1000) + (resp.nanos as u128 / 1_000_000);

                let latency_ms = now_ms - message_ms;

                println!("📡 Message latency: {} ms", latency_ms);
                if last_print.elapsed() >= print_interval {
                    println!("📊 Messages per second: {}", count);
                    count = 0;
                    last_print = Instant::now();
                }
            }

            Err(e) => {
                eprintln!("⚠️ Stream error: {:?}", e);
                break;
            }
        }
    }

    Ok(())
}