use std::time::{Duration, Instant};
use chrono::{DateTime, TimeZone, Utc};
use tonic::Request;
use futures::StreamExt;
use shared_proto::local;

use local::{time_producer_client::TimeProducerClient, Empty};

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = TimeProducerClient::connect("http://[::1]:50071").await?;

    let request_stream = futures::stream::iter(vec![Empty {}]);

    let mut stream = client.stream_times(Request::new(request_stream))
        .await?
        .into_inner();

    println!("🔁 Streaming time responses from server...");

    let mut count = 0;
    let mut last_print = Instant::now();
    let print_interval = Duration::from_secs(1);

    while let Some(time_response) = stream.next().await {
        match time_response {
            Ok(resp) => {
                count += 1;
                let now_ms = Utc::now().timestamp_millis();
                // let now_ms = SystemTime::now()
                //     .duration_since(UNIX_EPOCH)?
                //     .as_millis();

                let datetime: DateTime<Utc> = Utc.timestamp_opt(resp.seconds.parse::<i64>().unwrap(), resp.nanos as u32).unwrap();
                let message_ms = datetime.timestamp_millis();
                // let message_ms = (resp.seconds.parse::<u128>().unwrap() * 1000) + (resp.nanos as u128 / 1_000_000);
                
                let latency_ms = now_ms - message_ms;
                
                println!("📡 Message latency: {} ms", latency_ms);
                // println!("🕒 Received: {}", resp.seconds);
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