use chrono::Utc;
use std::pin::Pin;
use tonic::{transport::Server, Request, Response, Status, Streaming};
use futures::Stream;
use shared_proto::local;

use local::time_producer_server::{TimeProducer, TimeProducerServer};
use local::{Empty, TimeMessage};

#[derive(Default)]
pub struct MyTimeProducer {}

#[tonic::async_trait]
impl TimeProducer for MyTimeProducer {
    type StreamTimesStream = Pin<Box<dyn Stream<Item = Result<TimeMessage, Status>> + Send + 'static>>;

    async fn stream_times(
        &self,
        _request: Request<Streaming<Empty>>,
    ) -> Result<Response<Self::StreamTimesStream>, Status> {
        let (tx, rx) = tokio::sync::mpsc::channel(1024);

        tokio::spawn(async move {
            loop {
                let now = Utc::now();
                let msg = TimeMessage {
                    seconds: now.timestamp().to_string(),
                    nanos: now.timestamp_subsec_nanos() as i32,
                };

                if tx.send(Ok(msg)).await.is_err() {
                    println!("Client disconnected.");
                    break;
                }
            }
        });

        let stream = tokio_stream::wrappers::ReceiverStream::new(rx);
        Ok(Response::new(Box::pin(stream) as Self::StreamTimesStream))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();

    let addr = "[::1]:50071".parse().unwrap();
    println!("Streaming infinite time messages on {}", addr);

    Server::builder()
        .add_service(TimeProducerServer::new(MyTimeProducer::default()))
        .serve(addr)
        .await?;

    Ok(())
}
