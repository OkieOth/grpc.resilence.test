use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use clap::Parser;
use resilience_rust::api::{BidirectionalStreamRequest, BidirectionalStreamResponse, Ping};
use resilience_rust::logfmt;
use tokio::signal;
use tokio::sync::mpsc;
use tonic::codegen::tokio_stream;
use tonic::metadata::MetadataMap;
use tonic::transport::Channel;
use tonic::{Code, Request, Status};

const MAX_RETRIES: u32 = 10;
const RECONNECT_DELAY: Duration = Duration::from_secs(1);

#[derive(Parser, Debug)]
#[command(name = "client", about = "ResilienceService gRPC client")]
struct Args {
    /// gRPC target (default: localhost:50051)
    #[arg(default_value = "localhost:50051")]
    target: String,

    /// Identifier used in Ping messages (default: client)
    #[arg(long = "client-id", default_value = "client")]
    client_id: String,

    /// Interval between application-level Ping messages (default: 500)
    #[arg(long = "ping-interval-ms", default_value_t = 500)]
    ping_interval_ms: u64,

    /// gRPC keepalive time in seconds (default: 10)
    #[arg(long = "keepalive-time-sec", default_value_t = 10)]
    keepalive_time_sec: u64,

    /// gRPC keepalive timeout in seconds (default: 5)
    #[arg(long = "keepalive-timeout-sec", default_value_t = 5)]
    keepalive_timeout_sec: u64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    // Target normalization: prepend scheme if missing
    let url = if args.target.contains("://") {
        args.target.clone()
    } else {
        format!("http://{}", args.target)
    };

    let ping_interval = Duration::from_millis(args.ping_interval_ms);
    let keepalive_time = Duration::from_secs(args.keepalive_time_sec);
    let keepalive_timeout = Duration::from_secs(args.keepalive_timeout_sec);

    // Set up signal handling
    let (signal_tx, mut signal_rx) = tokio::sync::oneshot::channel::<u32>();

    let signal_task = tokio::spawn(async move {
        #[cfg(unix)]
        {
            use tokio::signal::unix::{signal, SignalKind};

            let ctrl_c = signal::ctrl_c();
            let mut term_stream =
                signal(SignalKind::terminate()).expect("failed to install SIGTERM handler");
            let term = term_stream.recv();

            tokio::select! {
                _ = ctrl_c => {
                    let _ = signal_tx.send(2); // SIGINT
                }
                _ = term => {
                    let _ = signal_tx.send(15); // SIGTERM
                }
            }
        }
        #[cfg(not(unix))]
        {
            signal::ctrl_c().await.ok();
            let _ = signal_tx.send(2); // SIGINT
        }
    });

    // Retry loop
    let mut attempt: u32 = 0;
    let total_pings_sent = std::sync::Arc::new(AtomicU64::new(0));
    let mut total_pong_received: u64 = 0;

    loop {
        attempt += 1;

        if attempt > 1 {
            logfmt::info("client", &format!("Reconnecting (attempt {attempt}/10) …"));
            tokio::time::sleep(RECONNECT_DELAY).await;
        }

        // Check if signal was received before attempting to connect
        if signal_rx.try_recv().is_ok() {
            logfmt::info("client", "Shutting down.");
            break;
        }

        // Try to connect and establish the stream
        let _ping_count = AtomicU64::new(0);
        let result = establish_connection(
            &url,
            &args.client_id,
            ping_interval,
            keepalive_time,
            keepalive_timeout,
            attempt,
            &total_pings_sent,
        )
        .await;

        match result {
            Ok(pong_count) => {
                total_pong_received += pong_count;
            }
            Err(_) => {
                // Connection/stream error, retry will happen
            }
        }

        // Check for signal after each attempt
        if signal_rx.try_recv().is_ok() {
            logfmt::info("client", "Shutting down.");
            break;
        }

        if attempt >= MAX_RETRIES {
            logfmt::info("client", "Max retries (10) reached.");
            break;
        }
    }

    // Wait for signal task to finish
    let _ = signal_task.await;

    let pings_sent = total_pings_sent.load(Ordering::SeqCst);
    logfmt::info(
        "client",
        &format!(
            "Done. Attempt={attempt}  Pings sent={pings_sent}  Pongs received={total_pong_received}"
        ),
    );

    Ok(())
}

async fn establish_connection(
    url: &str,
    client_id: &str,
    ping_interval: Duration,
    keepalive_time: Duration,
    keepalive_timeout: Duration,
    attempt: u32,
    total_pings_sent: &std::sync::Arc<AtomicU64>,
) -> Result<u64, Status> {
    // Connect to the server
    let channel = Channel::from_shared(url.to_string())
        .unwrap()
        .http2_keep_alive_interval(keepalive_time)
        .keep_alive_timeout(keepalive_timeout)
        .connect()
        .await
        .map_err(|e| {
            logfmt::error(
                "client",
                &format!("Connection error (attempt {attempt}): {e}"),
            );
            Status::new(Code::Unavailable, format!("Connection error: {e}"))
        })?;

    // Create a client
    let mut client =
        resilience_rust::api::resilience_service_client::ResilienceServiceClient::new(channel);

    // Set up request channel
    let (tx, rx) = mpsc::channel(100);

    // Build request with client-id metadata
    let mut request = Request::new(tokio_stream::wrappers::ReceiverStream::new(rx));
    let mut metadata = MetadataMap::new();
    metadata.insert("client-id", client_id.parse().unwrap());
    *request.metadata_mut() = metadata;

    // Spawn sender task: sends pings at the configured interval
    let sender_handle = tokio::spawn(send_pings(
        tx,
        ping_interval,
        client_id.to_string(),
        total_pings_sent.clone(),
    ));

    // Call the streaming RPC
    let response = match client.stream(request).await {
        Ok(resp) => resp,
        Err(status) => {
            logfmt::error(
                "client",
                &format!(
                    "RPC error (attempt {attempt}): {} – {}",
                    status.code(),
                    status.message()
                ),
            );
            sender_handle.abort();
            return Err(status);
        }
    };

    // Spawn receiver task
    let receiver_handle = tokio::spawn(recv_pongs(response.into_inner(), attempt));

    // Wait for sender to complete (it runs until channel is dropped)
    sender_handle.await.ok();

    // Wait for receiver to collect pongs
    let pong_count = receiver_handle.await.unwrap_or(0);

    Ok(pong_count)
}

async fn send_pings(
    tx: mpsc::Sender<BidirectionalStreamRequest>,
    ping_interval: Duration,
    client_id: String,
    total_pings_sent: std::sync::Arc<AtomicU64>,
) {
    let mut interval: tokio::time::Interval = tokio::time::interval(ping_interval);
    // Consume the initial tick immediately (like Python's send-then-sleep)
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    let mut ping_id: i32 = 0;
    loop {
        tokio::select! {
            _ = interval.tick() => {
                ping_id += 1;
                logfmt::info("client", &format!("Client sending ping #{ping_id}"));
                let req = BidirectionalStreamRequest {
                    ping: Some(Ping {
                        id: ping_id,
                        sender: client_id.clone(),
                    }),
                };
                let _ = tx.send(req).await;
                total_pings_sent.fetch_add(1, Ordering::SeqCst);
            }
        }
    }
}

async fn recv_pongs(
    mut stream: tonic::Streaming<BidirectionalStreamResponse>,
    attempt: u32,
) -> u64 {
    let mut count: u64 = 0;

    loop {
        match stream.message().await {
            Ok(Some(response)) => match response.payload {
                Some(resilience_rust::api::bidirectional_stream_response::Payload::Pong(pong)) => {
                    logfmt::info(
                        "client",
                        &format!("Client received Pong #{} from {}", pong.id, pong.sender),
                    );
                    count += 1;
                }
                Some(resilience_rust::api::bidirectional_stream_response::Payload::Error(err)) => {
                    logfmt::warn(
                        "client",
                        &format!(
                            "Client received Error #{}: code={} msg={}",
                            err.id, err.code, err.message
                        ),
                    );
                    break;
                }
                None => {
                    break;
                }
            },
            Ok(None) => {
                // Stream ended normally
                break;
            }
            Err(status) => {
                logfmt::error(
                    "client",
                    &format!(
                        "RPC error (attempt {attempt}): {} – {}",
                        status.code(),
                        status.message()
                    ),
                );
                break;
            }
        }
    }

    count
}
