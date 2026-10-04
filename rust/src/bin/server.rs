use std::net::SocketAddr;
use std::time::Duration;

use clap::Parser;
use resilience_rust::api::resilience_service_server::{ResilienceService, ResilienceServiceServer};
use resilience_rust::api::{BidirectionalStreamRequest, BidirectionalStreamResponse};
use resilience_rust::logfmt;
use tokio::signal;
use tonic::codegen::tokio_stream;
use tonic::transport::Server;
use tonic::{Request, Response, Status, Streaming};

#[derive(Parser, Debug)]
#[command(name = "server", about = "ResilienceService gRPC server")]
struct Args {
    /// Port to listen on (default: 50051)
    #[arg(default_value_t = 50051)]
    port: u16,

    /// Interval between application-level Pong messages (default: 500)
    #[arg(long = "ping-interval-ms", default_value_t = 500)]
    ping_interval_ms: u64,

    /// gRPC keepalive time in seconds (default: 10)
    #[arg(long = "keepalive-time-sec", default_value_t = 10)]
    keepalive_time_sec: u64,

    /// gRPC keepalive timeout in seconds (default: 5)
    #[arg(long = "keepalive-timeout-sec", default_value_t = 5)]
    keepalive_timeout_sec: u64,
}

#[derive(Clone)]
struct ResilienceServicer {
    ping_interval: Duration,
}

#[tonic::async_trait]
impl ResilienceService for ResilienceServicer {
    type StreamStream =
        tokio_stream::wrappers::ReceiverStream<Result<BidirectionalStreamResponse, Status>>;

    async fn stream(
        &self,
        request: Request<Streaming<BidirectionalStreamRequest>>,
    ) -> Result<Response<Self::StreamStream>, Status> {
        let client_id = request
            .metadata()
            .get("client-id")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_string();

        let peer = request
            .remote_addr()
            .map(|a| a.to_string())
            .unwrap_or_else(|| "<unknown>".to_string());

        let (tx, rx) = tokio::sync::mpsc::channel(100);
        let ping_interval = self.ping_interval;

        tokio::spawn(async move {
            let mut stream = request.into_inner();
            let mut server_ping_id: i32 = 0;
            let mut last_send: Option<tokio::time::Instant> = None;

            loop {
                match stream.message().await {
                    Ok(Some(req)) => {
                        let ping_id = req.ping.as_ref().map(|p| p.id).unwrap_or(0);
                        let sender = req.ping.as_ref().map(|p| p.sender.as_str()).unwrap_or("");

                        logfmt::info(
                            "server",
                            &format!("Server received ping #{ping_id} from {sender}"),
                        );

                        let now = tokio::time::Instant::now();
                        let should_send = match last_send {
                            None => true,
                            Some(last) => now.duration_since(last) >= ping_interval,
                        };

                        if should_send {
                            server_ping_id += 1;
                            last_send = Some(now);
                            logfmt::info(
                                "server",
                                &format!("Server sending Pong #{server_ping_id} to {sender}"),
                            );
                            let _ = tx.send(Ok(BidirectionalStreamResponse {
                                payload: Some(resilience_rust::api::bidirectional_stream_response::Payload::Pong(
                                    resilience_rust::api::Pong {
                                        id: server_ping_id,
                                        sender: "server".to_string(),
                                    },
                                )),
                            })).await;
                        }
                    }
                    Ok(None) => {
                        // Clean stream end
                        logfmt::info(
                            "server",
                            &format!("Connection closed (client={client_id}, peer={peer})"),
                        );
                        break;
                    }
                    Err(status) => {
                        // Transport error
                        logfmt::warn("server", &format!("Connection lost (client={client_id}, peer={peer}, reason={status})"));
                        break;
                    }
                }
            }
            // Drop tx to signal completion
        });

        Ok(Response::new(tokio_stream::wrappers::ReceiverStream::new(
            rx,
        )))
    }
}

#[cfg(unix)]
async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c().await.ok();
    };
    let terminate = async {
        let mut term_stream = signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler");
        let _ = term_stream.recv().await;
    };
    tokio::select! {
        _ = ctrl_c => (),
        _ = terminate => (),
    }
}

#[cfg(not(unix))]
async fn shutdown_signal() {
    signal::ctrl_c().await.ok();
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    let ping_interval = Duration::from_millis(args.ping_interval_ms);
    let keepalive_time = Duration::from_secs(args.keepalive_time_sec);
    let keepalive_timeout = Duration::from_secs(args.keepalive_timeout_sec);

    logfmt::info(
        "server",
        &format!(
            "Resilience gRPC server listening on port {} (app ping {} ms, keepalive {}s)",
            args.port, args.ping_interval_ms, args.keepalive_time_sec
        ),
    );

    let servicer = ResilienceServicer { ping_interval };

    let address: SocketAddr = format!("[::]:{}", args.port).parse()?;

    Server::builder()
        .http2_keepalive_interval(Some(keepalive_time))
        .http2_keepalive_timeout(Some(keepalive_timeout))
        .add_service(ResilienceServiceServer::new(servicer))
        .serve_with_shutdown(address, shutdown_signal())
        .await?;

    logfmt::info("server", "Server shutting down.");
    Ok(())
}
