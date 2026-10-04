//! Common test utilities for integration tests.
//! Provides server fixture, free port allocation, readiness wait,
//! and a client helper for in-process test clients.

use std::fs::File;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use resilience_rust::api::resilience_service_client::ResilienceServiceClient;
use resilience_rust::api::{BidirectionalStreamRequest, Ping, Pong};
use tokio::sync::mpsc;
use tonic::codegen::tokio_stream;
use tonic::metadata::MetadataMap;
use tonic::transport::Channel;

/// Allocate a free TCP port by binding and immediately dropping.
pub fn free_port() -> std::io::Result<u16> {
    let listener = std::net::TcpListener::bind("[::]:0")?;
    let port = listener.local_addr()?.port();
    Ok(port)
}

/// Server fixture: starts a background server process and cleans up on drop.
pub struct ServerFixture {
    pub address: String,
    pub log_path: PathBuf,
    child: Option<std::process::Child>,
}

impl ServerFixture {
    /// Start a server on a free port with test-friendly settings.
    pub fn new() -> std::io::Result<Self> {
        let port = free_port()?;
        let address = format!("localhost:{}", port);

        let log_path = PathBuf::from(format!("/tmp/rust_test_{}.log", port));
        let _ = File::create(&log_path).map_err(|e| {
            std::io::Error::new(e.kind(), format!("Failed to create log file: {}", e))
        })?;

        let server_path = std::env::var("CARGO_BIN_EXE_server")
            .unwrap_or_else(|_| "./target/debug/server".to_string());

        let child = std::process::Command::new(&server_path)
            .arg(port.to_string())
            .arg("--ping-interval-ms")
            .arg("100")
            .arg("--keepalive-time-sec")
            .arg("10")
            .arg("--keepalive-timeout-sec")
            .arg("5")
            .stdout(
                File::options()
                    .create(true)
                    .write(true)
                    .open(&log_path)
                    .map_err(|e| {
                        std::io::Error::new(e.kind(), format!("Failed to open log file: {}", e))
                    })?,
            )
            .spawn()
            .map_err(|e| std::io::Error::new(e.kind(), format!("Failed to start server: {}", e)))?;

        Ok(ServerFixture {
            address,
            log_path,
            child: Some(child),
        })
    }

    /// Read the server log file contents.
    pub fn read_log(&self) -> String {
        std::fs::read_to_string(&self.log_path).unwrap_or_default()
    }

    /// Wait for the server to be ready: connect, send one ping, read one pong, close.
    pub async fn wait_for_server(address: &str) -> Result<(), String> {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        let retry_interval = Duration::from_millis(250);

        loop {
            if tokio::time::Instant::now() > deadline {
                return Err(format!(
                    "Server did not become ready within 10s (address {})",
                    address
                ));
            }

            let result = (|| async {
                let channel = Channel::from_shared(format!("http://{address}"))
                    .map_err(|e| format!("bad url: {e}"))?
                    .connect()
                    .await
                    .map_err(|e| format!("connect: {e}"))?;

                let mut client = ResilienceServiceClient::new(channel);

                let (tx, rx) = mpsc::channel::<BidirectionalStreamRequest>(10);
                let mut request =
                    tonic::Request::new(tokio_stream::wrappers::ReceiverStream::new(rx));
                let mut metadata = MetadataMap::new();
                metadata.insert("client-id", "_health_".parse().unwrap());
                *request.metadata_mut() = metadata;

                // Send one ping, then close sender so server sees EOF.
                let _ = tx
                    .send(BidirectionalStreamRequest {
                        ping: Some(Ping {
                            id: 0,
                            sender: "_health_".to_string(),
                        }),
                    })
                    .await;
                drop(tx);

                let resp = match client.stream(request).await {
                    Ok(r) => r,
                    Err(e) => return Err(format!("rpc: {e}")),
                };
                let mut stream = resp.into_inner();

                // Expect one pong then EOF.
                match stream.message().await {
                    Ok(Some(_)) => {}
                    Ok(None) => {}
                    Err(e) => return Err(format!("stream: {e}")),
                }
                Ok::<(), String>(())
            })();

            if result.await.is_ok() {
                return Ok(());
            }
            tokio::time::sleep(retry_interval).await;
        }
    }

    /// Run a test client: sends pings at interval until duration elapses.
    /// Returns (pings_sent, list of received pongs).
    pub async fn run_client(
        &self,
        client_id: &str,
        interval_ms: u64,
        duration_s: f64,
    ) -> (u64, Vec<Pong>) {
        let total_pings = Arc::new(AtomicU64::new(0));
        let pongs = Arc::new(std::sync::Mutex::new(Vec::<Pong>::new()));
        let interval = Duration::from_millis(interval_ms);
        let duration = Duration::from_secs_f64(duration_s);
        let client_id_owned = client_id.to_string();

        // Single connection: sender sends pings through tx,
        // server reads from rx (inside request's ReceiverStream),
        // and the same response stream is used to receive pongs.
        let (tx, rx) = mpsc::channel(100);

        let client_addr = self.address.clone();
        let client_id_tx = client_id_owned.clone();
        let total_pings_tx = total_pings.clone();

        // Sender task: sends pings through tx until duration elapses.
        let sender = tokio::spawn(send_pings(
            tx,
            interval,
            client_id_tx,
            duration,
            total_pings_tx,
        ));

        // Receiver: connects to server, starts the stream, reads pongs.
        // The request uses the SAME rx that the sender writes to.
        let pongs_rc = pongs.clone();
        let receiver = tokio::spawn(async move {
            let result = (|| async {
                let channel = Channel::from_shared(format!("http://{client_addr}"))
                    .map_err(|e| format!("bad url: {e}"))?
                    .connect()
                    .await
                    .map_err(|e| format!("connect: {e}"))?;

                let mut client = ResilienceServiceClient::new(channel);

                let mut request =
                    tonic::Request::new(tokio_stream::wrappers::ReceiverStream::new(rx));
                let mut metadata = MetadataMap::new();
                metadata.insert("client-id", client_id_owned.parse().unwrap());
                *request.metadata_mut() = metadata;

                let resp = match client.stream(request).await {
                    Ok(r) => r,
                    Err(e) => return Err(format!("stream: {e}")),
                };
                let mut stream = resp.into_inner();

                // Read pongs until stream closes.
                loop {
                    match stream.message().await {
                        Ok(Some(resp)) => match resp.payload {
                            Some(
                                resilience_rust::api::bidirectional_stream_response::Payload::Pong(
                                    pong,
                                ),
                            ) => {
                                pongs_rc.lock().unwrap().push(pong);
                            }
                            Some(
                                resilience_rust::api::bidirectional_stream_response::Payload::Error(
                                    _,
                                ),
                            ) => {
                                break;
                            }
                            None => break,
                        },
                        Ok(None) => break,
                        Err(_) => break,
                    }
                }
                Ok::<(), String>(())
            })();

            let _ = result.await;
        });

        // Wait for sender to finish (runs until duration).
        let _ = sender.await;

        // Wait for receiver to finish (it receives EOF when sender closes tx).
        let _ = receiver.await;

        let pongs = Arc::try_unwrap(pongs).unwrap().into_inner().unwrap();
        (total_pings.load(Ordering::SeqCst), pongs)
    }
}

async fn send_pings(
    tx: mpsc::Sender<BidirectionalStreamRequest>,
    interval: Duration,
    client_id: String,
    duration: Duration,
    total: Arc<AtomicU64>,
) {
    let mut tick = tokio::time::interval(interval);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let deadline = tokio::time::Instant::now() + duration;

    let mut id: i32 = 0;
    loop {
        let sleep_fut = tokio::time::sleep_until(deadline);
        tokio::select! {
            biased;
            _ = sleep_fut => {
                // Duration elapsed.
                id += 1;
                let _ = tx.send(BidirectionalStreamRequest {
                    ping: Some(Ping { id, sender: client_id }),
                }).await;
                total.fetch_add(1, Ordering::SeqCst);
                return;
            }
            _ = tick.tick() => {
                id += 1;
                let _ = tx.send(BidirectionalStreamRequest {
                    ping: Some(Ping { id, sender: client_id.clone() }),
                }).await;
                total.fetch_add(1, Ordering::SeqCst);
            }
        }
    }
}

impl Drop for ServerFixture {
    fn drop(&mut self) {
        if let Some(ref mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}
