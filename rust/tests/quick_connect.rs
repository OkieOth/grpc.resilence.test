//! Quick test to verify server can start and be reached.
use std::fs::File;
use std::process::Command;
use std::time::Duration;

use resilience_rust::api::resilience_service_client::ResilienceServiceClient;
use resilience_rust::api::{BidirectionalStreamRequest, Ping};
use tokio::sync::mpsc;
use tonic::codegen::tokio_stream;
use tonic::metadata::MetadataMap;
use tonic::transport::Channel;

#[tokio::test]
async fn test_server_starts_and_reachable() {
    // Get the server binary path at runtime
    let server_path = std::env::var("CARGO_BIN_EXE_server")
        .unwrap_or_else(|_| "./target/debug/server".to_string());

    // Find free port, drop listener immediately to release the port.
    let temp_listener = std::net::TcpListener::bind("[::]:0").unwrap();
    let port = temp_listener.local_addr().unwrap().port();
    drop(temp_listener); // Release the port immediately

    let client_address = format!("localhost:{}", port);

    // Start server
    let log_path = format!("/tmp/integration_test_{}.log", port);
    let mut child = Command::new(&server_path)
        .arg(port.to_string())
        .arg("--ping-interval-ms")
        .arg("100")
        .stdout(File::create(&log_path).unwrap())
        .spawn()
        .expect("Failed to start server");

    // Wait for server to be ready
    let result = (|| async {
        let mut retry = 0;
        loop {
            match Channel::from_shared(format!("http://{client_address}"))
                .map_err(|e| format!("bad url: {e}"))?
                .connect()
                .await
                .map_err(|e| format!("connect: {e}"))
            {
                Ok(ch) => return Ok(ch),
                Err(e) => {
                    retry += 1;
                    if retry > 20 {
                        return Err(e);
                    }
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
            }
        }
    })()
    .await
    .unwrap();

    let mut client = ResilienceServiceClient::new(result);

    let (tx, rx) = mpsc::channel(10);
    let mut request = tonic::Request::new(tokio_stream::wrappers::ReceiverStream::new(rx));
    let mut metadata = MetadataMap::new();
    metadata.insert("client-id", "_test_".parse().unwrap());
    *request.metadata_mut() = metadata;

    // Send one ping, then close sender so server sees EOF and closes response.
    let _ = tx
        .send(BidirectionalStreamRequest {
            ping: Some(Ping {
                id: 0,
                sender: "_test_".to_string(),
            }),
        })
        .await;
    drop(tx);

    let response = client.stream(request).await.unwrap();
    let mut inner = response.into_inner();

    // Expect exactly one pong, then EOF.
    match inner.message().await.unwrap() {
        Some(resp) => assert!(
            matches!(
                &resp.payload,
                Some(resilience_rust::api::bidirectional_stream_response::Payload::Pong(_))
            ),
            "Expected pong, got {:?}",
            resp.payload
        ),
        None => panic!("Expected one message from server, got EOF immediately"),
    }

    // Second message should be EOF (sender dropped, server closes response).
    assert!(
        inner.message().await.unwrap().is_none(),
        "Expected EOF after pong"
    );

    // Kill the server
    let _ = child.kill();
    let _ = child.wait();

    // Read log
    let _log = std::fs::read_to_string(&log_path).unwrap_or_default();
}
