//! Single-client integration tests.
//! Mirrors the Python tests in test_single_client.py.

mod common;

use std::time::Duration;

use common::ServerFixture;

/// Test 1: single client sends pings, receives pongs with server as sender.
#[tokio::test]
async fn test_single_client_ping_pong() {
    let fixture = ServerFixture::new().expect("failed to start server");
    ServerFixture::wait_for_server(&fixture.address)
        .await
        .unwrap();

    let (pings_sent, pongs) = fixture.run_client("test_client_0", 100, 5.0).await;

    assert!(
        pongs.len() > 0,
        "Expected >0 pongs received, got {} (pings sent={})",
        pongs.len(),
        pings_sent
    );

    for pong in &pongs {
        assert_eq!(
            pong.sender, "server",
            "All pongs should have sender == 'server'"
        );
        assert!(pong.id > 0, "All pong ids should be > 0, got {}", pong.id);
    }
}

/// Test 2: single client count — verify >0 pings sent and >5 pongs over 5s.
#[tokio::test]
async fn test_single_client_count_pings_pongs() {
    let fixture = ServerFixture::new().expect("failed to start server");
    ServerFixture::wait_for_server(&fixture.address)
        .await
        .unwrap();

    let (pings_sent, pongs) = fixture.run_client("test_client_1", 100, 5.0).await;

    assert!(
        pings_sent > 0,
        "Expected pings sent > 0, got {}",
        pings_sent
    );
    assert!(
        pongs.len() > 5,
        "Expected >5 pongs over 5s (client 100ms interval, server 100ms), got {}",
        pongs.len()
    );
}

/// Test 3: verify server log contains expected "Server received ping" lines.
#[tokio::test]
async fn test_single_client_server_receives_pings() {
    let fixture = ServerFixture::new().expect("failed to start server");
    ServerFixture::wait_for_server(&fixture.address)
        .await
        .unwrap();

    // Run the test client for 2 seconds.
    let _ = fixture.run_client("test_client_0", 100, 2.0).await;

    // Wait for server to finish processing (settle time).
    tokio::time::sleep(Duration::from_secs(2)).await;

    let log = fixture.read_log();

    let matching_lines: Vec<&str> = log
        .lines()
        .filter(|line| line.contains("Server received ping") && line.contains("test_client_0"))
        .collect();

    assert!(
        !matching_lines.is_empty(),
        "Server log should contain lines with 'Server received ping' and 'test_client_0', got:\n{}",
        log.lines().rev().take(500).collect::<String>()
    );
}
