//! Multi-client integration tests.
//! Mirrors the Python tests in test_multi_clients.py.

mod common;

use std::time::Duration;

use common::ServerFixture;

/// Run three concurrent clients and return their results.
async fn run_three_clients(
    fixture: &ServerFixture,
) -> Vec<(String, u64, Vec<resilience_rust::api::Pong>)> {
    let alpha = fixture.run_client("client_alpha", 150, 5.0);
    let beta = fixture.run_client("client_beta", 150, 5.0);
    let gamma = fixture.run_client("client_gamma", 150, 5.0);

    let (alpha_result, (beta_pings, beta_pongs), (gamma_pings, gamma_pongs)) =
        tokio::join!(alpha, beta, gamma);

    vec![
        ("client_alpha".to_string(), alpha_result.0, alpha_result.1),
        ("client_beta".to_string(), beta_pings, beta_pongs),
        ("client_gamma".to_string(), gamma_pings, gamma_pongs),
    ]
}

/// Test 4: all three clients receive pongs.
#[tokio::test]
async fn test_multi_clients_all_receive_pongs() {
    let fixture = ServerFixture::new().expect("failed to start server");
    ServerFixture::wait_for_server(&fixture.address)
        .await
        .unwrap();

    let clients = run_three_clients(&fixture).await;

    for (client_id, _pings, pongs) in &clients {
        assert!(
            pongs.len() > 0,
            "Client '{}' should receive >0 pongs, got {}",
            client_id,
            pongs.len()
        );
        for pong in pongs {
            assert_eq!(
                pong.sender, "server",
                "Client '{}' pong sender should be 'server'",
                client_id
            );
        }
    }
}

/// Test 5: server log shows pings from all three clients.
#[tokio::test]
async fn test_multi_clients_distinguish_streams() {
    let fixture = ServerFixture::new().expect("failed to start server");
    ServerFixture::wait_for_server(&fixture.address)
        .await
        .unwrap();

    // Run all three clients simultaneously.
    let _clients = run_three_clients(&fixture).await;

    // Wait for server to finish processing (settle time).
    tokio::time::sleep(Duration::from_secs(3)).await;

    let log = fixture.read_log();

    for client_id in ["client_alpha", "client_beta", "client_gamma"] {
        let matching_lines: Vec<&str> = log
            .lines()
            .filter(|line| line.contains("Server received ping") && line.contains(client_id))
            .collect();

        assert!(
            !matching_lines.is_empty(),
            "Server log should have 'Server received ping' lines for '{}', got:\n{}",
            client_id,
            log.lines().rev().take(800).collect::<String>()
        );
    }
}

/// Test 6: total pongs across all clients > 0 and every client > 0.
#[tokio::test]
async fn test_multi_clients_total_pongs_exceed_single() {
    let fixture = ServerFixture::new().expect("failed to start server");
    ServerFixture::wait_for_server(&fixture.address)
        .await
        .unwrap();

    let clients = run_three_clients(&fixture).await;

    let total_pongs: u64 = clients.iter().map(|(_, _, pongs)| pongs.len() as u64).sum();

    assert!(
        total_pongs > 0,
        "Total pongs should be > 0, got {}",
        total_pongs
    );
    for (client_id, _, pongs) in &clients {
        assert!(
            pongs.len() > 0,
            "Every client should receive > 0 pongs; '{}' got {}",
            client_id,
            pongs.len()
        );
    }
}

/// Test 7: per-client, received pong ids are non-decreasing (monotonic).
#[tokio::test]
async fn test_multi_clients_sees_monotonic_pongs() {
    let fixture = ServerFixture::new().expect("failed to start server");
    ServerFixture::wait_for_server(&fixture.address)
        .await
        .unwrap();

    let clients = run_three_clients(&fixture).await;

    for (client_id, _, pongs) in &clients {
        let ids: Vec<i32> = pongs.iter().map(|p| p.id).collect();
        for window in ids.windows(2) {
            assert!(
                window[0] <= window[1],
                "Client '{}' received non-monotonic pong ids: {:?}, every stream has its own counter",
                client_id, ids
            );
        }
    }
}
