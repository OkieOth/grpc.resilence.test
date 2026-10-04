"""Integration test: 1 server + multiple concurrent clients."""

import threading
import time

import grpc
import pytest

from api import resilience_pb2, resilience_pb2_grpc
from _helpers import read_server_log


def _run_client(target: str, client_id: str,
                ping_interval_ms: int = 150, timeout: float = 5.0
                ) -> tuple[int, list[resilience_pb2.Pong]]:
    """Run a single client against the server for *timeout* seconds."""
    pings_sent = 0
    pongs_received: list[resilience_pb2.Pong] = []
    end_time = time.monotonic() + timeout

    def _request_gen():
        nonlocal pings_sent
        pid = 0
        while time.monotonic() < end_time:
            pid += 1
            pings_sent += 1
            yield resilience_pb2.BidirectionalStreamRequest(
                ping=resilience_pb2.Ping(id=pid, sender=client_id))
            time.sleep(ping_interval_ms / 1000.0)

    ch = grpc.insecure_channel(target, options=[
        ("grpc.keepalive_time_ms", 1000),
        ("grpc.keepalive_timeout_ms", 500),
    ])
    stub = resilience_pb2_grpc.ResilienceServiceStub(ch)
    stream = stub.Stream(_request_gen(), metadata=[("client-id", client_id)])
    for resp in stream:
        if resp.HasField("pong"):
            pongs_received.append(resp.pong)
        elif resp.HasField("error"):
            break
    ch.close()
    return pings_sent, pongs_received


def _run_multi_client(target: str, client_id: str,
                      ping_interval_ms: int = 150, timeout: float = 5.0
                      ) -> tuple[threading.Thread, int, list[resilience_pb2.Pong]]:
    """Run a client in a background thread so multiple clients can run concurrently.

    Returns (thread, pings_sent_ref, pongs_ref).
    """
    pings_sent = [0]
    pongs_received: list[resilience_pb2.Pong] = []
    end_time = time.monotonic() + timeout

    def _request_gen():
        pid = 0
        while time.monotonic() < end_time:
            pid += 1
            pings_sent[0] += 1
            yield resilience_pb2.BidirectionalStreamRequest(
                ping=resilience_pb2.Ping(id=pid, sender=client_id))
            time.sleep(ping_interval_ms / 1000.0)

    ch = grpc.insecure_channel(target, options=[
        ("grpc.keepalive_time_ms", 1000),
        ("grpc.keepalive_timeout_ms", 500),
    ])
    stub = resilience_pb2_grpc.ResilienceServiceStub(ch)
    stream = stub.Stream(_request_gen(), metadata=[("client-id", client_id)])
    # Drain responses in a separate loop so the generator keeps yielding
    def _drain():
        for resp in stream:
            if resp.HasField("pong"):
                pongs_received.append(resp.pong)
            elif resp.HasField("error"):
                break

    thread = threading.Thread(target=_drain, daemon=True)
    thread.start()
    return thread, pings_sent, pongs_received


@pytest.fixture
def multi_clients(grpc_server):
    """Three clients running concurrently against the fixture server."""
    results = []
    for i, cid in enumerate(["client_alpha", "client_beta", "client_gamma"]):
        t, pings, pongs = _run_multi_client(
            grpc_server.address,  # type: ignore[attr-defined]
            client_id=cid,
            ping_interval_ms=150,
            timeout=5.0,
        )
        results.append((cid, t, pings, pongs))

    # Wait for all drain threads to complete so pongs are fully collected
    for cid, t, _, _ in results:
        t.join(timeout=10)

    clients = [type("obj", (object,), {
            "channel": None,  # no channel to close, drain thread owns it
            "pongs_received": pongs,
            "client_id": cid,
            "pings_sent": pings[0],  # captured snapshot
        })() for cid, t, pings, pongs in results]

    yield clients


def test_multi_clients_all_receive_pongs(multi_clients):
    """Every connected client receives pongs from the server."""
    for client in multi_clients:
        assert len(client.pongs_received) > 0, (
            f"{client.client_id} received 0 server pongs "
            f"(sent {client.pings_sent} pings)"
        )
        for pong in client.pongs_received:
            assert pong.sender == "server"


def test_multi_clients_distinguish_streams(grpc_server, multi_clients):
    """The server logs pings from each client with the correct ID."""
    # Wait a few seconds for pings to accumulate
    time.sleep(3.0)

    server_log = read_server_log(grpc_server)

    for client in multi_clients:
        ping_count = sum(
            1 for line in server_log.splitlines()
            if client.client_id in line and "Server received ping" in line
        )
        assert ping_count > 0, (
            f"Server did not log any pings from {client.client_id}.\n"
            f"Server log excerpt:\n{server_log[-800:]}"
        )


def test_multi_clients_total_pongs_exceed_single(multi_clients):
    """With 3 clients, every client receives pongs (total > 0)."""
    total_pongs = sum(len(c.pongs_received) for c in multi_clients)
    assert total_pongs > 0, "No pongs received from any client"

    per_client_pongs = [len(c.pongs_received) for c in multi_clients]
    assert all(p > 0 for p in per_client_pongs), (
        f"Not all clients received pongs: {per_client_pongs}"
    )


def test_multi_clients_sees_monotonic_pongs(multi_clients):
    """Verify that each client's pongs have monotonically increasing IDs."""
    for client in multi_clients:
        if len(client.pongs_received) > 1:
            ids = [p.id for p in client.pongs_received]
            assert ids == sorted(ids), (
                f"{client.client_id} saw non-monotonic pong IDs: {ids}"
            )
