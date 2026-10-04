"""Integration test: 1 server + 1 client exchanging pings/pongs."""

import time

import grpc
import pytest

from api import resilience_pb2, resilience_pb2_grpc
from _helpers import read_server_log


def _run_client(target: str, client_id: str,
                ping_interval_ms: int = 100, timeout: float = 5.0
                ) -> tuple[int, list[resilience_pb2.Pong]]:
    """Run a single client against the server for *timeout* seconds.

    Returns (pings_sent, pongs_received).
    """
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


@pytest.fixture
def single_client(grpc_server):
    """One client running against the fixture server."""
    pings, pongs = _run_client(
        grpc_server.address,  # type: ignore[attr-defined]
        client_id="test_client_0",
        ping_interval_ms=100,
        timeout=5.0,
    )
    yield type("obj", (object,), {
        "pongs_received": pongs,
        "pings_sent": pings,
    })()


def test_single_client_ping_pong(single_client):
    """The server responds to client pings with application-level pongs."""
    assert len(single_client.pongs_received) > 0, (
        f"Expected server pongs but received none "
        f"(pings sent = {single_client.pings_sent})"
    )
    for pong in single_client.pongs_received:
        assert pong.sender == "server"
        assert pong.id > 0


def test_single_client_count_pings_pongs(single_client):
    """The number of pongs received is roughly proportional to test duration."""
    pings = single_client.pings_sent
    pongs = len(single_client.pongs_received)
    assert pings > 0, "Client should have sent at least one ping"
    # Server sends a pong every 100ms (matching client); over 5s we expect ~50.
    assert pongs > 5, (
        f"Expected >5 server pongs over 5s (got {pongs} pongs from {pings} pings)"
    )


def test_single_client_server_receives_pings(grpc_server, single_client):
    """The server logs each incoming ping with the correct client ID."""
    # Wait a couple seconds for pings to accumulate
    time.sleep(2.0)

    server_log = read_server_log(grpc_server)
    # The server outputs: "Server received ping #N from test_client_0"
    ping_from_client = sum(1 for line in server_log.splitlines()
                           if "test_client_0" in line and "Server received ping" in line)
    assert ping_from_client > 0, (
        f"Server did not log any pings from test_client_0.\n"
        f"Server log excerpt:\n{server_log[-500:]}"
    )
