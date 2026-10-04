"""Shared fixtures for gRPC integration tests."""

import subprocess
import sys
import tempfile
import time
from pathlib import Path

import grpc
import pytest

from _helpers import read_server_log

PYTHON_DIR = Path(__file__).resolve().parent.parent  # python/

# Make python/ importable (api module etc.)
sys.path.insert(0, str(PYTHON_DIR))


def _find_free_port() -> int:
    """Return a random free TCP port."""
    import socket
    with socket.socket(socket.AF_INET6, socket.SOCK_STREAM) as s:
        s.bind(("::", 0))
        return s.getsockname()[1]


def _wait_for_server(address: str, timeout: float = 10) -> None:
    """Block until the gRPC server accepts connections.

    Uses a simple unary call to confirm the server is reachable.
    """
    from api import resilience_pb2, resilience_pb2_grpc

    stub = resilience_pb2_grpc.ResilienceServiceStub(
        grpc.insecure_channel(address, options=[
            ("grpc.keepalive_time_ms", 1000),
            ("grpc.keepalive_timeout_ms", 500),
        ])
    )

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            def gen():
                yield resilience_pb2.BidirectionalStreamRequest(
                    ping=resilience_pb2.Ping(id=0, sender="_health_check_"),
                )
            # Open a stream briefly just to confirm the connection works
            stream = stub.Stream(gen())
            # Drain the stream (server will just close it)
            for _ in stream:
                pass
            return  # success
        except grpc.RpcError:
            pass
        except Exception:
            pass
        time.sleep(0.25)
    raise RuntimeError(f"Server did not become ready within {timeout}s")


@pytest.fixture(scope="session")
def grpc_server():
    """Start the ResilienceService gRPC server as a subprocess.

    Usage in tests:
        grpc_server.address   # e.g. "localhost:52345"
        grpc_server.log       # full server log (str)
    """
    port = _find_free_port()
    address = f"localhost:{port}"

    tmp_log = tempfile.NamedTemporaryFile(
        mode="w", suffix=".log", delete=False,
    )
    tmp_log.close()  # close so server can write

    server_proc = subprocess.Popen(
        [sys.executable, "server.py", str(port),
         "--ping-interval-ms", "100",
         "--keepalive-time-sec", "10",
         "--keepalive-timeout-sec", "5"],
        cwd=PYTHON_DIR,
        stdout=open(tmp_log.name, "a"),
        stderr=subprocess.STDOUT,
    )
    try:
        _wait_for_server(address)
    except Exception:
        server_proc.kill()
        server_proc.wait()
        raise RuntimeError(f"Server failed to start on port {port}") from None

    # Store data on the fixture object for tests to access
    grpc_server.address = address       # type: ignore[union-attr]
    grpc_server._log_path = tmp_log.name  # type: ignore[union-attr]
    grpc_server._proc = server_proc      # type: ignore[union-attr]

    yield grpc_server

    # Teardown: stop the server
    server_proc.terminate()
    try:
        server_proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        server_proc.kill()
        server_proc.wait()
