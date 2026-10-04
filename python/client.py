#!/usr/bin/env python3
"""ResilienceService gRPC client – bidirectional streaming ping/pong."""

import argparse
import logging
import signal
import sys
import time
from typing import Generator

import grpc
from api import resilience_pb2
from api import resilience_pb2_grpc

logging.basicConfig(
    level=logging.INFO,
    format="[client] %(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("client")

MAX_RETRIES = 10
RECONNECT_DELAY_S = 1.0


class ResilienceClient:
    """Manages a single gRPC channel with bidirectional ping/pong streaming."""

    def __init__(self, target: str, client_id: str):
        self.target = target
        self.client_id = client_id

    def run(self, ping_interval_ms: int, keepalive_time_sec: int = 10, keepalive_timeout_sec: int = 5) -> None:
        """Main loop: connect, send/receive pings, reconnect on failure.

        Uses gRPC keepalive (HTTP/2 PING frames) to detect dead connections
        and keep NAT/firewall mappings alive, independent of application
        traffic.
        """
        ping_interval_s = ping_interval_ms / 1000.0
        attempt = 0
        total_pings_sent = 0
        total_pong_received = 0

        # Keepalive options: send HTTP/2 PING frames at the transport level.
        # These operate below the application messages and detect dead peers
        # even when no application data is flowing.
        keepalive_options = [
            ("grpc.keepalive_time_ms", keepalive_time_sec * 1000),
            ("grpc.keepalive_timeout_ms", keepalive_timeout_sec * 1000),
        ]

        while True:
            attempt += 1
            if attempt > 1:
                log.info("Reconnecting (attempt %d/%d) …", attempt, MAX_RETRIES)
                time.sleep(RECONNECT_DELAY_S)

            try:
                channel = grpc.insecure_channel(self.target, options=keepalive_options)
                stub = resilience_pb2_grpc.ResilienceServiceStub(channel)

                try:
                    stream = stub.Stream(
                        self._request_generator(ping_interval_s),
                        metadata=[("client-id", self.client_id)],
                    )
                    for response in stream:
                        if response.HasField("pong"):
                            log.info("Client received Pong #%d from %s",
                                     response.pong.id, response.pong.sender)
                            total_pong_received += 1
                        elif response.HasField("error"):
                            log.warning("Client received Error #%d: code=%s msg=%s",
                                        response.error.id, response.error.code,
                                        response.error.message)
                            break  # stream closed after error
                except grpc.RpcError as exc:
                    log.error("RPC error (attempt %d): %s – %s", attempt, exc.code(), exc.details())
                finally:
                    channel.close()

            except Exception as exc:
                log.error("Connection error (attempt %d): %s", attempt, exc)

            if attempt >= MAX_RETRIES:
                log.info("Max retries (%d) reached.", MAX_RETRIES)
                break

        log.info(
            "Done. Attempt=%d  Pings sent=%d  Pongs received=%d",
            attempt,
            total_pings_sent,
            total_pong_received,
        )

    def _request_generator(
        self, ping_interval_s: float
    ) -> Generator[resilience_pb2.BidirectionalStreamRequest, None, None]:
        """Yields Ping requests at the configured interval."""
        ping_id = 0
        while True:
            ping_id += 1
            log.info("Client sending ping #%d", ping_id)
            yield resilience_pb2.BidirectionalStreamRequest(
                ping=resilience_pb2.Ping(id=ping_id, sender=self.client_id)
            )
            time.sleep(ping_interval_s)


def main() -> None:
    parser = argparse.ArgumentParser(description="ResilienceService gRPC client")
    parser.add_argument("target", nargs="?", default="localhost:50051",
                        help="gRPC target (default: localhost:50051)")
    parser.add_argument("--client-id", default="client",
                        help="Identifier used in Ping messages so the server can distinguish clients (default: client)")
    parser.add_argument("--ping-interval-ms", type=int, default=500,
                        help="Interval between application-level Ping messages (default: 500)")
    parser.add_argument("--keepalive-time-sec", type=int, default=10,
                        help="gRPC keepalive time in seconds – sends HTTP/2 PING frames (default: 10)")
    parser.add_argument("--keepalive-timeout-sec", type=int, default=5,
                        help="gRPC keepalive timeout in seconds – wait for PING ack (default: 5)")
    args = parser.parse_args()

    client = ResilienceClient(args.target, args.client_id)

    def _handle_signal(signum, _frame):
        log.info("Signal %d received, shutting down …", signum)
        sys.exit(0)

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    try:
        client.run(
            args.ping_interval_ms,
            args.keepalive_time_sec,
            args.keepalive_timeout_sec,
        )
    except KeyboardInterrupt:
        log.info("Shutting down.")


if __name__ == "__main__":
    main()
