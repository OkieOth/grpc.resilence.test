#!/usr/bin/env python3
"""ResilienceService gRPC server – bidirectional streaming ping/pong."""

import argparse
import logging
import signal
import sys
import time

import grpc
from api import resilience_pb2
from api import resilience_pb2_grpc

logging.basicConfig(
    level=logging.INFO,
    format="[server] %(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("server")


class ResilienceServicer(resilience_pb2_grpc.ResilienceServiceServicer):
    """Handles a single bidirectional ping/pong stream.

    The server iterates over client pings (requests) and yields Pong
    responses.  It sends Pongs at its own configured interval, which may
    differ from the client's ping interval.
    """

    def Stream(self, request_iterator, context):
        """Receive client pings and send Pong responses."""
        server_ping_id = 0
        last_send_time = 0.0

        for req in request_iterator:
            ping_id = req.ping.id
            sender = req.ping.sender
            log.info("Server received ping #%d from %s", ping_id, sender)

            # Respect the server's configured send interval.
            now = time.time()
            elapsed = now - last_send_time
            if elapsed >= self.ping_interval_s:
                server_ping_id += 1
                last_send_time = now
                log.info("Server sending Pong #%d", server_ping_id)
                yield resilience_pb2.BidirectionalStreamResponse(
                    pong=resilience_pb2.Pong(id=server_ping_id, sender="server")
                )
            else:
                log.debug(
                    "Skipping response (elapsed=%.3fs < %fs)",
                    elapsed,
                    self.ping_interval_s,
                )

        log.info("Connection closed (peer=%s)", self._peer(context))

    @staticmethod
    def _peer(context) -> str:
        try:
            return context.peer()
        except Exception:
            return "<unknown>"


def serve(
    port: int = 50051,
    ping_interval_ms: int = 500,
    keepalive_time_sec: int = 10,
    keepalive_timeout_sec: int = 5,
) -> None:
    from concurrent import futures

    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=10),
        options=[
            ("grpc.keepalive_time_ms", keepalive_time_sec * 1000),
            ("grpc.keepalive_timeout_ms", keepalive_timeout_sec * 1000),
            ("grpc.keepalive_permit_without_calls", True),
        ],
    )
    servicer = ResilienceServicer()
    servicer.ping_interval_s = ping_interval_ms / 1000.0
    resilience_pb2_grpc.add_ResilienceServiceServicer_to_server(servicer, server)
    server.add_insecure_port(f"[::]:{port}")
    server.start()
    log.info(
        "Resilience gRPC server listening on port %d (app ping %d ms, keepalive %ds)",
        port,
        ping_interval_ms,
        keepalive_time_sec,
    )
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        log.info("Server shutting down.")
        server.stop(grace=0).wait()


def parse_args():
    parser = argparse.ArgumentParser(description="ResilienceService gRPC server")
    parser.add_argument("port", nargs="?", type=int, default=50051,
                        help="Port to listen on (default: 50051)")
    parser.add_argument("--ping-interval-ms", type=int, default=500,
                        help="Interval between application-level Pong messages (default: 500)")
    parser.add_argument("--keepalive-time-sec", type=int, default=10,
                        help="gRPC keepalive time in seconds – sends HTTP/2 PING frames (default: 10)")
    parser.add_argument("--keepalive-timeout-sec", type=int, default=5,
                        help="gRPC keepalive timeout in seconds – wait for PING ack (default: 5)")
    args = parser.parse_args()
    return (args.port,
            args.ping_interval_ms,
            args.keepalive_time_sec,
            args.keepalive_timeout_sec)


if __name__ == "__main__":
    serve(*parse_args())
