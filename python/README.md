# Python – ResilienceService gRPC client & server

This directory contains the Python implementation of the
[ResilienceService](../api/resilience.proto) – a bidirectional streaming gRPC
service designed to test connection resilience under error conditions.

## Quick start

```bash
# 1. Generate protobuf / gRPC bindings (from python/ directory)
make gen

# 2a. Start the server in the background (port 50051)
make server
# 2b. Or in the foreground (blocks until Ctrl-C):
# make server-foreground   (also aliased as `make run`)

# 3. In another terminal, run the client
make client TARGET=localhost:50051

# 4. Stop server / client
make kill
```

## Running integration tests

```bash
make test         # starts a server, runs all tests, cleans up
make test TEST_TIMEOUT=120  # longer timeout per test
```

## Architecture

```
api/resilience.proto
        │
   make gen  ─────────────────────►  python/api/resilience_pb2.py
                                     python/api/resilience_pb2_grpc.py
                                         │
            ┌────────────────────────────┼─────────────────────────┐
            ▼                            ▼                         │
  python/server.py              python/client.py                   │
  ──────────────              ─────────────────                  │
  • Sends Pongs continuously  • Sends pings continuously         │
  • Receives client pings     • Receives server Pongs            │
  • Configurable interval     • Configurable interval            │
```

## gRPC implementation steps (Python)

### 1. Install dependencies

This project uses **[uv](https://github.com/astral-sh/uv)** for dependency and
virtual-environment management.  A `pyproject.toml` in the `python/` directory
declares the required packages:

```toml
[project]
dependencies = [
    "grpcio",
    "grpcio-tools",
    "protobuf",
]
```

Run the following to create (or re-create) the virtual environment and install
dependencies:

```bash
uv sync --project python/
```

### 2. Generate protobuf / gRPC bindings

From the repository root, run:

```bash
make gen
```

This invokes `grpc_tools.protoc` on `api/resilience.proto` and produces:

| File | Description |
|---|---|
| `python/api/resilience_pb2.py` | Message classes (Ping, Pong, Error, …) |
| `python/api/resilience_pb2_grpc.py` | Stub and servicer classes |

**Note:** The generated gRPC stub uses `channel.unary_unary()` by default for
non-streaming RPCs.  For bidirectional streaming (`stream … returns (stream …)`),
the tool generates `channel.stream_stream()` which accepts a request iterator
(generator in Python) and returns a response iterator.

### 3. Run the server

```bash
make server
```

The server listens on port **50051**.  It sends Pong messages at a configurable
interval while processing incoming client pings:

| Argument | Default | Meaning |
|---|---|---|
| port (positional) | 50051 | Port to listen on |
| `--ping-interval-ms` | 500 | Milliseconds between application-level Pong messages |
| `--keepalive-time-sec` | 10 | gRPC transport-level keepalive interval (HTTP/2 PING frames) |
| `--keepalive-timeout-sec` | 5 | Keepalive timeout – seconds to wait for PING ack |

The server:

1. Establishes a bidirectional stream with the client.
2. Receives client pings and logs each one.
3. Sends Pong messages at its configured interval and logs each send.

When the client disconnects, the stream closes and the server logs the
connection closed event.

### 4. Run the client

The client connects to a gRPC server, opens a bidirectional stream, and sends
Pings at a configurable interval while receiving server Pongs.

```bash
make client TARGET=localhost:50051          # explicit target
make client TARGET=localhost:50051 CLIENT_ID=alpha PING_INTERVAL_MS=2000  # custom params
```

Custom arguments are available via the command line:

```bash
uv run --project python/ \
    python client.py localhost:50051 \
    --ping-interval-ms 200
```

The client uses a **generator pattern** (`yield` in Python) to send requests
over the bidirectional stream:

```python
def _request_generator(ping_interval_s):
    ping_id = 0
    while True:
        ping_id += 1
        log.info("Client sending ping #%d", ping_id)
        yield BidirectionalStreamRequest(
            ping=Ping(id=ping_id, sender="client")
        )
        time.sleep(ping_interval_s)

stream = stub.Stream(_request_generator(0.5))
for response in stream:
    # handle pong or error
```

#### Bidirectional streaming model

Both the client and server operate as **bidirectional streaming** endpoints:

1. **Client** – sends Ping messages continuously at its configured interval,
   while the main thread iterates over Pong (or Error) responses from the
   server.
2. **Server** – receives client Pings and sends Pong responses at its own
   configured interval (independent of the client's interval).

Both endpoints log every send and every receive operation.  When the server
closes, the client detects the stream termination and attempts reconnection
(up to 10 retries with 1 s delay).

### 5. Stop the server / client

```bash
make kill
```

This stops all running server and client processes (by process matching with
`pkill`).

## Server log

The background server writes its log to `../temp/server.log` (relative to the
`python/` directory). Use `cat` or `tail -f` to monitor it:

```bash
tail -f ../temp/server.log
```

## File layout

```
python/
├── Makefile                     ← project-level Makefile (gRPC stub gen, server, client, kill)
├── pyproject.toml               ← uv dependencies
├── README.md                    ← this file
├── server.py                    ← ResilienceService server
├── client.py                    ← ResilienceService client
└── api/
    ├── __init__.py
    ├── resilience_pb2.py        ← generated message classes
    └── resilience_pb2_grpc.py   ← generated stubs & servicers
```

## Troubleshooting

* **`ModuleNotFoundError: No module named 'api'`** – make sure you run from the
  repository root so `python/` is on `sys.path` (or use `uv run --project
  python/`).
* **Address already in use** – run `make kill` first, then `make server`.
* **Stub / servicer not found** – run `make gen` to regenerate bindings.
* **TypeError: _StreamStreamMultiCallable missing request_iterator** – ensure
  the proto defines `rpc Stream(stream …) returns (stream …)` (bidirectional
  streaming), not unary-unary.
