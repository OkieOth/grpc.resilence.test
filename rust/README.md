# Rust – ResilienceService gRPC client & server

This directory contains the Rust implementation of the
[ResilienceService](../api/resilience.proto) — a bidirectional streaming gRPC
service designed to test connection resilience under error conditions.

## Quick start

```bash
# 1. Build (regenerates protobuf bindings from api/resilience.proto)
make build

# 2a. Start the server in the background (port 50051)
make server
# 2b. Or in the foreground (blocks until Ctrl-C):
# make server-foreground

# 3. In another terminal, run the client
make client TARGET=localhost:50051

# 4. Stop server / client
make kill
```

## Running integration tests

```bash
make test         # starts a server, runs all tests, cleans up
```

## Architecture

```
api/resilience.proto
        │
   build.rs (tonic-build)  ─────────────────►  OUT_DIR/*.rs
                                               │
            ┌──────────────────────────────────┼─────────────────────────┐
            ▼                                  ▼                         │
  src/bin/server.rs                   src/bin/client.py                  │
  ──────────────                    ─────────────────                  │
  • Sends Pongs continuously        • Sends pings continuously         │
  • Receives client pings           • Receives server Pongs            │
  • Configurable interval           • Configurable interval            │
```

## gRPC implementation steps (Rust)

### 1. Install dependencies

This project uses **Cargo** (Rust's package manager). Install Rust toolchain
(≥ 1.96) and `protoc`:

```bash
rustup update stable
cargo --version    # 1.96+
protoc --version   # 3.x or 4.x
```

### 2. Build (regenerates protobuf bindings)

From the repository root:

```bash
make build
```

`build.rs` invokes `tonic_build::compile_protos` on
`../api/resilience.proto` and places generated Rust modules into Cargo's OUT_DIR.
The types are re-exported via `tonic::include_proto!("resilience")` in
`src/lib.rs` as `crate::api::*`.

**Note:** There is no `make gen` — `build.rs` regenerates the bindings on
every build automatically.

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
| `--keepalive-time-sec` | 10 | HTTP/2 keepalive interval |
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
make client TARGET=localhost:50051            # explicit target
make client TARGET=localhost:50051 CLIENT_ID=alpha PING_INTERVAL_MS=2000  # custom params
```

Custom arguments are available via the command line:

```bash
cd rust && ./target/debug/client localhost:50051 \
    --client-id alpha --ping-interval-ms 200
```

The Rust client uses an **mpsc channel** to send requests: a spawned task ticks
at the configured interval, sending `BidirectionalStreamRequest` through a
channel. The main task calls `client.stream(request)` with a
`ReceiverStream` wrapping the channel receiver, then iterates over the response
stream (a separate `tonic::Streaming<Pong>`) for pongs and errors.

#### Bidirectional streaming model

Both the client and server operate as **bidirectional streaming** endpoints:

1. **Client** — sends Ping messages continuously at its configured interval,
   while the main task iterates over Pong (or Error) responses from the
   server.
2. **Server** — receives client Pings and sends Pong responses at its own
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

The background server writes its log to `../temp/rust/server.log`. Use `cat`
or `tail -f` to monitor it:

```bash
tail -f ../temp/rust/server.log
```

## File layout

```
rust/
├── Makefile                     ← (see root Makefile for rust targets)
├── Cargo.toml                   ← dependencies (tonic, tokio, prost, clap, time)
├── build.rs                     ← tonic_build → protobuf codegen
├── README.md                    ← this file
├── src/
│   ├── lib.rs                   ← re-exports generated API module
│   ├── bin/
│   │   ├── server.rs            ← ResilienceService server (tonic service)
│   │   └── client.rs            ← ResilienceService client (reconnect + signals)
│   └── logfmt.rs                ← Python-compatible log formatter (time crate)
└── tests/
    ├── common/
    │   └── mod.rs               ← ServerFixture, free_port, run_client helpers
    ├── single_client.rs         ← 3 single-client tests
    ├── multi_clients.rs         ← 4 multi-client tests
    ├── quick_connect.rs         ← 1 connectivity smoke test
    └── placeholder.rs           ← placeholder (always passes)
```

## Troubleshooting

* **`protoc not found`** — install it: `brew install protobuf` (macOS) or
  `apt-get install protobuf-compiler` (Linux).
* **Address already in use** — run `make kill` first, then `make server`.
* **Integration test hangs** — the test's `run_client` shares a single
  `tx`/`rx` channel between sender and receiver tasks; the sender writes to
  `tx` and the server reads from `rx` (inside the request's `ReceiverStream`);
  the response stream is separate. When the sender finishes, dropping `tx`
  signals EOF to the server, which closes the response stream.

## Implementation notes

* **Async model:** Uses `tonic 0.12` with `tokio` runtime. Bidirectional
  streaming is handled via `tonic::Streaming` (server side) and
  `tokio_stream::wrappers::ReceiverStream` (client side).
* **Log format:** Uses the `time` crate (with `macros` feature) to match Python's
  `[role] YYYY-MM-DD HH:MM:SS LEVEL message` format exactly, stripping
  timezone offsets via a custom `format_description!` spec.
* **OS signals:** Uses `tokio::signal::ctrl_c()` for graceful shutdown
  (SIGINT/SIGTERM).
* **Health check:** Server-side keepalive is handled by `tonic`'s built-in
  HTTP/2 keepalive (no separate enforcement logic).
* **Test fixtures:** Each test uses a function-scoped `ServerFixture` that
  starts the server as a subprocess, waits for readiness via a health-check
  ping, and tears down on drop (kill + wait).
