# ResilienceService — Go Implementation

A Go implementation of the gRPC ResilienceService — bidirectional streaming
ping/pong for testing connection resilience.

## Quick Start

### 1. Generate gRPC bindings (one-time, after proto changes)

```bash
cd go && make gen
```

> The generated code (`api/resilience.pb.go`, `api/resilience_grpc.pb.go`)
> is committed, so a fresh clone builds without `protoc`.

### 2. Build

```bash
make build
```

### 3. Start the server

```bash
make server              # background, log → ../temp/go/server.log
make server-foreground   # foreground (blocks)
```

### 4. Run a client

```bash
make client TARGET=localhost:50051

# with custom options:
make client TARGET=localhost:50051 CLIENT_ID=alpha PING_INTERVAL_MS=2000
```

### 5. Stop processes

```bash
make kill-server   # stop only the server
make kill-client   # stop only the client
make kill          # stop both
```

### 6. Run integration tests

```bash
make test
```

---

## API

The protobuf definition is at `../api/resilience.proto`. It defines:

```protobuf
service ResilienceService {
  rpc Stream(stream BidirectionalStreamRequest)
      returns (stream BidirectionalStreamResponse);
}

message BidirectionalStreamRequest {
  Ping ping = 1;
}

message BidirectionalStreamResponse {
  oneof payload {
    Pong pong = 1;
    Error  error = 2;
  }
}

message Ping  { int32 id = 1; string sender = 2; }
message Pong  { int32 id = 1; string sender = 2; }
message Error { int32 id = 1; string message = 2; int32 code = 3; }
```

Both client and server maintain per-stream **bidirectional streams**.
The client sends `Ping` messages at a configurable interval; the server
responds with `Pong` messages (also interval-gated). The client's `sender`
field identifies it; the server echoes `sender="server"` in its pongs.

Each client gets its **own per-stream pong counter** (starts at 0, increments
per stream, not globally).

## Arguments

### Server

| Flag                         | Default | Description                      |
| ---------------------------- | ------- | -------------------------------- |
| `<port>` (positional)        | 50051   | TCP port to listen on            |
| `--ping-interval-ms`         | 500     | Server pong send interval (ms)   |
| `--keepalive-time-sec`       | 10      | HTTP/2 keepalive time (seconds)  |
| `--keepalive-timeout-sec`    | 5       | HTTP/2 keepalive timeout (seconds) |

**Both flag-before-positional and positional-before-flag orderings are
supported** (e.g. `server 50051 --ping-interval-ms 100` and
`server --ping-intival-ms 100 50051`).

### Client

| Flag                         | Default    | Description                        |
| ---------------------------- | ---------- | ---------------------------------- |
| `<target>` (positional)      | `localhost:50051` | Server address             |
| `--client-id`                | `client`   | Client identifier (used in metadata) |
| `--ping-interval-ms`         | 500        | Ping send interval (ms)            |
| `--keepalive-time-sec`       | 10         | HTTP/2 keepalive time (seconds)    |
| `--keepalive-timeout-sec`    | 5          | HTTP/2 keepalive timeout (seconds) |

## Architecture

```
api/resilience.proto
        │
        ▼  (protoc → go)
api/resilience.pb.go         ← protobuf generated
api/resilience_grpc.pb.go    ← gRPC stub generated
        │
        ├─→ cmd/server/main.go  ← ResilienceService implementation
        └─→ cmd/client/main.go  ← reconnect loop + sender/receiver
```

## Implementation Notes

### Keepalive enforcement policy

grpc-go's default server-side `EnforcementPolicy` has `MinTime = 5 min`
and `PermitWithoutStream = false`. This tears down test clients whose
keepalive is shorter (1 s / 500 ms). The server explicitly configures:

```go
grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
    MinTime:             1 * time.Second,  // ≤ keepalive time
    PermitWithoutStream: true,
})
```

### Sender goroutine vs. channel-based sender

The Python server uses a generator (`def _send_pongs(): yield ...`).
The Go server sends pongs **inside the receive loop** (same approach as
Python: interval-gated on `time.Since(lastSend)`), avoiding an independent
ticker goroutine.

### Signal handling

Python uses `signal.signal()` + `try/finally`.  Go uses
`signal.NotifyContext()` for the server (graceful shutdown) and a separate
`signal.Notify()` goroutine in the client that cancels the active attempt's
context and exits cleanly (exit code 0).

### Ping counting fix

The Python client has a bug where `total_pings_sent` is never incremented
(always prints 0). The Go client tracks pings correctly using `atomic.Int64`.

## File Layout

```
go/
├── .gitignore
├── Makefile
├── README.md
├── go.mod / go.sum
├── api/
│   ├── resilience.pb.go
│   └── resilience_grpc.pb.go
├── cmd/
│   ├── server/main.go
│   └── client/main.go
└── tests/
    ├── helpers_test.go
    ├── server_test.go
    ├── single_client_test.go
    └── multi_clients_test.go
```

## Server Log Location

```bash
tail -f ../temp/go/server.log
```

## Troubleshooting

- **"server did not become ready within 10s"** — The server may have failed
  to start. Check `../temp/go/server.log` for the server's startup message
  and error output.
- **port already in use** — Kill any leftover server: `make kill-server`
- **tests fail on first run** — Ensure the binary is built: `make build`
  (test auto-builds from the pre-built `bin/server` when available).
