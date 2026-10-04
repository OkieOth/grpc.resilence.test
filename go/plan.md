# Go Migration Plan — ResilienceService

This plan migrates the Python implementation of the gRPC resilience test to
Go. The Go code must provide the **same CLI interface** and the **same
integration tests** as the Python implementation in `../python/`.

The steps below are executed **in sequence, each in a fresh context**. Every
step is self-contained: it lists the files to read first, the exact
deliverables, implementation gotchas, verification commands, and a
definition of done. Do not skip a step's verification before starting the
next.

Current state: `go/go.mod` exists (module `github.com/example/grpc-resilience`,
`go 1.23`, requires `google.golang.org/grpc v1.63.2`,
`google.golang.org/protobuf v1.33.2`,
`google.golang.org/genproto/googleapis/rpc`). Nothing else exists under `go/`
yet.

---

## Ground truth: Python behavior to replicate

Read `../python/server.py`, `../python/client.py`, `../python/tests/`,
`../python/Makefile`, and `api/resilience.proto` for the full picture. This
section is the condensed contract the Go code must match.

### API

`api/resilience.proto` (package `resilience`) defines
`ResilienceService.Stream(stream BidirectionalStreamRequest) returns (stream
BidirectionalStreamResponse)`. Requests carry a `Ping{id, sender}`; responses
carry a oneof of `Pong{id, sender}` or `Error{id, message, code}`. The proto
already declares `option go_package = "github.com/example/grpc-resilience/api"`
which matches the Go module, so generated files belong in `go/api/`.

### CLI surface (must be identical)

| Command  | Positional arg (default) | Flags |
|----------|--------------------------|-------|
| server   | `port` (50051)           | `--ping-interval-ms` (500), `--keepalive-time-sec` (10), `--keepalive-timeout-sec` (5) |
| client   | `target` (localhost:50051) | `--client-id` (client), `--ping-interval-ms` (500), `--keepalive-time-sec` (10), `--keepalive-timeout-sec` (5) |

Both commands must support `--help` with sensible usage text.
**Gotcha:** Go's `flag` package stops parsing at the first non-flag argument,
but the Python CLI (argparse) — and the Makefile — pass the positional
argument *before* flags (e.g. `server 50051 --ping-interval-ms 100`). The Go
code must normalize `os.Args` before `flag.Parse()` (move tokens starting
with `-` in front of bare tokens) so both orderings work.

### Server behavior & log strings

Log format (must match Python): `[server] YYYY-MM-DD HH:MM:SS LEVEL message`
(layout `2006-01-02 15:04:05`, levels `INFO`/`WARNING`/`ERROR`; Python uses
`WARNING`). Message templates, verbatim:

- `Resilience gRPC server listening on port %d (app ping %d ms, keepalive %ds)`
- `Server received ping #%d from %s` (INFO)
- `Server sending Pong #%d to %s` (INFO)
- `Skipping response (elapsed=%.3fs < %fs)` (DEBUG — not visible at INFO level; do not emit at INFO)
- `Connection lost (client=%s, peer=%s, reason=%s)` (WARNING)
- `Connection closed (client=%s, peer=%s)` (INFO)
- `Server shutting down.` (INFO)

Behavior:
- Bind an **insecure** listener on `[::]:<port>`.
- Each stream: read the `client-id` key from incoming gRPC metadata; for every
  received ping log it; send a Pong (`sender="server"`, per-stream incrementing
  id starting at 0→1) **gated by the configured send interval** — i.e. Pongs
  are sent inside the receive loop only when `elapsed >= ping_interval`
  (same as Python; do *not* add an independent ticker).
- Configured keepalive: time = `--keepalive-time-sec`, timeout =
  `--keepalive-timeout-sec`, permit without calls.
  **Critical gotcha:** grpc-go's default server-side `EnforcementPolicy`
  (`MinTime` = 5 min, `PermitWithoutCalls` = false) will tear down clients
  whose keepalive is shorter (the integration tests use 1s/500ms). You must
  set `grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{MinTime: <≤ keepalive time, e.g. 1s>, PermitWithoutCalls: true})`.
- Graceful shutdown on SIGINT/SIGTERM (Python reacts to Ctrl-C, `stop()`).

### Client behavior & log strings

Log format: `[client] YYYY-MM-DD HH:MM:SS LEVEL message`. Message templates,
verbatim (note the unicode ellipsis `…` and en-dash `–`):

- `Reconnecting (attempt %d/10) …` (INFO, only for attempts > 1, logged before the 1s delay)
- `Client sending ping #%d` (INFO)
- `Client received Pong #%d from %s` (INFO)
- `Client received Error #%d: code=%d msg=%s` (WARNING; on error the stream is treated as closed)
- `RPC error (attempt %d): %s – %s` (ERROR; code name, then details)
- `Connection error (attempt %d): %s` (ERROR)
- `Max retries (10) reached.` (INFO)
- `Done. Attempt=%d  Pings sent=%d  Pongs received=%d` (INFO; note double space)
- `Signal %d received, shutting down …` (INFO)
- `Shutting down.` (INFO)

Behavior:
- Reconnect loop: up to `MAX_RETRIES = 10` attempts; between attempts sleep
  `RECONNECT_DELAY_S = 1.0s`; a **new channel per attempt**; attach the
  `client-id` metadata to the call; channel keepalive options = configured
  keepalive time/timeout (these are HTTP/2 PING frames, below app messages).
- Pings sent at `--ping-interval-ms` with a per-stream incrementing id
  starting at 1, `sender=<client-id>`.
- On RPC/stream error: count the attempt, close/cancel, sleep, retry; after
  10 attempts log `Max retries (10) reached.` and finish with the `Done.`
  summary. SIGINT/SIGTERM → clean exit(0).
- **Deliberate fix:** the Python client never increments
  `total_pings_sent` (it always prints 0). The Go client *should* count pings
  correctly; keep the summary line format identical.

### Integration tests (pytest → go test)

Python runs the server as a **subprocess** (session-scoped fixture: random
free port, log to a temp file, `--ping-interval-ms 100 --keepalive-time-sec 10
--keepalive-timeout-sec 5`, readiness wait = open a short stream with a single
ping `sender="_health_check_"`, 10s deadline; teardown SIGTERM, 5s timeout,
then SIGKILL). Clients in the tests are **in-process** gRPC channels with
keepalive 1000ms/500ms.

| Python test | What it asserts |
|---|---|
| `test_single_client_ping_pong` | >0 pongs received; all `sender == "server"`; all `id > 0` |
| `test_single_client_count_pings_pongs` | pings sent > 0; >5 pongs over the 5s run (client 100ms interval, server 100ms) |
| `test_single_client_server_receives_pings` | server log contains lines with `Server received ping` and `test_client_0` (after a 2s settle sleep) |
| `test_multi_clients_all_receive_pongs` | each of 3 concurrent clients (ids `client_alpha`, `client_beta`, `client_gamma`; 150ms interval; 5s) received >0 pongs, all `sender == "server"` |
| `test_multi_clients_distinguish_streams` | server log has `Server received ping` lines for each of the 3 client ids (after a 3s settle sleep) |
| `test_multi_clients_total_pongs_exceed_single` | total pongs > 0 and every client > 0 |
| `test_multi_clients_sees_monotonic_pongs` | per client, received pong ids are non-decreasing (each stream has its own counter) |

Go equivalents: `TestSingleClientPingPong`, `TestSingleClientCountPingsPongs`,
`TestSingleClientServerReceivesPings`, `TestMultiClientsAllReceivePongs`,
`TestMultiClientsDistinguishStreams`, `TestMultiClientsTotalPongsExceedSingle`,
`TestMultiClientsSeesMonotonicPongs`.

### Makefile targets (mirror `../python/Makefile`)

`gen`, `server` (background, log to file), `server-foreground`, `client`
(make vars `TARGET`, `CLIENT_ID`, `PING_INTERVAL_MS`, `KEEPALIVE_TIME_SEC`,
`KEEPALIVE_TIMEOUT_SEC`), `kill-server`, `kill-client`, `kill`, `test`,
`clean`, `help` — same defaults (50051, 500ms, 10s, 5s).

---

## Target file layout

```
go/
├── .gitignore           (step 1: bin/, *.log)
├── Makefile             (step 1: gen; step 4: full target set)
├── README.md            (step 6)
├── go.mod               (exists)
├── go.sum               (step 1)
├── api/                 (step 1, generated, committed like python/)
│   ├── resilience.pb.go
│   └── resilience_grpc.pb.go
├── cmd/
│   ├── server/main.go   (step 2)
│   └── client/main.go   (step 3)
└── tests/               (step 5)
    ├── server_test.go            (TestMain: start/stop server subprocess, readiness, log capture)
    ├── helpers_test.go           (readServerLog, runClient, free port, readiness wait)
    ├── single_client_test.go     (3 tests, sync.Once shared scenario)
    └── multi_clients_test.go     (4 tests, sync.Once shared 3-client scenario)
```

Build artifacts go to `go/bin/` (gitignored). The generated code is **committed**
(mirrors Python, which commits `resilience_pb2*.py`), so a fresh clone builds
without protoc.

---

## Step 1 — Toolchain, scaffolding, generated API code

**Goal:** working code generation from `api/resilience.proto` into
`go/api/`, plus repo scaffolding. No hand-written Go code yet.

**Read first:** this plan (ground truth + layout), `go/go.mod`,
`api/resilience.proto`, `../python/Makefile` (its `gen` target, for reference).

**Deliverables**

1. Verify toolchain (macOS, Go ≥ 1.23 installed):
   - `go version`
   - `protoc --version` — if missing, install it (e.g. `brew install protoc`).
   - Install generators pinned to the go.mod versions:
     `go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.33.2`
     `go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.3.0`
     Ensure `$(go env GOPATH)/bin` is on PATH for the `protoc` invocations.
2. `go/api/` — run (from `go/`):
   ```bash
   protoc --proto_path=../api \
     --go_out=. --go_opt=module=github.com/example/grpc-resilience \
     --go-grpc_out=. --go-grpc_opt=module=github.com/example/grpc-resilience \
     resilience.proto
   ```
   Expect `api/resilience.pb.go` and `api/resilience_grpc.pb.go`.
3. `go/.gitignore` — at least: `bin/`, `*.log`.
4. `go/Makefile` — for now just the `gen` target (running the protoc command
   above, printing what it generated) and a `help` placeholder. The full
   target set arrives in step 4; keep the `gen` target text stable so step 4
   only adds targets.
5. `go mod tidy` → produces `go.sum` (grpc, protobuf, genproto).

**Verification**

```bash
cd go
make gen                 # regenerates, must succeed
go build ./...           # compiles api package
go vet ./...
gofmt -l .               # no output
ls api/resilience.pb.go api/resilience_grpc.pb.go go.sum
```

**Definition of done:** generated code committed; `go build ./...` green;
`make gen` reproducible.

---

## Step 2 — gRPC server (`cmd/server/main.go`)

**Goal:** Go server with byte-compatible CLI and log output vs
`../python/server.py`.

**Read first:** `../python/server.py` (whole file), `go/api/resilience_grpc.pb.go`
(interface you implement: `ResilienceServiceStream` with `Send`/`Recv`),
this plan's "Server behavior & log strings" section.

**Deliverables**

`go/cmd/server/main.go` (single file, mirroring the single-file Python
script):

- Flag handling per the CLI table; positional `port` after flag
  normalization (see gotcha in "CLI surface"). Defaults 50051/500/10/5.
- A small logging helper producing
  `[server] <2006-01-02 15:04:05> LEVEL msg` (levels INFO/WARNING/ERROR/DEBUG,
  DEBUG effectively disabled at the default level).
- `ResilienceServiceImpl.Stream(stream)`:
  - `metadata.MDFromIncomingContext(ctx)` → `client-id` (may be absent → `""`).
  - `for { resp, err := stream.Recv(); ... }`:
    - `io.EOF` → log `Connection closed (client=%s, peer=%s)` and return.
    - other error → log `Connection lost (client=%s, peer=%s, reason=%s)`
      and return.
    - log `Server received ping #%d from %s`.
    - Interval gate: on first ping send immediately (Python:
      `last_send_time = 0.0` → first elapsed is huge); afterwards only if
      `time.Since(lastSend) >= pingInterval`. On send: id++,
      log `Server sending Pong #%d to %s`,
      `stream.Send(&BidirectionalStreamResponse{Pong: &Pong{Id: id, Sender: "server"}})`.
  - Note: with grpc-go the handler runs in its own goroutine per stream —
    this is the equivalent of Python's `ThreadPoolExecutor(max_workers=10)`.
- `grpc.NewServer` with:
  - `grpc.Creds(insecure credentials)` — i.e. `grpc.Creds(credentials.NewInsecureCredentials())`,
  - `grpc.KeepaliveParams(keepalive.Time=time.Duration(keepaliveTimeSec)*time.Second, keepalive.Timeout=... )`,
  - `grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{MinTime: time.Second, PermitWithoutCalls: true})` —
    see the critical gotcha in the plan (test clients use 1s/500ms keepalive).
  - `lis, _ := net.Listen("tcp", fmt.Sprintf("[::]:%d", port))`;
    log the startup line; `server.Serve(lis)`; on SIGINT/SIGTERM
    (`signal.NotifyContext`) log `Server shutting down.` and
    `GracefulStop()`.

**Verification (manual)**

```bash
cd go
go build -o bin/server ./cmd/server
./bin/server 50061 --ping-interval-ms 100 &   # any free port
# expect: [server] ... INFO Resilience gRPC server listening on port 50061 (app ping 100 ms, keepalive 10s)
# also verify flag-before-positional order works:
./bin/server --ping-interval-ms 100 50062
kill %1  # SIGINT → "Server shutting down."
```

**Definition of done:** builds; both arg orderings work; log lines match the
templates exactly (diff against expected strings); clean shutdown on SIGINT.

---

## Step 3 — gRPC client (`cmd/client/main.go`)

**Goal:** Go client with byte-compatible CLI, reconnect loop, and log output
vs `../python/client.py`.

**Read first:** `../python/client.py` (whole file), `../python/server.py`
(for the server you'll test against), this plan's "Client behavior & log
strings" section.

**Deliverables**

`go/cmd/client/main.go` (single file):

- Flags: positional `target` (default `localhost:50051`), `--client-id`
  (default `client`), `--ping-interval-ms` (500), `--keepalive-time-sec` (10),
  `--keepalive-timeout-sec` (5); same positional/flag-order normalization as
  the server.
- Logging helper: `[client] <ts> LEVEL msg` (INFO/WARNING/ERROR).
- Run loop, constants `maxRetries = 10`, `reconnectDelay = 1.0 * time.Second`:
  ```
  attempt = 0
  for {
      attempt++
      if attempt > 1: log "Reconnecting (attempt %d/%d) …"; sleep 1s
      ctx, cancel := signal.NotifyContext(context.Background(), SIGINT, SIGTERM)
      conn, err := grpc.NewClient(target,
                  grpc.WithTransportCredentials(insecure.NewCredentials()),
                  grpc.WithKeepaliveParams(keepalive.Time/Timeout as configured))
      stream, err := stub.Stream(ctx)
      if err == nil:
          set "client-id" metadata: use metadata.AppendToOutgoingContext
          (grpc-go metadata keys are lower-cased; "client-id" is correct)
          sender goroutine: tick every pingInterval:
              id++; log "Client sending ping #%d";
              stream.Send(req); on error → signal stop, cancel
              (count pings sent atomically — fix the Python count bug)
          recv loop: for { resp, err := stream.Recv() }
              pong → log "Client received Pong #%d from %s", count++
              error payload → log "Client received Error ...", break
              err != nil → break
      else log "Connection error (attempt %d): %s"
      on stream error: log "RPC error (attempt %d): %s – %s"
          (status.Code(err).String(), status.Error(err).Message())
      cleanup: cancel ctx, wait for sender goroutine (channel), conn.Close()
      if attempt >= maxRetries: log "Max retries (10) reached."; break
  }
  log "Done. Attempt=%d  Pings sent=%d  Pongs received=%d"
  ```
- On SIGINT/SIGTERM received during the run: log
  `Signal %d received, shutting down …` (note: on signal, cancel the current
  attempt's context and exit cleanly after the summary — mirror Python's
  `sys.exit(0)` + finally-block cleanup; do not report a spurious
  "RPC error" for the signal-induced cancellation; a received signal should
  stop the retry loop, log `Shutting down.`, and exit 0).

**Verification (manual)**

```bash
cd go
go build -o bin/client ./cmd/client
make-less smoke:
./bin/server 50051 --ping-interval-ms 100 &        # or: see Step 4 `make server`
./bin/client localhost:50051 --client-id c1 --ping-interval-ms 200
# expect: interleaved "Client sending ping #N" / "Client received Pong #N from server"
# resilience check: kill the server while the client runs, restart it,
# client must log "Reconnecting (attempt 2/10) …" then resume pings/pongs
# stop check: Ctrl-C → "Signal 2 received, shutting down …" + "Shutting down." + "Done. ..."
kill %1
```

**Definition of done:** builds; happy path, reconnect path, and signal path
all produce the exact log templates; exit code 0 on signal.

---

## Step 4 — Full Makefile + placeholder tests

**Goal:** `go/Makefile` mirrors `../python/Makefile` target-for-target;
`make test` is green (placeholder only — real tests arrive in step 5).

**Read first:** `../python/Makefile` (copy the structure, comments, and help
text layout), existing `go/Makefile` (from step 1).

**Deliverables**

1. `go/Makefile` targets (same names/semantics/defaults as Python's):
   - `build` (new helper target): `go build -o bin/server ./cmd/server` and
     `go build -o bin/client ./cmd/client`.
   - `gen`: from step 1 (keep as-is).
   - `server`: build, then
     `./bin/server 50051 --ping-interval-ms 500 --keepalive-time-sec 10 --keepalive-timeout-sec 5 > ../temp/go/server.log 2>&1 &`
     + `sleep 1` + status echo. **Use `../temp/go/server.log`** (not
     `../temp/server.log`, which is the Python server's log; the root
     `.gitignore` ignores `temp/`).
   - `server-foreground`: same invocation, no `&`, no log redirection
     (aliases: keep the Python naming — `server-foreground` target).
   - `client`: vars `TARGET ?=` (empty → Go default), `CLIENT_ID ?= client`,
     `PING_INTERVAL_MS ?= 500`, `KEEPALIVE_TIME_SEC ?= 10`,
     `KEEPALIVE_TIMEOUT_SEC ?= 5`; runs
     `./bin/client $(TARGET) --client-id $(CLIENT_ID) ...`.
   - `kill-server` / `kill-client` / `kill`: `pkill -f "bin/server"` /
     `pkill -f "bin/client"` with the same `&& echo ... || echo "  No server running."`
     messages as Python (adjust the pattern text to match the Go binaries).
   - `test`: `go test ./tests/ -v -count=1 -timeout 120s`.
   - `clean`: remove `bin/` and `../temp/go/server.log` (do **not** remove
     committed generated code in `api/`; Python's clean only removes
     regenerable artifacts — same policy).
   - `help`: list of targets + client flag table + examples, mirroring the
     Python help text (update the examples to `./bin/client` / `make client`).
2. `go/tests/doc_test.go` (or `placeholder_test.go`):
   `package tests` with a trivial passing test
   (`func TestPlaceholder(t *testing.T) { /* replaced in step 5 */ }`)
   so `make test` is green before step 5.

**Verification**

```bash
cd go
make build
make server                # "Server started (port 50051, keepalive 10s)"
tail ../temp/go/server.log # startup line present
make client TARGET=localhost:50051 CLIENT_ID=make_test PING_INTERVAL_MS=200 &
sleep 3; kill %2; wait %2  # pings/pongs visible
make kill                  # "  Server stopped." / "  No client running."
make test                  # placeholder passes
make help
```

**Definition of done:** every Python Makefile target exists in Go with the
same defaults and messages; the full manual loop (server → client → kill →
test) works.

---

## Step 5 — Integration tests (`tests/` package)

**Goal:** port all 7 pytest tests to Go; `make test` runs the real suite.

**Read first:** `../python/tests/conftest.py`,
`../python/tests/test_single_client.py`,
`../python/tests/test_multi_clients.py`, `../python/_helpers.py`, this plan's
"Integration tests" table.

**Deliverables**

`go/tests/` (all files `package tests`):

1. `helpers_test.go`
   - `freePort() int` — `net.Listen("tcp", "[::]:0")`, read port, close.
   - `waitForServer(t, address string)` — 10s deadline, 250ms retry: open a
     stream on an insecure conn (keepalive 1000ms/500ms), send one ping
     `sender="_health_check_"`, drain/EOF; success returns; on any error
     retry; timeout → `t.Fatal`-style error.
   - `type serverFixture struct { Address string; logPath string; proc *exec.Cmd }`
   - `readServerLog(f *serverFixture) string` — read the whole temp log file
     (equivalent of `python/_helpers.py::read_server_log`).
2. `server_test.go`
   - `func TestMain(m *testing.M)`: find free port; create temp log file;
     `exec.Command` **the built server binary** (`../bin/server` if built,
     else build on the fly via `go build -o <tmp>/server ./cmd/server` —
     prefer building to a temp path inside TestMain so `make test` does not
     depend on prior `make build`), args
     `<port> --ping-interval-ms 100 --keepalive-time-sec 10
     --keepalive-timeout-sec 5`, stdout+stderr → temp log; wait for
     readiness; `code := m.Run()`; teardown: `proc.Signal(syscall.SIGTERM)`,
     wait 5s then `proc.Kill()`; remove temp log; `os.Exit(code)`.
     This is the session-scoped-fixture equivalent (start once per package
     run, stop once at the end).
3. `single_client_test.go`
   - `runClient(t, target, clientID string, interval time.Duration,
     duration time.Duration) (pingsSent int, pongs []*resilience.Pong)`:
     in-process conn (keepalive 1s/500ms), start stream with `client-id`
     metadata; sender goroutine sends until the duration deadline (counting
     pings atomically); recv loop collects pongs, breaks on error payload or
     stream end; cancel + close at end. (Equivalent of Python
     `_run_client`.)
   - `var singleOnce sync.Once; var singleResult ...` — run once with
     `client_id="test_client_0"`, 100ms, 5.0s; all three tests read the
     shared result (pytest re-ran the fixture per test; sharing is safe
     because tests only read recorded data — and is much faster).
   - `TestSingleClientPingPong`, `TestSingleClientCountPingsPongs`,
     `TestSingleClientServerReceivesPings` — assertions exactly as in the
     ground-truth table (log checks read `readServerLog`, counting lines
     containing `Server received ping` and the client id; keep the 2s settle
     `time.Sleep` in the log test).
4. `multi_clients_test.go`
   - `runMultiOnce` (sync.Once): for ids `client_alpha`, `client_beta`,
     `client_gamma` start three goroutines calling `runClient` (150ms,
     5.0s, same deadline), collect `(clientID, pingsSent, pongs)`; wait for
     all. (Equivalent of the Python fixture with drain threads.)
   - `TestMultiClientsAllReceivePongs`,
     `TestMultiClientsDistinguishStreams` (3s settle),
     `TestMultiClientsTotalPongsExceedSingle`,
     `TestMultiClientsSeesMonotonicPongs` — assertions per the table.
   - Replace/delete the step-4 placeholder test file.

**Verification**

```bash
cd go
go vet ./...
gofmt -l .            # no output
make test             # 7 tests, all pass
make test | tail -20  # -v output shows all 7 test names passing
# run twice in a row to check for flakiness/state leakage:
make test
```

**Definition of done:** 7/7 tests pass in `make test`; server subprocess is
started and stopped exactly once per run; no leftover processes
(`pgrep -f bin/server` empty after the run).

---

## Step 6 — Documentation + final end-to-end verification

**Goal:** `go/README.md` mirroring the Python README, root README Go
section, and a full parity pass.

**Read first:** `../python/README.md`, root `README.md`, all `go/` files
created so far, this plan's ground-truth sections.

**Deliverables**

1. `go/README.md` — mirror the structure of `../python/README.md`:
   quick start (`make gen` (only needed after proto changes; generated code
   is committed), `make server` / `make server-foreground`,
   `make client TARGET=...`, `make kill`, `make test`);
   architecture diagram (proto → `go/api/` generated → `cmd/server`,
   `cmd/client`); argument tables (identical defaults); an implementation
   notes section translating the Python-specific pieces to Go (goroutine
   sender instead of generator; `signal.NotifyContext` instead of
   `signal.signal`; keepalive enforcement policy gotcha; the
   pings-sent counting fix); server log location `../temp/go/server.log` +
   `tail -f`; file layout; troubleshooting.
2. Root `README.md` — add a `## Go` section with the usage commands
   (`cd go && make server`, `make client PING_INTERVAL_MS=... CLIENT_ID=...`,
   `make test`) mirroring the `## Running tests` / `## Python` sections, and
   update the note about there being no root-level Makefile (now both
   `python/` and `go/` have their own Makefiles).
3. Final hygiene: `go mod tidy` (no go.mod/go.sum drift), `go vet ./...`,
   `gofmt -l .` clean.

**Verification (final parity pass)**

```bash
cd go
make clean && make gen && make build
make server
make client TARGET=localhost:50051 CLIENT_ID=alpha PING_INTERVAL_MS=10000 &
make client TARGET=localhost:50051 CLIENT_ID=beta  PING_INTERVAL_MS=9500  &
sleep 20   # observe: per-client pings, independent per-stream pong counters,
           # interleaved [client]/[server] logs, no keepalive-induced GOAWAYs
make kill
make test            # full integration suite green
make help
# cross-language check: start the Go server, then run the PYTHON client
# against it (and vice versa) — both are the same API:
cd ../python && uv run python client.py localhost:50051 --client-id py_client
```

**Definition of done:** docs complete; every command from the README/plan
works as written; `make test` green twice in a row; Go ↔ Python
interoperability smoke works (same wire protocol, `client-id` metadata read
on both sides).

---

## Sequencing summary

| Step | Deliverable | Depends on |
|------|-------------|------------|
| 1 | Toolchain, `go/api/` generated code, `.gitignore`, `gen` target | — |
| 2 | `cmd/server` (CLI + behavior parity) | 1 |
| 3 | `cmd/client` (CLI + reconnect + signals) | 1, 2 |
| 4 | Full `Makefile` + placeholder test | 2, 3 |
| 5 | `tests/` package — all 7 integration tests | 4 |
| 6 | `go/README.md`, root README, final parity verification | 5 |
