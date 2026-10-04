# Rust Migration Plan — ResilienceService

This plan migrates the Python implementation of the gRPC resilience test to
Rust. The Rust code must provide the **same CLI interface** and the **same
integration tests** as the Python implementation in `../python/`.

The steps below are executed **in sequence, each in a fresh context**. Every
step is self-contained: it lists the files to read first, the exact
deliverables, implementation gotchas, verification commands, and a
definition of done. Do not skip a step's verification before starting the
next.

Current state: only this plan file exists under `rust/` (no `Cargo.toml`
yet). The shared API is `api/resilience.proto` (package `resilience`).
Toolchain available on this machine: `rustc`/`cargo` 1.96; `protoc` may or
may not be installed (Step 1 handles it). For reference, the Go port lives in
`../go/` (plan: `../go/plan.md`) — the ground-truth sections in this document
are the single source of truth for behavior parity.

---

## Rust design decisions (read once, applies to all steps)

1. **Stack:** [`tonic`](https://github.com/hyperium/tonic) (gRPC, async) +
   `prost` (protos) + `tokio` (runtime) + `clap` (CLI) + `time` (timestamp
   formatting). Codegen via `tonic-build` in `build.rs`.
2. **Two separate binaries** (mirrors the two Python scripts, no subcommand):
   `src/bin/server.rs` and `src/bin/client.rs`, plus `src/lib.rs` exposing the
   generated API and shared helpers so integration tests can use the client
   in-process.
3. **Generated code is NOT committed** (deliberate difference from Python/Go,
   which commit their generated stubs): `tonic-build` writes to Cargo's
   `OUT_DIR` during `cargo build`. Consequence: building this crate requires
   `protoc` on PATH. `target/` is gitignored.
4. **Logging:** no `log`/`tracing` crate — a tiny `src/logfmt.rs` helper that
   `println!`s the exact Python-compatible line
   `[role] YYYY-MM-DD HH:MM:SS LEVEL message` (local time via the `time`
   crate, levels `INFO`/`WARNING`/`ERROR`; Python's asctime is local time).
   Exact output control without framework overhead.
5. **Bidi streaming pattern:** client side uses a `tokio::sync::mpsc` channel
   as the request stream (`Request::new(receiver)` — tonic implements
   `IntoStreamingRequest` for `Request<M: Stream>`; if the pinned tonic
   version lacks the blanket impl, wrap with `tonic::Streaming::new(rx)` or
   use `response.into_split()` with `sender.send()`). Server side uses
   `tonic::Streaming` with `message()`/`send()`.
6. **Test fixtures are function-scoped** (each test starts and `Drop`s its
   own server subprocess) — the Rust idiom equivalent of pytest fixtures.
   This is *more* hermetic than Python's session-scoped server (fresh log per
   test) and avoids Rust's lack of atexit; the 7 test assertions stay
   identical. Test binaries locate the server via cargo's
   `CARGO_BIN_EXE_server` env var.
7. **No keepalive-enforcement gotcha** (unlike grpc-go): hyper/tonic does not
   tear down clients whose keepalive PINGs are too frequent, so test clients
   with 1s/500ms keepalive are safe. Still verify this in Step 5.

---

## Ground truth: Python behavior to replicate

Read `../python/server.py`, `../python/client.py`, `../python/tests/`,
`../python/Makefile`, and `api/resilience.proto` for the full picture. This
section is the condensed contract the Rust code must match.

### API

`api/resilience.proto` defines
`ResilienceService.Stream(stream BidirectionalStreamRequest) returns (stream
BidirectionalStreamResponse)`. Requests carry a `Ping{id, sender}`; responses
carry a oneof of `Pong{id, sender}` or `Error{id, message, code}`.

### CLI surface (must be identical)

| Command  | Positional arg (default) | Flags |
|----------|--------------------------|-------|
| server   | `port` (50051)           | `--ping-interval-ms` (500), `--keepalive-time-sec` (10), `--keepalive-timeout-sec` (5) |
| client   | `target` (localhost:50051) | `--client-id` (client), `--ping-interval-ms` (500), `--keepalive-time-sec` (10), `--keepalive-timeout-sec` (5) |

Both commands must support `--help` with sensible usage text.
**Note:** clap (derive) parses mixed flag/positional order natively, like
argparse — no normalization needed (this *was* a gotcha in the Go port).
Use `#[arg(default_value_t = 50051)] port: u16` and
`#[arg(default_value = "localhost:50051")] target: String` for optional
positionals.

### Server behavior & log strings

Log format (must match Python): `[server] YYYY-MM-DD HH:MM:SS LEVEL message`
(local time, levels `INFO`/`WARNING`/`ERROR`). Message templates, verbatim:

- `Resilience gRPC server listening on port %d (app ping %d ms, keepalive %ds)`
- `Server received ping #%d from %s` (INFO)
- `Server sending Pong #%d to %s` (INFO)
- `Skipping response (elapsed=%.3fs < %fs)` (DEBUG — never visible at the default INFO level; do not emit at INFO)
- `Connection lost (client=%s, peer=%s, reason=%s)` (WARNING)
- `Connection closed (client=%s, peer=%s)` (INFO)
- `Server shutting down.` (INFO)

Behavior:
- Bind an **insecure** (plaintext HTTP/2) listener on `[::]:<port>`.
- Each stream: read the `client-id` key from incoming metadata (tonic
  metadata keys are lowercase — `request.metadata().get("client-id")`; may be
  absent → `""`); for every received ping log it; send a Pong
  (`sender="server"`, per-stream incrementing id starting at 0→1) **gated by
  the configured send interval** — i.e. Pongs are sent inside the receive
  loop only when `elapsed >= ping_interval` (Python: first ping always
  triggers a send because `last_send_time = 0.0`; in Rust track
  `Option<Instant>` — `None` means "send now"). Do *not* add an independent
  timer.
- `tonic::transport::Server::builder().keepalive_interval(Some(…)).
  keepalive_timeout(Some(…))` with the configured seconds.
- Peer address for the close/lost logs: `request.remote_addr()`
  (`Option<SocketAddr>`); on `None` print `<unknown>` (mirrors Python's
  try/except in `_peer()`).
- Clean stream end (`message() → Ok(None)`) → `Connection closed`; transport
  error (`Err(status)`) → `Connection lost`.
- Graceful shutdown on SIGINT/SIGTERM
  (`tokio::signal::ctrl_c()` + `tokio::signal::unix` listener combined into
  one future, `Server::serve_with_shutdown`), log `Server shutting down.`

### Client behavior & log strings

Log format: `[client] YYYY-MM-DD HH:MM:SS LEVEL message`. Message templates,
verbatim (note the unicode ellipsis `…` and en-dash `–`):

- `Reconnecting (attempt %d/10) …` (INFO, only for attempts > 1, logged before the 1s delay)
- `Client sending ping #%d` (INFO)
- `Client received Pong #%d from %s` (INFO)
- `Client received Error #%d: code=%d msg=%s` (WARNING; on error the stream is treated as closed)
- `RPC error (attempt %d): %s – %s` (ERROR; code name, then details — `Status::code()` / `Status::message()`)
- `Connection error (attempt %d): %s` (ERROR)
- `Max retries (10) reached.` (INFO)
- `Done. Attempt=%d  Pings sent=%d  Pongs received=%d` (INFO; note double space)
- `Signal %d received, shutting down …` (INFO)
- `Shutting down.` (INFO)

Behavior:
- Reconnect loop: up to `MAX_RETRIES = 10` attempts; between attempts sleep
  `1.0s`; a **new channel per attempt**; set the `client-id` entry in the
  outgoing `MetadataMap` (`request.set_metadata(…)`); channel keepalive =
  configured time/timeout (`Channel::from_shared(url)?.keepalive_interval(
  Some(…)).keepalive_timeout(Some(…))`).
- **Target normalization:** tonic endpoints require a scheme — if the user
  passes `localhost:50051` (the default, scheme-less, exactly like Python),
  prepend `http://` before `from_shared`.
- Pings sent at `--ping-interval-ms` with a per-stream incrementing id
  starting at 1, `sender=<client-id>`, via the mpsc request stream (a
  sender task using `tokio::time::interval` — its first tick fires
  immediately, matching Python's send-then-sleep generator).
- On stream/RPC error: count the attempt, cancel/close, sleep, retry; after
  10 attempts log `Max retries (10) reached.` and finish with the `Done.`
  summary. SIGINT/SIGTERM during the run → log the signal line, stop the
  retry loop (do not report a spurious "RPC error" for the
  signal-induced cancellation), log `Shutting down.`, exit 0.
- **Deliberate fix:** the Python client never increments
  `total_pings_sent` (it always prints 0). The Rust client *should* count
  pings correctly; keep the summary line format identical.

### Integration tests (pytest → cargo test)

Python starts the server as a **subprocess** (fixture: random free port, log
to a temp file, `--ping-interval-ms 100 --keepalive-time-sec 10
--keepalive-timeout-sec 5`, readiness wait = open a short stream with a
single ping `sender="_health_check_"`, 10s deadline; teardown SIGTERM, 5s
timeout, then SIGKILL). Clients in the tests are **in-process** channels with
keepalive 1000ms/500ms. Rust equivalent: function-scoped fixture in
`tests/common/mod.rs` built on `CARGO_BIN_EXE_server` + `Drop` (see design
decision 6).

| Python test | What it asserts |
|---|---|
| `test_single_client_ping_pong` | >0 pongs received; all `sender == "server"`; all `id > 0` |
| `test_single_client_count_pings_pongs` | pings sent > 0; >5 pongs over the 5s run (client 100ms interval, server 100ms) |
| `test_single_client_server_receives_pings` | server log contains lines with `Server received ping` and `test_client_0` (after a 2s settle sleep) |
| `test_multi_clients_all_receive_pongs` | each of 3 concurrent clients (ids `client_alpha`, `client_beta`, `client_gamma`; 150ms interval; 5s) received >0 pongs, all `sender == "server"` |
| `test_multi_clients_distinguish_streams` | server log has `Server received ping` lines for each of the 3 client ids (after a 3s settle sleep) |
| `test_multi_clients_total_pongs_exceed_single` | total pongs > 0 and every client > 0 |
| `test_multi_clients_sees_monotonic_pongs` | per client, received pong ids are non-decreasing (each stream has its own counter) |

Rust equivalents (`#[tokio::test]`): `test_single_client_ping_pong`,
`test_single_client_count_pings_pongs`,
`test_single_client_server_receives_pings`,
`test_multi_clients_all_receive_pongs`,
`test_multi_clients_distinguish_streams`,
`test_multi_clients_total_pongs_exceed_single`,
`test_multi_clients_sees_monotonic_pongs`.

### Makefile targets (mirror `../python/Makefile`)

`gen`, `build`, `server` (background, log to file), `server-foreground`,
`client` (make vars `TARGET`, `CLIENT_ID`, `PING_INTERVAL_MS`,
`KEEPALIVE_TIME_SEC`, `KEEPALIVE_TIMEOUT_SEC`), `kill-server`,
`kill-client`, `kill`, `test`, `clean`, `help` — same defaults (50051, 500ms,
10s, 5s). Rust `gen` is a no-op echo (codegen is automatic via `build.rs`)
and `clean` = `cargo clean`.

---

## Target file layout

```
rust/
├── .gitignore           (step 1: target/)
├── Cargo.toml           (step 1)
├── build.rs             (step 1: tonic-build codegen → OUT_DIR)
├── Makefile             (step 1: gen/build skeleton; step 4: full set)
├── README.md            (step 6)
├── plan.md              (this file)
└── src/
    ├── lib.rs           (step 1: pub mod api { tonic::include_proto!("resilience"); } + logfmt)
    ├── logfmt.rs        (step 2: exact-format logging helper)
    └── bin/
        ├── server.rs    (step 2)
        └── client.rs    (step 3)
└── tests/               (step 5)
    ├── common/mod.rs    (fixture, free port, readiness wait, run_client, log reader)
    ├── single_client.rs (3 tests)
    └── multi_clients.rs (4 tests)
```

Generated code lives in `target/debug/build/<crate>-*/out/` (never
committed). Types are reachable as
`resilience_rust::api::resilience::{Ping, Pong, Error,
BidirectionalStreamRequest, BidirectionalStreamResponse,
ResilienceServiceClient, ResilienceServiceServer, ResilienceService}` —
verify against the actual OUT_DIR file in Step 1 and adjust the plan's
assumptions if `include_proto!` yields a different module path.

---

## Step 1 — Scaffolding: Cargo.toml, build.rs, generated API

**Goal:** compiling skeleton crate with tonic codegen from
`api/resilience.proto`; no behavior code yet.

**Read first:** this plan (design decisions + layout), `go/go.mod` (for
comparison only), `api/resilience.proto`, `../python/Makefile` (gen target,
for reference).

**Deliverables**

1. Toolchain check:
   - `cargo --version`, `rustc --version` (present: 1.96).
   - `protoc --version` — if missing, install it (e.g. `brew install protoc`).
     `tonic-build` shells out to `protoc` (or `$PROTOC`).
2. `rust/Cargo.toml`:
   ```toml
   [package]
   name = "resilience-rust"
   version = "0.1.0"
   edition = "2021"

   [dependencies]
   tonic = "0.12"          # or the latest 0.1x available; keep prost in lockstep
   tokio = { version = "1", features = ["full"] }
   prost = "0.13"
   clap = { version = "4", features = ["derive"] }
   time = { version = "0.3", features = ["local-offset", "formatting"] }

   [build-dependencies]
   tonic-build = "0.12"

   [[bin]]
   name = "server"
   path = "src/bin/server.rs"

   [[bin]]
   name = "client"
   path = "src/bin/client.rs"
   ```
   (If `tonic 0.12` and `prost 0.13` turn out mismatched for the resolved
   versions, let `cargo`'s lockfile decide and use `cargo add` to align.)
3. `rust/build.rs`:
   ```rust
   fn main() -> Result<(), Box<dyn std::error::Error>> {
       tonic_build::configure()
           .build_client(true)
           .build_server(true)
           .compile_protos(&["../api/resilience.proto"], &["../api"])?;
       Ok(())
   }
   ```
4. `rust/src/lib.rs`:
   ```rust
   pub mod api {
       tonic::include_proto!("resilience");
   }
   ```
   plus empty stub files `src/bin/server.rs` and `src/bin/client.rs`
   (`fn main() { /* step 2 / step 3 */ }`) so the crate compiles.
5. `rust/.gitignore`: `target/`.

**Verification**

```bash
cd rust
cargo build
ls target/debug/build/resilience-rust-*/out/   # generated .rs present
cargo test                                      # 0 tests, green
cargo clippy --all-targets 2>/dev/null || true # informational only
```

**Definition of done:** crate compiles; generated module is importable
(write a quick throwaway test or `cargo doc` to confirm the type paths listed
in the layout section, and correct the plan note if they differ).

---

## Step 2 — gRPC server (`src/bin/server.rs` + `src/logfmt.rs`)

**Goal:** Rust server with byte-compatible CLI and log output vs
`../python/server.py`.

**Read first:** `../python/server.py` (whole file), this plan's "Server
behavior & log strings" section, the generated API in
`target/debug/build/*/out/` (client/server traits, `tonic::Streaming`),
Step 1's outcome notes.

**Deliverables**

1. `rust/src/logfmt.rs` — shared by both binaries (declare
   `pub mod logfmt;` in `lib.rs`):
   - `pub fn log_line(role: &str, level: &str, msg: &str)` printing
     `[<role>] <YYYY-MM-DD HH:MM:SS> <LEVEL> <msg>` using `time::Local::now()`
     (format description `"[year]-[month]-[day] [hour]:[minute]:[second]"`).
   - Convenience wrappers `info!`/`warn!`/`error!` (macros or functions) for
     a fixed role, mapping to levels `INFO`/`WARNING`/`ERROR`.
   - A unit test asserting an exact expected line shape (timestamps
     redacted or frozen via a test seam — e.g. take the formatted timestamp
     as a parameter).
2. `rust/src/bin/server.rs`:
   - clap derive struct per the CLI table (`port: u16` default 50051, three
     `--…-sec/ms` flags with defaults 500/10/5); `--help` works.
   - `#[tokio::main]` (multi-thread runtime).
   - `ResilienceServiceImpl` (implements the generated `ResilienceService`
     trait) holding `ping_interval: Duration`:
     ```rust
     async fn stream(&self, request: Request<Streaming<BidirectionalStreamRequest>>)
         -> Result<Response<Streaming<BidirectionalStreamResponse>>, Status> {
         let client_id = request.metadata().get("client-id")
             .and_then(|v| v.to_str().ok()).unwrap_or("").to_string();
         let peer = request.remote_addr()
             .map(|a| a.to_string()).unwrap_or_else(|| "<unknown>".into());
         let mut out = /* Response from stream.into_inner() */;
         let mut server_ping_id: i32 = 0;
         let mut last_send: Option<Instant> = None;   // None ⇒ send on first ping
         while let Some(req) = stream.message().await? {
             let (id, sender) = (req.ping.id, req.ping.sender.clone());
             server_log::info(&format!("Server received ping #{id} from {sender}"));
             let now = Instant::now();
             if last_send.map_or(true, |t| now.duration_since(t) >= self.ping_interval) {
                 server_ping_id += 1;
                 last_send = Some(now);
                 server_log::info(&format!("Server sending Pong #{server_ping_id} to {sender}"));
                 out.send(BidirectionalStreamResponse {
                     payload: Some(payload::Pong(Pong { id: server_ping_id, sender: "server".into() })),
                 }).await?;
             }
         }
         // clean end:
         server_log::info(&format!("Connection closed (client={client_id}, peer={peer})"));
         Ok(Response::new(...))
     }
     ```
     Map the `Err(status)` escape path to
     `Connection lost (client=%s, peer=%s, reason=%s)` (WARNING) before
     propagating the `Status`.
   - Server bootstrap: `tonic::transport::Server::builder()`
     + `.keepalive_interval(Some(d)).keepalive_timeout(Some(d))`
     + `.add_service(ResilienceServiceServer::new(impl))`
     + `serve_with_shutdown("[::]:<port>".parse::<SocketAddr>()?, shutdown_signal)`,
     where `shutdown_signal` = `tokio::select!` over `ctrl_c()` and the Unix
     SIGTERM listener. Log the startup line (exact template) **before**
     serving; on shutdown signal log `Server shutting down.` and exit 0.

**Verification (manual)**

```bash
cd rust
cargo build
./target/debug/server 50061 --ping-interval-ms 100 &
# expect: [server] … INFO Resilience gRPC server listening on port 50061 (app ping 100 ms, keepalive 10s)
# both arg orders must work (clap handles it):
./target/debug/server --ping-interval-ms 100 50062
kill -INT %1   # → "Server shutting down."
```
(Real traffic verification happens in Step 3 with the client.)

**Definition of done:** builds; `--help` for all flags; both arg orderings
work; log lines match the templates exactly; clean SIGINT/SIGTERM shutdown.

---

## Step 3 — gRPC client (`src/bin/client.rs`)

**Goal:** Rust client with byte-compatible CLI, reconnect loop, and log
output vs `../python/client.py`.

**Read first:** `../python/client.py` (whole file), Step 2's server (to test
against), this plan's "Client behavior & log strings" section.

**Deliverables**

`rust/src/bin/client.rs`:

- clap derive struct per the CLI table: `target: String` (default
  `localhost:50051`), `--client-id` (default `client`), `--ping-interval-ms`
  (500), `--keepalive-time-sec` (10), `--keepalive-timeout-sec` (5).
- Target normalization: prepend `http://` when the value has no `://` scheme.
- `#[tokio::main]`, constants `MAX_RETRIES: u32 = 10`,
  `RECONNECT_DELAY: Duration = 1s`.
- Run loop:
  ```rust
  let signal_rx = /* oneshot fired by a signal task watching
                     ctrl_c() + Unix SIGTERM (logs
                     "Signal %d received, shutting down …") */;
  let mut attempt = 0u32;
  let mut totals = (0u64, 0u64); // pings sent, pongs received
  loop {
      attempt += 1;
      if attempt > 1 {
          client_log::info(&format!("Reconnecting (attempt {attempt}/10) …"));
          tokio::time::sleep(RECONNECT_DELAY).await;
      }
      // fresh channel per attempt
      match Channel::from_shared(url.clone())
          .unwrap()
          .keepalive_interval(Some(ktime))
          .keepalive_timeout(Some(ktimeout))
          .connect().await
      {
          Err(e) => client_log::error(&format!("Connection error (attempt {attempt}): {e}")),
          Ok(channel) => {
              let mut map = MetadataMap::new();
              map.insert("client-id", client_id.parse().unwrap());
              let mut request = Request::new(receiver); // mpsc receiver (see design decision 5)
              request.set_metadata(map);
              // sender task: interval loop, ping id starts at 1,
              //   log "Client sending ping #N", tx.send(req), count via AtomicU64
              match client.stream(request).await { Ok(resp) => { recv loop },
                     Err(s) => log "RPC error (attempt {attempt}): {} – {}" }
          }
      }
      if signal fired { client_log::info("Shutting down."); break; }  // exit 0
      if attempt >= MAX_RETRIES { client_log::info("Max retries (10) reached."); break; }
  }
  client_log::info(&format!("Done. Attempt={attempt}  Pings sent={}  Pongs received={}", …));
  ```
  - Recv loop over `response.into_inner()` (`tonic::Streaming`):
    `Pong` → log `Client received Pong #… from …`, count;
    `Error` payload → log
    `Client received Error #%d: code=%d msg=%s` (WARNING), break;
    transport `Err(status)` → break, then log
    `RPC error (attempt %d): %s – %s` (ERROR) with
    `status.code().to_string()` and `status.message()`.
  - Sender task ends (drop of the mpsc sender) only on cancellation — the
    long-running client pings forever until error/signal, exactly like
    Python's infinite generator.
  - If the `Request<Receiver<T>>` → `IntoStreamingRequest` path fails to
    compile with the pinned tonic, switch to
    `tonic::Streaming::new(receiver)` or the `into_split()` +
    `sender.send()` pattern; keep the observable behavior identical.

**Verification (manual)**

```bash
cd rust
cargo build
./target/debug/server 50051 --ping-interval-ms 100 &
./target/debug/client localhost:50051 --client-id c1 --ping-interval-ms 200
# expect: interleaved "Client sending ping #N" / "Client received Pong #N from server"
# resilience: kill the server, restart it, client must log
#   "Reconnecting (attempt 2/10) …" then resume pings/pongs
# stop: Ctrl-C → "Signal 2 received, shutting down …" + "Shutting down." + "Done. ..."
kill %1
```

**Definition of done:** builds; happy path, reconnect path, and signal path
produce the exact log templates; exit code 0 on signal; pings actually
counted in the summary (the deliberate Python-bug fix).

---

## Step 4 — Full Makefile + placeholder tests

**Goal:** `rust/Makefile` mirrors `../python/Makefile` target-for-target;
`make test` is green (placeholder only — real tests arrive in Step 5).

**Read first:** `../python/Makefile` (copy structure, comments, help text),
the existing `rust/Makefile` from Step 1.

**Deliverables**

1. `rust/Makefile` (same names/semantics/defaults as Python's):
   - `gen`: echo that Rust codegen runs automatically via `build.rs` on every
     build (requires `protoc`); optionally `touch build.rs` to force
     re-running.
   - `build`: `cargo build` (debug profile).
   - `server`: `./target/debug/server 50051 --ping-interval-ms 500
     --keepalive-time-sec 10 --keepalive-timeout-sec 5 > ../temp/rust/server.log 2>&1 &`
     + `sleep 1` + status echo. **Use `../temp/rust/server.log`** (root
     `.gitignore` ignores `temp/`; avoids clobbering `../temp/server.log`
     used by the Python port).
   - `server-foreground`: same invocation, foreground, no redirection.
   - `client`: vars `TARGET ?=` (empty → Rust default), `CLIENT_ID ?= client`,
     `PING_INTERVAL_MS ?= 500`, `KEEPALIVE_TIME_SEC ?= 10`,
     `KEEPALIVE_TIMEOUT_SEC ?= 5`; runs
     `./target/debug/client $(TARGET) --client-id $(CLIENT_ID) …`.
   - `kill-server` / `kill-client` / `kill`: `pkill -f "rust/target/debug/server"`
     / `pkill -f "rust/target/debug/client"` with the same echo messages as
     Python (`"  Server stopped."` / `"  No server running."` etc.).
   - `test`: `cargo test` (add `-v` and a `-- --test-threads=` note later if
     needed; default parallelism is fine because each test uses its own
     port).
   - `clean`: `cargo clean` + `rm -rf ../temp/rust/server.log` (do not touch
     anything committed — there is no generated code to remove).
   - `help`: mirrors Python's help text (targets, client flag table,
     examples with `make client TARGET=… CLIENT_ID=… PING_INTERVAL_MS=…`).
2. `rust/tests/placeholder.rs` (or `tests/common/mod.rs` with one trivial
   `#[test]`) so `cargo test` is green before Step 5.

**Verification**

```bash
cd rust
make build
make server               # "Server started (port 50051, keepalive 10s)"
tail ../temp/rust/server.log   # startup line present
make client TARGET=localhost:50051 CLIENT_ID=make_test PING_INTERVAL_MS=200 &
sleep 3; kill %2; wait %2  # pings/pongs visible
make kill
make test                 # placeholder passes
make help
```

**Definition of done:** every Python Makefile target exists with the same
defaults and messages; the full manual loop (server → client → kill → test)
works.

---

## Step 5 — Integration tests (`tests/` directory)

**Goal:** port all 7 pytest tests to `cargo test`; `make test` runs the real
suite.

**Read first:** `../python/tests/conftest.py`,
`../python/tests/test_single_client.py`,
`../python/tests/test_multi_clients.py`, `../python/_helpers.py`, this plan's
"Integration tests" table and design decision 6.

**Deliverables**

`rust/tests/` (each file its own test crate linking the `resilience_rust`
lib):

1. `tests/common/mod.rs`
   - `free_port() -> u16` — `TcpListener::bind("[::]:0")`, read port, drop.
   - `struct ServerFixture { pub address: String, pub log_path: PathBuf,
     child: Child }` with `impl Drop` → `SIGTERM`, wait up to 5s, then
     `kill()` (mirrors the Python fixture teardown), and `fn read_log(&self)
     -> String`.
   - `fn start_server() -> ServerFixture` —
     `std::process::Command::new(env!("CARGO_BIN_EXE_server"))` with args
     `"<port>" "--ping-interval-ms" "100" "--keepalive-time-sec" "10"
     "--keepalive-timeout-sec" "5"`, stdout+stderr → a `NamedTempFile`;
     then `wait_for_server`.
   - `async fn wait_for_server(address: &str)` — 10s deadline, 250ms retry:
     connect a `Channel` (keepalive 1s/500ms), open `stream`, send one ping
     `sender="_health_check_"`, drain/EOF; any error → retry; timeout →
     panic with the fixture log tail.
   - `async fn run_client(address, client_id, interval_ms, duration)
     -> (u64, Vec<Pong>)` — in-process: mpsc request stream (sender task
     ticks until the deadline, counting pings in an `AtomicU64`), attach
     `client-id` metadata, collect pongs, break on `Error` payload or stream
     end; cancel + drop at the end. (Equivalent of Python `_run_client` /
     `_run_multi_client`.)
2. `tests/single_client.rs` — `#[tokio::test]` async tests; each starts its
   own fixture (function-scoped; dropped at test end):
   - `test_single_client_ping_pong` — `run_client(addr, "test_client_0",
     100, 5.0s)`; assert >0 pongs, all `sender == "server"`, all `id > 0`
     (failure message includes pings sent, like Python).
   - `test_single_client_count_pings_pongs` — pings > 0; pongs > 5.
   - `test_single_client_server_receives_pings` — after the client run,
     `tokio::time::sleep(2s)`; count log lines containing both
     `Server received ping` and `test_client_0`; assert > 0; on failure
     include the last 500 chars of the log.
3. `tests/multi_clients.rs` — helper
   `async fn run_three_clients(addr) -> Vec<(String, u64, Vec<Pong>)>`
   spawning 3 concurrent `run_client` tasks (`client_alpha`, `client_beta`,
   `client_gamma`, 150ms, 5.0s); each test starts its own fixture:
   - `test_multi_clients_all_receive_pongs` — every client >0 pongs, all
     `sender == "server"`.
   - `test_multi_clients_distinguish_streams` — sleep 3s; for each client id,
     >0 log lines with `Server received ping`; on failure include last 800
     chars of the log.
   - `test_multi_clients_total_pongs_exceed_single` — total > 0 and every
     client > 0.
   - `test_multi_clients_sees_monotonic_pongs` — per client, pong ids
     non-decreasing.
   - Delete the Step-4 placeholder test.

**Verification**

```bash
cd rust
cargo test -v                # 9 tests (3 + 4 + 1 + 1), all pass
make test
cargo test                   # second run — check for flakiness/state leakage
pgrep -f "target/debug/server" || echo "no leftover servers"   # after the run
```

**Definition of done:** All tests pass in `make test`, twice in a row; no
leftover server processes; also verify (keepalive sanity) that the 1s/500ms
test-client keepalive does **not** trip any server-side enforcement
(design decision 7) — visible as unexpected `Connection lost` lines or
flaky failures.

**Note on implementation:** The test's `run_client` must share a single `tx`/`rx`
channel between sender and receiver tasks — the sender writes to `tx` and the
server reads from `rx` (inside the request's `ReceiverStream`). The response
stream (separate) delivers pongs back. Additionally, the `tokio::select!`
for duration-based exit must use `biased` with `sleep_until(deadline)` to
ensure the duration timer fires correctly alongside the interval timer.

---

## Step 6 — Documentation + final end-to-end verification

**Goal:** `rust/README.md` mirroring the Python README, root README Rust
section, and a full parity pass.

**Read first:** `../python/README.md`, root `README.md`, all `rust/` files
created so far, this plan's ground-truth sections.

**Deliverables**

1. `rust/README.md` — mirror the structure of `../python/README.md`:
   quick start (`make build`, `make server` / `make server-foreground`,
   `make client TARGET=…`, `make kill`, `make test`; note that `make gen` is
   a no-op because `build.rs` regenerates from `api/resilience.proto` on
   every build and `protoc` is required);
   architecture diagram (proto → `build.rs`/`tonic-build` → OUT_DIR →
   `src/bin/server`, `src/bin/client`); argument tables (identical
   defaults); an implementation notes section translating the Python
   pieces to Rust (async tonic `Streaming` + mpsc request stream instead of
   generators; `tokio::signal` instead of `signal.signal`; tonic/hyper
   has no grpc-go-style keepalive enforcement; the pings-sent counting fix;
   function-scoped test fixtures vs Python's session scope);
   server log location `../temp/rust/server.log` + `tail -f`; file layout;
   troubleshooting (protoc missing, port in use → `make kill`, rebuild
   after proto changes).
2. Root `README.md` — add a `## Rust` usage section mirroring the existing
   Python/Go sections (`cd rust && make server`,
   `make client PING_INTERVAL_MS=… CLIENT_ID=…`, `make test`) and update the
   "no root-level Makefile" note to list all three sub-project Makefiles.
3. Final hygiene: `cargo fmt --all`, `cargo clippy --all-targets` (fix
   warnings), `cargo build --release` sanity check (optional).

**Verification (final parity pass)**

```bash
cd rust
make clean && make build
make server
make client TARGET=localhost:50051 CLIENT_ID=alpha PING_INTERVAL_MS=10000 &
make client TARGET=localhost:50051 CLIENT_ID=beta  PING_INTERVAL_MS=9500  &
sleep 20   # observe: per-client pings, independent per-stream pong counters,
           # interleaved [client]/[server] logs, no keepalive-induced disconnects
make kill
make test            # full integration suite green, twice
make help
# cross-language check: start the Rust server, then run the PYTHON client
# against it (and vice versa) — all ports speak the same wire protocol:
cd ../python && uv run python client.py localhost:50051 --client-id py_client
# and the Go port, if present:
cd ../go && ./bin/client localhost:50051 --client-id go_client
```

**Definition of done:** docs complete; every command from the README/plan
works as written; `make test` green twice in a row; Rust ↔ Python (and ↔ Go,
if present) interop smoke works (same API, `client-id` metadata read on both
sides).

---

## Sequencing summary

| Done | Step | Deliverable | Depends on |
|---|------|-------------|------------|
| ✓ | 1 | `Cargo.toml`, `build.rs`, `lib.rs`, generated API, `.gitignore` | — |
| ✓ | 2 | `src/bin/server.rs` + `src/logfmt.rs` (CLI + behavior parity) | 1 |
| ✓ | 3 | `src/bin/client.rs` (CLI + reconnect + signals) | 1, 2 |
| ✓ | 4 | Root `Makefile` (rust targets) + placeholder test | 2, 3 |
| ✓ | 5 | `tests/` — 9 integration tests (3 single + 4 multi + 1 quick + 1 placeholder) | 4 |
| ✓ | 6 | `rust/README.md`, root README (Rust section), `cargo fmt` + `cargo clippy` clean | 5 |
