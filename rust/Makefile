# ─────────────────────────────────────────────────────────────────────
#  Makefile – ResilienceService (Rust)
#
#  Targets:
#    gen        – generate protobuf / gRPC bindings (no-op; build.rs handles it)
#    build      – compile server and client binaries (and regenerate protos)
#    server     – start the gRPC server in the background
#    server-foreground – start the gRPC server in the foreground
#    client     – run the gRPC client
#    test       – run integration tests
#    clean      – remove build artifacts
#    kill-server – stop any running server
#    kill-client – stop any running client
#    kill       – stop server and client
#    help       – show this help message
#
#  Protobuf file is at the repo root.
# ─────────────────────────────────────────────────────────────────────

SHELL := /bin/bash

# The server / client binaries live under rust/target/debug/.
SERVER_BIN ?= target/debug/server
CLIENT_BIN ?= target/debug/client

PROTO      := ../api/resilience.proto

# Test configuration (seconds per test run).
TEST_TIMEOUT ?= 120

# ── Generate protobuf (no-op) ──────────────────────────────────────
# Rust's build.rs regenerates bindings on every cargo build, so there is
# no standalone `gen` step.
.PHONY: gen
gen:
	@echo "  (Rust: build.rs regenerates bindings on every 'cargo build')"

# ── Build ───────────────────────────────────────────────────────────
.PHONY: build
build:
	@cd rust && cargo build

# ── Server ──────────────────────────────────────────────────────────
.PHONY: server
server:
	@mkdir -p ../temp/rust
	@./$(SERVER_BIN) 50051 --ping-interval-ms 500 --keepalive-time-sec 10 --keepalive-timeout-sec 5 > ../temp/rust/server.log 2>&1 &
	@sleep 1
	@echo "  Server started (port 50051, keepalive 10s)"

# Run the server in the foreground (blocks until Ctrl-C / SIGINT).
.PHONY: server-foreground
server-foreground:
	@./$(SERVER_BIN) 50051 --ping-interval-ms 500 --keepalive-time-sec 10 --keepalive-timeout-sec 5

# Default overrideable arguments (set via make command line, e.g. PING_INTERVAL_MS=2000)
CLIENT_ID         ?= client
PING_INTERVAL_MS  ?= 500
KEEPALIVE_TIME_SEC ?= 10
KEEPALIVE_TIMEOUT_SEC ?= 5

# ── Client ──────────────────────────────────────────────────────────
.PHONY: client
client:
	@./$(CLIENT_BIN) $(TARGET) \
		--client-id $(CLIENT_ID) \
		--ping-interval-ms $(PING_INTERVAL_MS) \
		--keepalive-time-sec $(KEEPALIVE_TIME_SEC) \
		--keepalive-timeout-sec $(KEEPALIVE_TIMEOUT_SEC)

# ── Kill processes ──────────────────────────────────────────────────
.PHONY: kill-server
kill-server:
	@pkill -f "target/debug/server" 2>/dev/null && echo "  Server stopped." || echo "  No server running."

.PHONY: kill-client
kill-client:
	@pkill -f "target/debug/client" 2>/dev/null && echo "  Client stopped." || echo "  No client running."

.PHONY: kill
kill: kill-server kill-client

# ── Clean build artifacts ───────────────────────────────────────────
.PHONY: clean
clean:
	@cargo clean
	@rm -rf ../temp/rust/server.log
	@echo "Cleaned build artifacts."

# ── Integration Tests ──────────────────────────────────────────────
.PHONY: test
test:
	@cargo test

# ── Help ────────────────────────────────────────────────────────────
.PHONY: help
help:
	@echo "Targets:"
	@echo "  gen              Generate protobuf / gRPC bindings (no-op; build.rs handles it)"
	@echo "  build            Compile server and client binaries (and regenerate protos)"
	@echo "  server           Start the gRPC server (port 50051)"
	@echo "  server-foreground Start the gRPC server in the foreground (blocks)"
	@echo "  client           Run the gRPC client (requires TARGET)"
	@echo "  kill-server      Stop only the running server"
	@echo "  kill-client      Stop only the running client"
	@echo "  kill             Stop server and client"
	@echo "  test             Run integration tests"
	@echo "  clean            Remove build artifacts"
	@echo "  help             Show this message"
	@echo ""
	@echo "  Client flags (overridable on the command line):"
	@echo "    CLIENT_ID            client"
	@echo "    PING_INTERVAL_MS     500"
	@echo "    KEEPALIVE_TIME_SEC   10"
	@echo "    KEEPALIVE_TIMEOUT_SEC  5"
	@echo ""
	@echo "  Examples:"
	@echo "    make client TARGET=localhost:50051"
	@echo "    make client TARGET=localhost:50051 CLIENT_ID=alpha PING_INTERVAL_MS=2000"
	@echo "    make client TARGET=localhost:50051 CLIENT_ID=beta"
