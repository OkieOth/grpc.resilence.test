# ─────────────────────────────────────────────────────────────────────
#  Makefile – ResilienceService (Rust)
#
#  Targets:
#    gen            Generate protobuf / gRPC Rust bindings (no-op)
#    build          Build the Rust project (debug profile)
#    server         Start the gRPC server in the background
#    server-foreground  Start the gRPC server in the foreground (blocks)
#    client         Run the gRPC client
#    kill-server    Stop any running server
#    kill-client    Stop any running client
#    kill           Stop server and client
#    test           Run integration tests
#    clean          Remove generated files and bytecode
#    help           Show this help message
#
#  The server and client now support bidirectional streaming: both
#  endpoints send messages continuously at configurable intervals.
# ─────────────────────────────────────────────────────────────────────

SHELL := /bin/bash

# Test configuration
TEST_TIMEOUT ?= 60  # seconds per test

# ── Generate protobuf & gRPC Rust bindings ──────────────────────────
.PHONY: gen
gen:
	@echo "Rust gRPC codegen runs automatically via build.rs on every cargo build."
	@echo "Requires protoc on PATH."
	@touch build.rs 2>/dev/null || true

# ── Build ───────────────────────────────────────────────────────────
.PHONY: build
build:
	@cd rust && cargo build

# ── Server ──────────────────────────────────────────────────────────
.PHONY: server
server: build
	@mkdir -p ../temp/rust
	@cd rust && $(SERVER_BIN) 50051 --ping-interval-ms 500 --keepalive-time-sec 10 --keepalive-timeout-sec 5 > ../temp/rust/server.log 2>&1 &
	@sleep 1
	@echo "Server started (port 50051, keepalive 10s)"

# Run the server in the foreground (blocks until Ctrl-C / SIGINT).
.PHONY: server-foreground
server-foreground: build
	@cd rust && $(SERVER_BIN) 50051 --ping-interval-ms 500 --keepalive-time-sec 10 --keepalive-timeout-sec 5

# Default overrideable arguments (set via make command line, e.g. PING_INTERVAL_MS=2000)
CLIENT_ID         ?= client
PING_INTERVAL_MS  ?= 500
KEEPALIVE_TIME_SEC ?= 10
KEEPALIVE_TIMEOUT_SEC ?= 5

# ── Client ──────────────────────────────────────────────────────────
.PHONY: client
client: build
	@cd rust && $(CLIENT_BIN) $(TARGET) \
		--client-id $(CLIENT_ID) \
		--ping-interval-ms $(PING_INTERVAL_MS) \
		--keepalive-time-sec $(KEEPALIVE_TIME_SEC) \
		--keepalive-timeout-sec $(KEEPALIVE_TIMEOUT_SEC)

# ── Kill processes ──────────────────────────────────────────────────
# Kill only the running server (matches "target/debug/server").
.PHONY: kill-server
kill-server:
	@pkill -f "rust/target/debug/server" 2>/dev/null && echo "  Server stopped." || echo "  No server running."

# Kill only the running client(s) (matches "target/debug/client").
.PHONY: kill-client
kill-client:
	@pkill -f "rust/target/debug/client" 2>/dev/null && echo "  Client stopped." || echo "  No client running."

# Kill server and client.
.PHONY: kill
kill: kill-server kill-client

# ── Clean generated artifacts ───────────────────────────────────────
.PHONY: clean
clean:
	@cd rust && cargo clean
	@rm -rf ../temp/rust/server.log
	@echo "Cleaned."

# ── Integration Tests ──────────────────────────────────────────────
.PHONY: test
test: build
	@cd rust && cargo test

# ── Help ────────────────────────────────────────────────────────────
.PHONY: help
help:
	@echo "Targets:"
	@echo "  gen                Generate Rust gRPC bindings (no-op via build.rs)"
	@echo "  build              Build the Rust project (debug profile)"
	@echo "  server             Start the gRPC server (port 50051)"
	@echo "  server-foreground  Start the gRPC server in the foreground (blocks)"
	@echo "  client             Run the gRPC client (requires TARGET)"
	@echo "  kill-server        Stop only the running server"
	@echo "  kill-client        Stop only the running client"
	@echo "  kill               Stop server and client"
	@echo "  test               Run integration tests"
	@echo "  clean              Remove build artifacts"
	@echo "  help               Show this message"
	@echo ""
	@echo "  Client flags (overridable on the command line):"
	@echo "    CLIENT_ID            client"
	@echo "    PING_INTERVAL_MS     500"
	@echo "    KEEPALIVE_TIME_SEC   10"
	@echo "    KEEPALIVE_TIMEOUT_SEC  5"
	@echo ""
	@echo "  Examples:"
	@echo "    make client TARGET=localhost:50051"
	@echo "    make client 'TARGET=localhost:50051' CLIENT_ID=alpha PING_INTERVAL_MS=2000"
	@echo "    make client 'TARGET=localhost:50051' CLIENT_ID=beta"
