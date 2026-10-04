# ─────────────────────────────────────────────────────────────────────
#  Makefile – ResilienceService (Python)
#
#  Targets:
#    gen        – generate protobuf / gRPC stubs
#    server     – start the gRPC server in the background  (server.py)
#    run        – start the gRPC server in the foreground
#    client     – run the gRPC client    (client.py)
#    test       – run integration tests
#    clean      – remove generated files and bytecode
#    kill-server – stop any running server
#    kill-client – stop any running client
#    kill       – stop server and client
#    help       – show this help message
#
#  The server and client now support bidirectional streaming: both
#  endpoints send messages continuously at configurable intervals.
# ─────────────────────────────────────────────────────────────────────

SHELL := /bin/bash

# The proto file is always at the repo root.
PROTO  := ../api/resilience.proto
PY_API := api

# Test configuration
TEST_TIMEOUT ?= 60  # seconds per test

# ── Generate protobuf & gRPC Python bindings ────────────────────────
.PHONY: gen
gen: $(PY_API)/resilience_pb2.py $(PY_API)/resilience_pb2_grpc.py

$(PY_API)/resilience_pb2.py $(PY_API)/resilience_pb2_grpc.py: $(PROTO)
	@mkdir -p $(PY_API)
	@echo "Generating Python gRPC bindings …"
	@uv run python -m grpc_tools.protoc \
		--proto_path=../api \
		--python_out=$(PY_API) \
		--grpc_python_out=$(PY_API) \
		$(PROTO)
	@sed -i '' 's|^import resilience_pb2 |from api import resilience_pb2 |' $(PY_API)/resilience_pb2_grpc.py
	@echo "  → $(PY_API)/resilience_pb2.py"
	@echo "  → $(PY_API)/resilience_pb2_grpc.py"

# ── Server ──────────────────────────────────────────────────────────
.PHONY: server
server:
	@uv run python server.py 50051 --ping-interval-ms 500 --keepalive-time-sec 10 --keepalive-timeout-sec 5 > ../temp/server.log 2>&1 &
	@sleep 1
	@echo "Server started (port 50051, keepalive 10s)"

# Run the server in the foreground (blocks until Ctrl-C / SIGINT).
.PHONY: run
server-foreground:
	@uv run python server.py 50051 --ping-interval-ms 500 --keepalive-time-sec 10 --keepalive-timeout-sec 5

# Default overrideable arguments (set via make command line, e.g. PING_INTERVAL_MS=2000)
CLIENT_ID         ?= client
PING_INTERVAL_MS  ?= 500
KEEPALIVE_TIME_SEC ?= 10
KEEPALIVE_TIMEOUT_SEC ?= 5

# ── Client ──────────────────────────────────────────────────────────
.PHONY: client
client:
	@uv run python client.py $(TARGET) \
		--client-id $(CLIENT_ID) \
		--ping-interval-ms $(PING_INTERVAL_MS) \
		--keepalive-time-sec $(KEEPALIVE_TIME_SEC) \
		--keepalive-timeout-sec $(KEEPALIVE_TIMEOUT_SEC)

# ── Kill processes ──────────────────────────────────────────────────
# Kill only the running server (matches “python server.py”).
.PHONY: kill-server
kill-server:
	@pkill -f "python server.py" 2>/dev/null && echo "  Server stopped." || echo "  No server running."

# Kill only the running client(s) (matches “python client.py”).
.PHONY: kill-client
kill-client:
	@pkill -f "python client.py" 2>/dev/null && echo "  Client stopped." || echo "  No client running."

# Kill server and client.
.PHONY: kill
kill: kill-server kill-client

# ── Clean generated artifacts ───────────────────────────────────────
.PHONY: clean
clean:
	@rm -f $(PY_API)/resilience_pb2.py
	@rm -f $(PY_API)/resilience_pb2_grpc.py
	@rm -rf ../temp/server.log
	@echo "Cleaned generated files."

# ── Integration Tests ──────────────────────────────────────────────
.PHONY: test
test:
	@uv run pytest tests/ -v -x --timeout=$(TEST_TIMEOUT) --tb=short

# ── Help ────────────────────────────────────────────────────────────
.PHONY: help
help:
	@echo "Targets:"
	@echo "  gen            Generate Python gRPC stubs from .proto"
	@echo "  server         Start the gRPC server (port 50051)"
	@echo "  run            Start the gRPC server in the foreground (blocks)"
	@echo "  client         Run the gRPC client (requires TARGET)"
	@echo "  kill-server    Stop only the running server"
	@echo "  kill-client    Stop only the running client"
	@echo "  kill           Stop server and client"
	@echo "  test           Run integration tests"
	@echo "  clean          Remove generated files"
	@echo "  help           Show this message"
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
