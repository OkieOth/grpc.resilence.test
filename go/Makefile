# ─────────────────────────────────────────────────────────────────────
#  Makefile – ResilienceService (Go)
#
#  Targets:
#    gen        – generate protobuf / gRPC stubs
#    server     – start the gRPC server in the background
#    server-foreground – start the gRPC server in the foreground
#    client     – run the gRPC client
#    build      – build server and client binaries
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

# Ensure protoc-gen-go is on PATH
GOPATH_BIN := $(shell go env GOPATH)/bin
PATH := $(GOPATH_BIN):$(PATH)

PROTO  := ../api/resilience.proto
GO_API := api

# Test configuration
# seconds per test run.
TEST_TIMEOUT ?= 120

# ── Generate protobuf & gRPC Go bindings ────────────────────────────
.PHONY: gen
gen: $(GO_API)/resilience.pb.go $(GO_API)/resilience_grpc.pb.go

$(GO_API)/resilience.pb.go $(GO_API)/resilience_grpc.pb.go: $(PROTO)
	@mkdir -p $(GO_API)
	@echo "Generating Go gRPC bindings …"
	@protoc --proto_path=../api \
		--go_out=. --go_opt=module=github.com/example/grpc-resilience \
		--go-grpc_out=. --go-grpc_opt=module=github.com/example/grpc-resilience \
		$(PROTO)
	@echo "  → $(GO_API)/resilience.pb.go"
	@echo "  → $(GO_API)/resilience_grpc.pb.go"

# ── Build ───────────────────────────────────────────────────────────
.PHONY: build
build:
	@go build -o bin/server ./cmd/server
	@go build -o bin/client ./cmd/client
	@echo "Build complete."

# ── Server ──────────────────────────────────────────────────────────
.PHONY: server
server:
	@mkdir -p ../temp/go
	@./bin/server 50051 --ping-interval-ms 500 --keepalive-time-sec 10 --keepalive-timeout-sec 5 > ../temp/go/server.log 2>&1 &
	@sleep 1
	@echo "Server started (port 50051, keepalive 10s)"

# Run the server in the foreground (blocks until Ctrl-C / SIGINT).
.PHONY: server-foreground
server-foreground:
	@./bin/server 50051 --ping-interval-ms 500 --keepalive-time-sec 10 --keepalive-timeout-sec 5

# Default overrideable arguments (set via make command line, e.g. PING_INTERVAL_MS=2000)
CLIENT_ID         ?= client
PING_INTERVAL_MS  ?= 500
KEEPALIVE_TIME_SEC ?= 10
KEEPALIVE_TIMEOUT_SEC ?= 5

# ── Client ──────────────────────────────────────────────────────────
.PHONY: client
client:
	@./bin/client $(TARGET) \
		--client-id $(CLIENT_ID) \
		--ping-interval-ms $(PING_INTERVAL_MS) \
		--keepalive-time-sec $(KEEPALIVE_TIME_SEC) \
		--keepalive-timeout-sec $(KEEPALIVE_TIMEOUT_SEC)

# ── Kill processes ──────────────────────────────────────────────────
.PHONY: kill-server
kill-server:
	@pkill -f "bin/server" 2>/dev/null && echo "  Server stopped." || echo "  No server running."

.PHONY: kill-client
kill-client:
	@pkill -f "bin/client" 2>/dev/null && echo "  Client stopped." || echo "  No client running."

.PHONY: kill
kill: kill-server kill-client

# ── Clean build artifacts ───────────────────────────────────────────
.PHONY: clean
clean:
	@rm -rf bin/
	@rm -f ../temp/go/server.log
	@echo "Cleaned build artifacts."

# ── Integration Tests ──────────────────────────────────────────────
.PHONY: test
test:
	@go test ./tests/ -v -count=1 -timeout ${TEST_TIMEOUT}s

# ── Help ────────────────────────────────────────────────────────────
.PHONY: help
help:
	@echo "Targets:"
	@echo "  gen              Generate Go gRPC stubs from .proto"
	@echo "  build            Build server and client binaries"
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
