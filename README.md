# grpc_test

This is an example repo the test the behavior of long living gRPC streaming
connections in case of network connection errors after the initial connection
was established.

The repo covers example implementations in multiple programming languages.

# API

All tests are using the same API for the tests. The protobuf file is here:
`./api/resilience.proto`

# Usage

## Running tests

```bash
cd python && make test
cd go     && make test
```

## Python

First, generate protobuf / gRPC bindings (one-time):

```bash
cd python && make gen
```

Then start the server and clients:

```bash
cd python

# start the server running in the foreground … it will send Pongs every 500ms by default
make server-foreground

# open a new terminal to run client 1
make client PING_INTERVAL_MS=10000 CLIENT_ID=client_1

# open another terminal to run client 2
make client PING_INTERVAL_MS=9500 CLIENT_ID=client_2
```

## Go

First, generate protobuf / gRPC bindings (one-time; already committed):

```bash
cd go && make gen
```

Then start the server and clients:

```bash
cd go

# build binaries (one-time, or after proto changes)
make build

# start the server running in the foreground
make server-foreground

# run client 1 (new terminal)
make client PING_INTERVAL_MS=10000 CLIENT_ID=client_1

# run client 2 (another terminal)
make client PING_INTERVAL_MS=9500 CLIENT_ID=client_2
```

### Running tests

```bash
cd go && make test
```

## Rust

```bash
# build (regenerates protobuf bindings from api/resilience.proto)
make build

# start the server in the background (port 50051)
make server

# run a client (in another terminal)
make client TARGET=localhost:50051 PING_INTERVAL_MS=500

# stop server / client
make kill

# run all integration tests
make test
```

### Running tests

```bash
cd go && make test
```

> **Note:** There is no root-level `Makefile`. All `make` commands must be run
> from the subdirectory (`python/`, `go/`, or `rust/`) that contains the desired
> implementation, or specify the path explicitly.

