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

> **Note:** There is no root-level `Makefile`. All `make` commands must be run
> from the `python/` subdirectory (or specify the path explicitly).
> Alternatively, use `uv run --project python/` to run scripts directly.
