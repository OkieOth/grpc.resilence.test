# grpc_test

This is an example repo the test the behavior of long living gRPC streaming
connections in case of network connection errors after the initial connection
was established.

The repo covers example implementations in multiple programming languages.

# API

All tests are using the same API for the tests. The protobuf file is here:
`./api/resilience.proto`

# Usage

# Python

```bash
# start the server running in the foreground ... it will pong all 20s
make server-foreground

# open a new terminal to run client 1
make client PING_INTERVAL_MS=10000 CLIENT_ID=client_1

# open another terminal to run client 2
make client PING_INTERVAL_MS=9500 CLIENT_ID=client_1
```
