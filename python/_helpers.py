"""Shared helpers for gRPC integration tests."""


def read_server_log(grpc_server_fixture) -> str:
    """Return the full server log buffer (reading from the temp file).

    Must be called with the session-scoped *grpc_server* fixture:

    .. code-block:: python

        def test_something(grpc_server):
            log = read_server_log(grpc_server)
            assert "Server received ping" in log
    """
    log_path = getattr(grpc_server_fixture, "_log_path", None)  # type: ignore[arg-type]
    if log_path is None:
        return ""
    with open(log_path) as fh:
        return fh.read()
