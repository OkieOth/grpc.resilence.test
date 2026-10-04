package tests

import (
	"fmt"
	"os"
	"testing"
)

// globalFixture holds the server fixture for all tests (session-scoped).
var globalFixture *serverFixture

func serverAddress() string {
	if globalFixture == nil {
		panic("server not started — TestMain must be used")
	}
	return globalFixture.Address
}

func serverLogContent() (string, error) {
	return readServerLog(globalFixture.LogPath)
}

func TestMain(m *testing.M) {
	// Start the server once for the entire test suite.
	var err error
	globalFixture, err = startServer()
	if err != nil {
		fmt.Fprintf(os.Stderr, "FATAL: %s\n", err)
		os.Exit(1)
	}

	// Run all tests.
	code := m.Run()

	// Teardown: stop the server and clean up.
	stopServer(globalFixture)
	cleanupServer(globalFixture)

	os.Exit(code)
}
