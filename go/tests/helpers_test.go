package tests

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"

	pb "github.com/example/grpc-resilience/api"
)

// ── freePort returns a random free TCP port. ─────────────────────────

func freePort() (int, error) {
	lis, err := net.Listen("tcp", "[::]:0")
	if err != nil {
		return 0, err
	}
	port := lis.Addr().(*net.TCPAddr).Port
	lis.Close()
	return port, nil
}

// ── waitForServer blocks until the gRPC server accepts connections. ──

func waitForServer(address string) error {
	conn, err := grpc.NewClient(address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                1 * time.Second,
			Timeout:             500 * time.Millisecond,
			PermitWithoutStream: true,
		}),
	)
	if err != nil {
		return err
	}
	defer conn.Close()

	stub := pb.NewResilienceServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	streamCtx := metadata.AppendToOutgoingContext(ctx, "client-id", "_health_check_")

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		stream, streamErr := stub.Stream(streamCtx)
		if streamErr == nil {
			req := &pb.BidirectionalStreamRequest{
				Ping: &pb.Ping{Id: 0, Sender: "_health_check_"},
			}
			sendErr := stream.Send(req)
			if sendErr == nil {
				stream.CloseSend()
				for {
					_, recvErr := stream.Recv()
					if recvErr == io.EOF || (recvErr != nil && strings.HasSuffix(recvErr.Error(), "EOF")) {
						return nil
					}
					if recvErr != nil {
						break
					}
				}
			}
		}
		time.Sleep(250 * time.Millisecond)
	}
	return fmt.Errorf("server did not become ready within 10s")
}

// ── readServerLog reads the full server log from the temp file. ─────

func readServerLog(logPath string) (string, error) {
	data, err := os.ReadFile(logPath)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

// ── Server fixture ───────────────────────────────────────────────────

// serverFixture holds info about the test server subprocess.
type serverFixture struct {
	Address string
	LogPath string
	Proc    *exec.Cmd
}

func startServer() (*serverFixture, error) {
	port, err := freePort()
	if err != nil {
		return nil, fmt.Errorf("failed to find free port: %w", err)
	}
	address := fmt.Sprintf("localhost:%d", port)

	// Create a temp directory for test artifacts.
	tmpDir, err := os.MkdirTemp("", "grpc-resilience-test-*")
	if err != nil {
		return nil, fmt.Errorf("failed to create temp dir: %w", err)
	}
	logPath := filepath.Join(tmpDir, "server.log")
	binPath := filepath.Join(tmpDir, "server")

	// Ensure server binary exists in the temp location.
	if _, err := os.Stat(binPath); os.IsNotExist(err) {
		// Try the pre-built binary first (from `make build`).
		if _, err := os.Stat("../bin/server"); err == nil {
			cmd := exec.Command("cp", "../bin/server", binPath)
			if err := cmd.Run(); err != nil {
				return nil, fmt.Errorf("failed to copy server binary: %w", err)
			}
		} else {
			// Fallback: build from the go/ directory.
			buildCmd := exec.Command("go", "build", "-o", binPath, "./cmd/server")
			buildCmd.Dir = ".."
			if err := buildCmd.Run(); err != nil {
				return nil, fmt.Errorf("failed to build server: %w", err)
			}
		}
	}

	// Open log file for the server's stdout/stderr.
	logFile, err := os.Create(logPath)
	if err != nil {
		return nil, fmt.Errorf("failed to create log file: %w", err)
	}

	args := []string{fmt.Sprintf("%d", port),
		"--ping-interval-ms", "100",
		"--keepalive-time-sec", "10",
		"--keepalive-timeout-sec", "5"}

	// Start the server using exec.Command which properly manages file descriptors.
	serverCmd := exec.Command(binPath, args...)
	serverCmd.Dir = ".."
	serverCmd.Stdout = logFile
	serverCmd.Stderr = logFile

	if err := serverCmd.Start(); err != nil {
		logFile.Close()
		os.RemoveAll(tmpDir)
		return nil, fmt.Errorf("failed to start server: %w", err)
	}

	// Brief pause then check the log.
	time.Sleep(2 * time.Second)
	_ = logPath // log written to file for tests to read.

	// Wait for server to be ready.
	if err := waitForServer(address); err != nil {
		serverCmd.Process.Kill()
		serverCmd.Wait()
		logFile.Close()
		os.RemoveAll(tmpDir)
		return nil, fmt.Errorf("server failed to start on port %d: %w", port, err)
	}

	return &serverFixture{
		Address: address,
		LogPath: logPath,
		Proc:    serverCmd,
	}, nil
}

func stopServer(f *serverFixture) {
	if f == nil || f.Proc == nil || f.Proc.Process == nil {
		return
	}
	f.Proc.Process.Signal(os.Interrupt)
	done := make(chan error, 1)
	go func() {
		done <- f.Proc.Wait()
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		f.Proc.Process.Kill()
		f.Proc.Wait()
	}
}

func cleanupServer(f *serverFixture) {
	if f != nil {
		os.RemoveAll(filepath.Dir(f.LogPath))
	}
}
