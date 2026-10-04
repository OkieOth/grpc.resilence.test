package tests

import (
	"context"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"

	pb "github.com/example/grpc-resilience/api"
)

// ── Single client helpers ────────────────────────────────────────────

// runClient runs a single client against the server for the specified duration.
// Returns (pingsSent, pongsReceived).
func runClient(t *testing.T, target string, clientID string,
	pingIntervalMs int, duration time.Duration) (int, []*pb.Pong) {

	pingsSent := 0
	var pongsReceived []*pb.Pong

	keepaliveOpts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                1 * time.Second,
			Timeout:             500 * time.Millisecond,
			PermitWithoutStream: true,
		}),
	}

	conn, err := grpc.NewClient(target, keepaliveOpts...)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	stub := pb.NewResilienceServiceClient(conn)
	ctx, cancel := context.WithCancel(context.Background())
	ctx = metadata.AppendToOutgoingContext(ctx, "client-id", clientID)
	stream, err := stub.Stream(ctx)
	if err != nil {
		t.Fatalf("Failed to create stream: %v", err)
	}

	// Sender goroutine: sends pings at configured interval until duration expires.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		pid := int32(0)
		ticker := time.NewTicker(time.Duration(pingIntervalMs) * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				pid++
				pingsSent++
				req := &pb.BidirectionalStreamRequest{
					Ping: &pb.Ping{Id: pid, Sender: clientID},
				}
				if err := stream.Send(req); err != nil {
					return
				}
			}
		}
	}()

	// Wait for duration, then close send side to unblock the server.
	time.Sleep(duration)
	stream.CloseSend()

	// Recv loop: collect pongs, break on error payload or stream end.
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			// Treat gRPC-wrapped EOF the same.
			if strings.HasSuffix(err.Error(), "EOF") {
				break
			}
			break
		}
		switch p := resp.Payload.(type) {
		case *pb.BidirectionalStreamResponse_Pong:
			pongsReceived = append(pongsReceived, p.Pong)
		case *pb.BidirectionalStreamResponse_Error:
			goto done
		}
	}

done:
	// Wait for sender, then cancel.
	wg.Wait()
	cancel()
	return pingsSent, pongsReceived
}

// ── Shared scenario (sync.Once) ──────────────────────────────────────

var singleOnce sync.Once
var singlePings int
var singlePongs []*pb.Pong

// runSingleClient runs the single-client scenario once.
func runSingleClient() {
	addr := serverAddress()
	singlePings, singlePongs = runClient(&testing.T{}, addr, "test_client_0", 100, 5*time.Second)
}

// ── Tests ────────────────────────────────────────────────────────────

func TestSingleClientPingPong(t *testing.T) {
	singleOnce.Do(runSingleClient)

	if len(singlePongs) == 0 {
		t.Fatalf("Expected server pongs but received none (pings sent = %d)", singlePings)
	}
	for _, pong := range singlePongs {
		if pong.Sender != "server" {
			t.Errorf("Expected pong sender 'server', got '%s'", pong.Sender)
		}
		if pong.Id <= 0 {
			t.Errorf("Expected pong id > 0, got %d", pong.Id)
		}
	}
}

func TestSingleClientCountPingsPongs(t *testing.T) {
	singleOnce.Do(runSingleClient)

	pongs := len(singlePongs)
	if singlePings == 0 {
		t.Fatal("Client should have sent at least one ping")
	}
	if pongs <= 5 {
		t.Fatalf("Expected >5 server pongs over 5s (got %d pongs from %d pings)", pongs, singlePings)
	}
}

func TestSingleClientServerReceivesPings(t *testing.T) {
	singleOnce.Do(runSingleClient)

	// Wait a couple seconds for pings to accumulate in the log.
	time.Sleep(2 * time.Second)

	log, err := readServerLog(globalFixture.LogPath)
	if err != nil {
		t.Fatalf("Failed to read server log: %v", err)
	}

	// The server outputs: "Server received ping #N from test_client_0"
	pingCount := 0
	for _, line := range strings.Split(log, "\n") {
		if strings.Contains(line, "test_client_0") && strings.Contains(line, "Server received ping") {
			pingCount++
		}
	}
	if pingCount == 0 {
		t.Fatalf("Server did not log any pings from test_client_0.\nServer log:\n%s", log)
	}
}
