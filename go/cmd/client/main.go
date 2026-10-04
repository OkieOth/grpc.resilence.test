// ResilienceService gRPC client – bidirectional streaming ping/pong.

package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"

	pb "github.com/example/grpc-resilience/api"
)

const (
	maxRetries     = 10
	reconnectDelay = 1.0 * time.Second
)

// ── Logging helper (matches Python log format) ───────────────────────

type clientLog struct {
	logger *log.Logger
	level  string
}

func newClientLog() *clientLog {
	return &clientLog{
		logger: log.New(os.Stderr, "", 0),
		level:  "INFO",
	}
}

func (l *clientLog) log(prefix, level, format string, args ...any) {
	levelOrder := map[string]int{"INFO": 0, "WARNING": 1, "ERROR": 2}
	if levelOrder[level] > levelOrder[l.level] {
		return
	}
	ts := time.Now().Format("2006-01-02 15:04:05")
	msg := fmt.Sprintf(format, args...)
	l.logger.Output(3, fmt.Sprintf("[%s] %s %s %s", prefix, ts, level, msg))
}

func (l *clientLog) Info(format string, args ...any)    { l.log("client", "INFO", format, args...) }
func (l *clientLog) Warning(format string, args ...any) { l.log("client", "WARNING", format, args...) }
func (l *clientLog) Error(format string, args ...any)   { l.log("client", "ERROR", format, args...) }

// ── CLI argument parsing ─────────────────────────────────────────────

// parseClientArgs collects named flags and positionals.
// The first positional is target (default: localhost:50051).
func parseClientArgs(rawArgs []string) (target string, clientID string,
	pingIntervalMs int, keepaliveTimeSec int, keepaliveTimeoutSec int) {
	// Defaults.
	clientID = "client"
	pingIntervalMs = 500
	keepaliveTimeSec = 10
	keepaliveTimeoutSec = 5

	var flags map[string]string
	var positionals []string
	i := 0
	for i < len(rawArgs) {
		arg := rawArgs[i]
		if strings.HasPrefix(arg, "-") && len(arg) > 1 {
			if flags == nil {
				flags = make(map[string]string)
			}
			key := strings.TrimLeft(arg, "-")
			if i+1 < len(rawArgs) && !strings.HasPrefix(rawArgs[i+1], "-") {
				flags[key] = rawArgs[i+1]
				i += 2
			} else {
				flags[key] = ""
				i++
			}
		} else {
			positionals = append(positionals, arg)
			i++
		}
	}

	// Apply flag overrides.
	if v, ok := flags["client-id"]; ok && v != "" {
		clientID = v
	}
	if v, ok := flags["ping-interval-ms"]; ok && v != "" {
		fmt.Sscanf(v, "%d", &pingIntervalMs)
	}
	if v, ok := flags["keepalive-time-sec"]; ok && v != "" {
		fmt.Sscanf(v, "%d", &keepaliveTimeSec)
	}
	if v, ok := flags["keepalive-timeout-sec"]; ok && v != "" {
		fmt.Sscanf(v, "%d", &keepaliveTimeoutSec)
	}

	// First positional is target (override default).
	if len(positionals) > 0 {
		target = positionals[0]
	} else {
		target = "localhost:50051"
	}

	return
}

// ── Client implementation ────────────────────────────────────────────

type resilienceClient struct {
	target    string
	clientID  string
	slog      *clientLog
	pingsSent atomic.Int64
	pongsRecv atomic.Int64
}

func (c *resilienceClient) run(pingIntervalMs int, keepaliveTimeSec int,
	keepaliveTimeoutSec int) {

	pingInterval := time.Duration(pingIntervalMs) * time.Millisecond

	keepaliveOpts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                time.Duration(keepaliveTimeSec) * time.Second,
			Timeout:             time.Duration(keepaliveTimeoutSec) * time.Second,
			PermitWithoutStream: true,
		}),
	}

	attempt := 0
	for {
		attempt++
		if attempt > 1 {
			c.slog.Info("Reconnecting (attempt %d/%d) …", attempt, maxRetries)
			time.Sleep(reconnectDelay)
		}

		// Create context with signal handling.
		ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)

		// Create connection.
		conn, connErr := grpc.NewClient(c.target, keepaliveOpts...)
		if connErr != nil {
			c.slog.Error("Connection error (attempt %d): %s", attempt, connErr.Error())
			cancel()
			if attempt >= maxRetries {
				c.slog.Info("Max retries (%d) reached.", maxRetries)
				break
			}
			continue
		}

		stub := pb.NewResilienceServiceClient(conn)
		streamCtx := metadata.AppendToOutgoingContext(ctx, "client-id", c.clientID)

		// Create bidirectional stream.
		stream, streamErr := stub.Stream(streamCtx)

		// Handle stream error.
		if streamErr != nil {
			if connErr == nil {
				c.slog.Error("RPC error (attempt %d): %s – %s",
					attempt, codes.Unknown.String(), streamErr.Error())
			} else {
				c.slog.Error("Connection error (attempt %d): %s", attempt, connErr.Error())
			}
			cancel()
			if attempt >= maxRetries {
				c.slog.Info("Max retries (%d) reached.", maxRetries)
				break
			}
			continue
		}

		// Both connection and stream are good — run sender and receiver.
		pingTimer := time.NewTimer(pingInterval)
		pingID := int32(0)
		var wg sync.WaitGroup
		wg.Add(1)

		// Sender goroutine: sends pings at configured interval.
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					pingTimer.Stop()
					return
				case <-pingTimer.C:
					pingID++
					c.pingsSent.Add(1)
					c.slog.Info("Client sending ping #%d", pingID)
					req := &pb.BidirectionalStreamRequest{
						Ping: &pb.Ping{Id: pingID, Sender: c.clientID},
					}
					if sendErr := stream.Send(req); sendErr != nil {
						pingTimer.Stop()
						return
					}
					pingTimer.Reset(pingInterval)
				}
			}
		}()

		// Recv loop: collect pongs, break on error payload or stream end.
		for {
			resp, recvErr := stream.Recv()
			if recvErr == io.EOF {
				break
			}
			if recvErr != nil {
				c.slog.Error("RPC error (attempt %d): %s – %s",
					attempt, codes.Unknown.String(), recvErr.Error())
				break
			}

			// Handle response payload.
			switch p := resp.Payload.(type) {
			case *pb.BidirectionalStreamResponse_Pong:
				c.slog.Info("Client received Pong #%d from %s", p.Pong.Id, p.Pong.Sender)
				c.pongsRecv.Add(1)
			case *pb.BidirectionalStreamResponse_Error:
				c.slog.Warning("Client received Error #%d: code=%d msg=%s",
					p.Error.Id, p.Error.Code, p.Error.Message)
			}
		}

		// Wait for sender, then clean up.
		pingTimer.Stop()
		wg.Wait()
		cancel()
		conn.Close()

		if attempt >= maxRetries {
			c.slog.Info("Max retries (%d) reached.", maxRetries)
			break
		}
	}

	c.slog.Info("Done. Attempt=%d  Pings sent=%d  Pons received=%d",
		attempt, c.pingsSent.Load(), c.pongsRecv.Load())
}

// ── Main ─────────────────────────────────────────────────────────────

func main() {
	target, clientID, pingIntervalMs, keepaliveTimeSec, keepaliveTimeoutSec :=
		parseClientArgs(os.Args[1:])

	client := &resilienceClient{
		target:   target,
		clientID: clientID,
		slog:     newClientLog(),
	}

	// Signal handling.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		if s, ok := sig.(syscall.Signal); ok {
			client.slog.Info("Signal %d received, shutting down …", int(s))
		}
		client.slog.Info("Shutting down.")
		client.slog.Info("Done. Attempt=%d  Pings sent=%d  Pons received=%d",
			0, client.pingsSent.Load(), client.pongsRecv.Load())
		os.Exit(0)
	}()

	client.run(pingIntervalMs, keepaliveTimeSec, keepaliveTimeoutSec)
}
