// ResilienceService gRPC server – bidirectional streaming ping/pong.

package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"

	pb "github.com/example/grpc-resilience/api"
)

// ── Logging helper (matches Python log format) ───────────────────────

type serverLog struct {
	logger *log.Logger
	level  string
}

func newServerLog() *serverLog {
	return &serverLog{
		logger: log.New(os.Stderr, "", 0),
		level:  "INFO",
	}
}

func (l *serverLog) log(prefix, level, format string, args ...any) {
	// Map log levels: Python uses "WARNING" (not "WARN").
	// Only emit messages at or above the configured level.
	levelOrder := map[string]int{"DEBUG": 0, "INFO": 1, "WARNING": 2, "ERROR": 3}
	if levelOrder[level] > levelOrder[l.level] {
		return
	}
	ts := time.Now().Format("2006-01-02 15:04:05")
	msg := fmt.Sprintf(format, args...)
	l.logger.Output(3, fmt.Sprintf("[%s] %s %s %s", prefix, ts, level, msg))
}

func (l *serverLog) Info(format string, args ...any)    { l.log("server", "INFO", format, args...) }
func (l *serverLog) Warning(format string, args ...any) { l.log("server", "WARNING", format, args...) }
func (l *serverLog) Error(format string, args ...any)   { l.log("server", "ERROR", format, args...) }
func (l *serverLog) Debug(format string, args ...any)   { l.log("server", "DEBUG", format, args...) }

// ── Server implementation ────────────────────────────────────────────

type resilienceServer struct {
	pb.UnimplementedResilienceServiceServer
	log          *serverLog
	pingInterval time.Duration
}

// peer returns a human-readable peer address from the gRPC context.
func peer(ctx context.Context) string {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "<unknown>"
	}
	if peers := md["peer"]; len(peers) > 0 {
		return peers[0]
	}
	return "<unknown>"
}

// Stream handles a single bidirectional ping/pong stream.
func (s *resilienceServer) Stream(stream pb.ResilienceService_StreamServer) error {
	// Extract client_id from gRPC metadata (set by client on call).
	clientID := ""
	if md, ok := metadata.FromIncomingContext(stream.Context()); ok {
		if vals := md["client-id"]; len(vals) > 0 {
			clientID = vals[0]
		}
	}

	var serverPingID int32
	var lastSendTime time.Time

	for {
		resp, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				s.log.Info("Connection closed (client=%s, peer=%s)", clientID, peer(stream.Context()))
			} else {
				s.log.Warning("Connection lost (client=%s, peer=%s, reason=%s)",
					clientID, peer(stream.Context()), err.Error())
			}
			return err
		}

		// Log received ping.
		s.log.Info("Server received ping #%d from %s", resp.Ping.Id, resp.Ping.Sender)

		// Interval-gated pong sending (same logic as Python).
		now := time.Now()
		elapsed := now.Sub(lastSendTime)
		if elapsed >= s.pingInterval || lastSendTime.IsZero() {
			serverPingID++
			lastSendTime = now
			s.log.Info("Server sending Pong #%d to %s", serverPingID, resp.Ping.Sender)
			if err := stream.Send(&pb.BidirectionalStreamResponse{
				Payload: &pb.BidirectionalStreamResponse_Pong{
					Pong: &pb.Pong{Id: serverPingID, Sender: "server"},
				},
			}); err != nil {
				return err
			}
		} else {
			s.log.Debug("Skipping response (elapsed=%.3fs < %fs)", elapsed.Seconds(), s.pingInterval.Seconds())
		}
	}
}

// parseServerArgs manually parses CLI arguments so both orderings work:
//
//	server 50051 --ping-interval-ms 100    (positional before flags)
//	server --ping-interval-ms 100 50051    (flags before positional)
//
// Python's argparse handles both; Go's flag package does not.
func parseServerArgs(rawArgs []string) (port int, pingIntervalMs int,
	keepaliveTimeSec int, keepaliveTimeoutSec int) {
	port = 50051
	pingIntervalMs = 500
	keepaliveTimeSec = 10
	keepaliveTimeoutSec = 5

	// Collect named flags and positionals separately.
	var flags map[string]string
	positionals := []string{}
	i := 0
	for i < len(rawArgs) {
		arg := rawArgs[i]
		if strings.HasPrefix(arg, "-") && len(arg) > 1 {
			if flags == nil {
				flags = make(map[string]string)
			}
			key := strings.TrimLeft(arg, "-") // strip one or two leading dashes
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
	if v, ok := flags["port"]; ok && v != "" {
		fmt.Sscanf(v, "%d", &port)
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

	// Apply positional override (port).
	if len(positionals) > 0 {
		fmt.Sscanf(positionals[0], "%d", &port)
	}

	return
}

// ── Main ─────────────────────────────────────────────────────────────

func main() {
	port, pingIntervalMs, keepaliveTimeSec, keepaliveTimeoutSec :=
		parseServerArgs(os.Args[1:])

	slog := newServerLog()

	pingInterval := time.Duration(pingIntervalMs) * time.Millisecond
	keepaliveTime := time.Duration(keepaliveTimeSec) * time.Second
	keepaliveTimeout := time.Duration(keepaliveTimeoutSec) * time.Second

	servicer := &resilienceServer{
		log:          slog,
		pingInterval: pingInterval,
	}

	s := grpc.NewServer(
		grpc.Creds(insecure.NewCredentials()),
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time:    keepaliveTime,
			Timeout: keepaliveTimeout,
		}),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             1 * time.Second, // Must be ≤ keepalive time for tests
			PermitWithoutStream: true,
		}),
	)
	pb.RegisterResilienceServiceServer(s, servicer)

	lis, err := net.Listen("tcp", fmt.Sprintf("[::]:%d", port))
	if err != nil {
		slog.Error("Failed to listen on port %d: %s", port, err.Error())
		os.Exit(1)
	}

	slog.Info("Resilience gRPC server listening on port %d (app ping %d ms, keepalive %ds)",
		port, pingIntervalMs, keepaliveTimeSec)

	go func() {
		if err := s.Serve(lis); err != nil {
			slog.Error("Server error: %s", err.Error())
		}
	}()

	// Graceful shutdown on SIGINT/SIGTERM.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	<-ctx.Done()

	slog.Info("Server shutting down.")
	s.GracefulStop()
}
