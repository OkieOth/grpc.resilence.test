package tests

import (
	"strings"
	"sync"
	"testing"
	"time"

	pb "github.com/example/grpc-resilience/api"
)

// ── Multi-client helpers ─────────────────────────────────────────────

type clientResult struct {
	ClientID  string
	PingsSent int
	Pongs     []*pb.Pong
}

// runMultiClient runs 3 concurrent clients and returns their results.
func runMultiClient(t *testing.T) []clientResult {
	clients := []string{"client_alpha", "client_beta", "client_gamma"}
	var mu sync.Mutex
	results := make([]clientResult, len(clients))

	var wg sync.WaitGroup
	for i, cid := range clients {
		wg.Add(1)
		go func(idx int, clientID string) {
			defer wg.Done()
			pings, pongs := runClient(t, serverAddress(), clientID, 150, 5*time.Second)
			mu.Lock()
			results[idx] = clientResult{
				ClientID:  clientID,
				PingsSent: pings,
				Pongs:     pongs,
			}
			mu.Unlock()
		}(i, cid)
	}
	wg.Wait()
	return results
}

// ── Shared scenario (sync.Once) ──────────────────────────────────────

var multiOnce sync.Once
var multiResults []clientResult

// runMultiClients runs the multi-client scenario once.
func runMultiClients() {
	multiResults = runMultiClient(&testing.T{})
}

// ── Tests ────────────────────────────────────────────────────────────

func TestMultiClientsAllReceivePongs(t *testing.T) {
	multiOnce.Do(runMultiClients)

	for _, c := range multiResults {
		if len(c.Pongs) == 0 {
			t.Fatalf("%s received 0 server pongs (sent %d pings)", c.ClientID, c.PingsSent)
		}
		for _, pong := range c.Pongs {
			if pong.Sender != "server" {
				t.Errorf("%s: expected pong sender 'server', got '%s'", c.ClientID, pong.Sender)
			}
		}
	}
}

func TestMultiClientsDistinguishStreams(t *testing.T) {
	multiOnce.Do(runMultiClients)

	// Wait a few seconds for pings to accumulate.
	time.Sleep(3 * time.Second)

	log, err := readServerLog(globalFixture.LogPath)
	if err != nil {
		t.Fatalf("Failed to read server log: %v", err)
	}

	for _, c := range multiResults {
		pingCount := 0
		for _, line := range strings.Split(log, "\n") {
			if strings.Contains(line, c.ClientID) && strings.Contains(line, "Server received ping") {
				pingCount++
			}
		}
		if pingCount == 0 {
			t.Fatalf("Server did not log any pings from %s.\nServer log:\n%s", c.ClientID, log)
		}
	}
}

func TestMultiClientsTotalPongsExceedSingle(t *testing.T) {
	multiOnce.Do(runMultiClients)

	totalPongs := 0
	for _, c := range multiResults {
		totalPongs += len(c.Pongs)
	}
	if totalPongs == 0 {
		t.Fatal("No pongs received from any client")
	}

	perClientPongs := make([]int, len(multiResults))
	for i, c := range multiResults {
		perClientPongs[i] = len(c.Pongs)
	}
	for _, p := range perClientPongs {
		if p == 0 {
			t.Fatalf("Not all clients received pongs: %v", perClientPongs)
		}
	}
}

func TestMultiClientsSeesMonotonicPongs(t *testing.T) {
	multiOnce.Do(runMultiClients)

	for _, c := range multiResults {
		if len(c.Pongs) > 1 {
			ids := make([]int32, len(c.Pongs))
			for i, p := range c.Pongs {
				ids[i] = p.Id
			}
			for i := 1; i < len(ids); i++ {
				if ids[i] < ids[i-1] {
					t.Fatalf("%s saw non-monotonic pong IDs: %v", c.ClientID, ids)
				}
			}
		}
	}
}
