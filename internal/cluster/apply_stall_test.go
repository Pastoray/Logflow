package cluster

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"logflow/internal/channel"
)

// gatedChannel is a minimal channel.Channel whose Put blocks until
// release() is called (simulating a permanently-full local channel), then
// stores events for Take.
type gatedChannel struct {
	gate   chan struct{}
	mu     sync.Mutex
	events []*channel.Event
}

func newGatedChannel() *gatedChannel { return &gatedChannel{gate: make(chan struct{})} }

func (g *gatedChannel) release()     { close(g.gate) }
func (g *gatedChannel) Name() string { return "gated" }
func (g *gatedChannel) Put(ctx context.Context, e *channel.Event) error {
	select {
	case <-g.gate:
	case <-ctx.Done():
		return ctx.Err()
	}
	g.mu.Lock()
	g.events = append(g.events, e)
	g.mu.Unlock()
	return nil
}
func (g *gatedChannel) Take(ctx context.Context) (*channel.Event, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}
func (g *gatedChannel) Stats() channel.Stats { return channel.Stats{} }
func (g *gatedChannel) Close() error         { return nil }
func (g *gatedChannel) count() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return len(g.events)
}

// TestApplyStuckDetectionAndRecovery: a local channel whose Put blocks
// must surface as ApplyStuck()/ApplyStuckSeconds()/ApplyStallWarnings(),
// and once unblocked the entry must still be applied (never dropped) and
// the stuck state must clear.
func TestApplyStuckDetectionAndRecovery(t *testing.T) {
	g := newGatedChannel()
	peer := Peer{ID: "solo", Addr: fmt.Sprintf("127.0.0.1:%d", freePort(t)), ClientAddr: fmt.Sprintf("127.0.0.1:%d", freePort(t))}
	rc, err := NewReplicatedChannel("stall-test", g, Config{
		NodeID: "solo", Peers: []Peer{peer}, DataDir: t.TempDir(),
		ElectionTimeoutMin: 30 * time.Millisecond, ElectionTimeoutMax: 60 * time.Millisecond,
		HeartbeatInterval: 10 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	rc.SetApplyStallWarnInterval(100 * time.Millisecond)
	defer rc.Close()

	deadline := time.Now().Add(2 * time.Second)
	for !rc.node.IsLeader() {
		if time.Now().After(deadline) {
			t.Fatal("never became leader")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if rc.ApplyStuck() {
		t.Fatal("ApplyStuck() true before any write")
	}

	// Put returns once the entry is committed+applied; the apply blocks on
	// the gate, so run it in the background.
	putDone := make(chan error, 1)
	go func() { putDone <- rc.Put(context.Background(), mkEvent("x")) }()

	deadline = time.Now().Add(3 * time.Second)
	for !rc.ApplyStuck() {
		if time.Now().After(deadline) {
			t.Fatal("ApplyStuck() never became true while local Put was blocked")
		}
		time.Sleep(10 * time.Millisecond)
	}
	time.Sleep(250 * time.Millisecond) // let a couple of warn intervals elapse
	if rc.ApplyStallWarnings() < 2 {
		t.Errorf("ApplyStallWarnings() = %d, want >= 2 after ~350ms stuck at 100ms interval", rc.ApplyStallWarnings())
	}
	if rc.ApplyStuckSeconds() <= 0 {
		t.Errorf("ApplyStuckSeconds() = %v, want > 0", rc.ApplyStuckSeconds())
	}
	select {
	case err := <-putDone:
		t.Fatalf("Put returned (%v) while the local apply was still blocked - success reported before the entry was applied", err)
	default:
	}

	g.release()
	select {
	case err := <-putDone:
		if err != nil {
			t.Fatalf("Put after release: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Put never returned after releasing the local channel")
	}
	if g.count() != 1 {
		t.Errorf("local channel holds %d events, want exactly 1 (entry must be applied, not dropped)", g.count())
	}
	if rc.ApplyStuck() {
		t.Error("ApplyStuck() still true after recovery")
	}
}

// TestApplyOrderingWaitsForApplyFn: Apply must not report success until
// ApplyFn has actually returned on the leader (lastApplied used to be
// bumped before ApplyFn ran).
func TestApplyOrderingWaitsForApplyFn(t *testing.T) {
	peers := makePeers(t, 3)
	dataDir := t.TempDir()
	armed := make(map[string]chan struct{})
	release := make(map[string]chan struct{})
	nodes := make(map[string]*Node)
	for _, p := range peers {
		id := p.ID
		armed[id] = make(chan struct{}, 1)
		release[id] = make(chan struct{})
		n, err := NewNode(Config{
			NodeID: id, Peers: peers, DataDir: dataDir + "/" + id,
			ElectionTimeoutMin: 60 * time.Millisecond, ElectionTimeoutMax: 120 * time.Millisecond,
			HeartbeatInterval: 20 * time.Millisecond,
			ApplyFn: func(data []byte) {
				select {
				case <-armed[id]:
					<-release[id]
				default:
				}
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		nodes[id] = n
	}
	defer func() {
		for _, n := range nodes {
			n.Close()
		}
	}()

	var leader *Node
	deadline := time.Now().Add(3 * time.Second)
	for leader == nil && time.Now().Before(deadline) {
		for _, n := range nodes {
			if n.IsLeader() {
				leader = n
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	if leader == nil {
		t.Fatal("no leader")
	}
	id := leader.cfg.NodeID
	armed[id] <- struct{}{}

	done := make(chan error, 1)
	go func() { done <- leader.Apply([]byte("e1"), 5*time.Second) }()
	select {
	case err := <-done:
		t.Fatalf("Apply returned (%v) before the leader's ApplyFn finished", err)
	case <-time.After(400 * time.Millisecond):
	}
	close(release[id])
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Apply: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Apply never returned after ApplyFn was released")
	}
}
