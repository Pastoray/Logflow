package cluster

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"logflow/internal/channel"
)

// replicatedTestCluster is like testCluster in raft_test.go, but builds a
// full ReplicatedChannel per node (local memory channel + Raft node) instead
// of a bare Node, so these tests exercise Put()'s batching path end to end
// rather than just raft.Node.Apply().
type replicatedTestCluster struct {
	t       *testing.T
	peers   []Peer
	dataDir string
	rcs     map[string]*ReplicatedChannel
	mu      sync.Mutex
}

func newReplicatedTestCluster(t *testing.T, n int) *replicatedTestCluster {
	rtc := &replicatedTestCluster{
		t: t, peers: makePeers(t, n), dataDir: t.TempDir(),
		rcs: make(map[string]*ReplicatedChannel),
	}
	for _, p := range rtc.peers {
		rtc.start(p.ID)
	}
	return rtc
}

func (rtc *replicatedTestCluster) start(id string) *ReplicatedChannel {
	rtc.t.Helper()
	local := channel.NewMemoryChannel("test-"+id, 10000, 1)
	rc, err := NewReplicatedChannel("test", local, Config{
		NodeID: id, Peers: rtc.peers, DataDir: rtc.dataDir + "/" + id,
		ElectionTimeoutMin: 60 * time.Millisecond, ElectionTimeoutMax: 120 * time.Millisecond,
		HeartbeatInterval: 20 * time.Millisecond,
	})
	if err != nil {
		rtc.t.Fatalf("starting replicated channel %s: %v", id, err)
	}
	rtc.mu.Lock()
	rtc.rcs[id] = rc
	rtc.mu.Unlock()
	return rc
}

func (rtc *replicatedTestCluster) closeAll() {
	rtc.mu.Lock()
	defer rtc.mu.Unlock()
	for _, rc := range rtc.rcs {
		rc.Close()
	}
}

func (rtc *replicatedTestCluster) live() []*ReplicatedChannel {
	rtc.mu.Lock()
	defer rtc.mu.Unlock()
	out := make([]*ReplicatedChannel, 0, len(rtc.rcs))
	for _, rc := range rtc.rcs {
		out = append(out, rc)
	}
	return out
}

func (rtc *replicatedTestCluster) awaitLeader(timeout time.Duration) *ReplicatedChannel {
	rtc.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, rc := range rtc.live() {
			if rc.node.IsLeader() {
				return rc
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	rtc.t.Fatalf("no leader elected within %s", timeout)
	return nil
}

func mkEvent(body string) *channel.Event {
	return &channel.Event{Body: []byte(body), Timestamp: time.Now()}
}

// TestBatchSizeTriggersFlush checks that once BatchSize concurrent Put()
// calls have joined a batch, it's flushed (and so all of them return)
// well before BatchLinger would otherwise have fired - i.e. the size
// threshold, not just the timer, drives a flush.
func TestBatchSizeTriggersFlush(t *testing.T) {
	rtc := newReplicatedTestCluster(t, 3)
	defer rtc.closeAll()
	leader := rtc.awaitLeader(2 * time.Second)
	leader.SetBatching(10, 10*time.Second) // linger absurdly long on purpose

	const n = 10
	start := time.Now()
	var wg sync.WaitGroup
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs[i] = leader.Put(context.Background(), mkEvent(fmt.Sprintf("e%d", i)))
		}(i)
	}
	wg.Wait()
	elapsed := time.Since(start)

	for i, err := range errs {
		if err != nil {
			t.Errorf("Put %d: %v", i, err)
		}
	}
	if elapsed > 2*time.Second {
		t.Errorf("batch of %d (== BatchSize) took %s to flush - expected it to flush immediately on size, not wait for the 10s linger", n, elapsed)
	}
}

// TestBatchLingerFlushesSubThresholdBatch checks that a batch under
// BatchSize still flushes once BatchLinger elapses, rather than waiting
// forever for more Put() calls that never come.
func TestBatchLingerFlushesSubThresholdBatch(t *testing.T) {
	rtc := newReplicatedTestCluster(t, 3)
	defer rtc.closeAll()
	leader := rtc.awaitLeader(2 * time.Second)
	leader.SetBatching(1000, 100*time.Millisecond) // far below BatchSize

	start := time.Now()
	if err := leader.Put(context.Background(), mkEvent("solo")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	elapsed := time.Since(start)
	if elapsed < 90*time.Millisecond {
		t.Errorf("single Put flushed after %s, want >= ~100ms (BatchLinger)", elapsed)
	}
	if elapsed > 2*time.Second {
		t.Errorf("single Put took %s to flush - batching linger timer may be broken", elapsed)
	}
}

// TestBatchCoalescesIntoOneEntry confirms many concurrent Put()s under one
// BatchSize threshold really do become exactly one committed Raft entry
// (one EventBatch), not N separate ones - the whole point of batching.
func TestBatchCoalescesIntoOneEntry(t *testing.T) {
	rtc := newReplicatedTestCluster(t, 3)
	defer rtc.closeAll()
	leader := rtc.awaitLeader(2 * time.Second)
	leader.SetBatching(50, 5*time.Second)

	const n = 50
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if err := leader.Put(context.Background(), mkEvent(fmt.Sprintf("c%d", i))); err != nil {
				t.Errorf("Put %d: %v", i, err)
			}
		}(i)
	}
	wg.Wait()

	leader.node.mu.Lock()
	ci := leader.node.commitIndex
	leader.node.mu.Unlock()
	// Index 1 is the new leader's no-op (Raft §8); the batch is entry 2.
	if ci != 2 {
		t.Errorf("commitIndex = %d, want 2 (leader no-op + one entry; all %d Put()s should coalesce into a single Raft entry)", ci, n)
	}
}

// TestEveryPutGetsResult ensures each individual Put() caller in a batch
// gets its own correct result, not just "the batch as a whole succeeded" -
// e.g. that results aren't mixed up or dropped when many callers share one
// flush.
func TestEveryPutGetsResult(t *testing.T) {
	rtc := newReplicatedTestCluster(t, 3)
	defer rtc.closeAll()
	leader := rtc.awaitLeader(2 * time.Second)
	leader.SetBatching(20, 50*time.Millisecond)

	const n = 20
	var wg sync.WaitGroup
	results := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			results[i] = leader.Put(ctx, mkEvent(fmt.Sprintf("r%d", i)))
		}(i)
	}
	wg.Wait()

	for i, err := range results {
		if err != nil {
			t.Errorf("Put %d: got error %v, want nil", i, err)
		}
	}

	// And every event should actually be retrievable from the leader's
	// local channel - confirms the batch was unpacked back into individual
	// events on apply, not left as one opaque blob.
	got := map[string]bool{}
	for i := 0; i < n; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		e, err := leader.Take(ctx)
		cancel()
		if err != nil {
			t.Fatalf("Take %d: %v", i, err)
		}
		got[string(e.Body)] = true
	}
	for i := 0; i < n; i++ {
		want := fmt.Sprintf("r%d", i)
		if !got[want] {
			t.Errorf("missing event %q among taken events", want)
		}
	}
}

// TestBatchedThroughput is TestApplyThroughput's counterpart for the
// batched path: same 3-node localhost cluster, but driving Put() through a
// ReplicatedChannel with batching enabled, from many concurrent callers (the
// realistic shape of load - many gRPC source goroutines calling Put()
// concurrently, not one goroutine calling Apply() in a loop). This is the
// number that should land far above TestApplyThroughput's ~113-120/sec, and
// is what the main README's "Verified behavior" entry should quote.
func TestBatchedThroughput(t *testing.T) {
	rtc := newReplicatedTestCluster(t, 3)
	defer rtc.closeAll()
	leader := rtc.awaitLeader(2 * time.Second)
	leader.SetBatching(200, 50*time.Millisecond) // the package's own defaults

	const n = 5000
	const concurrency = 50
	var wg sync.WaitGroup
	start := time.Now()
	for w := 0; w < concurrency; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < n/concurrency; i++ {
				var err error
				for try := 0; try < 20; try++ {
					if err = leader.Put(context.Background(), mkEvent(fmt.Sprintf("w%d-%d", w, i))); err == nil || !strings.Contains(err.Error(), "not leader") {
						break
					}
					time.Sleep(25 * time.Millisecond)
				}
				if err != nil {
					t.Errorf("Put: %v", err)
					return
				}
			}
		}(w)
	}
	wg.Wait()
	elapsed := time.Since(start)
	t.Logf("%d Put()s (%d concurrent callers, BatchSize=200/Linger=50ms, 3-node cluster) in %s = %.0f/sec",
		n, concurrency, elapsed, float64(n)/elapsed.Seconds())
}

// TestCloseFlushesPendingBatch confirms Close() doesn't hang or abandon a
// Put() that's still sitting in an unflushed batch - the closeWG.Wait()
// path in Close() exists specifically for this.
func TestCloseFlushesPendingBatch(t *testing.T) {
	rtc := newReplicatedTestCluster(t, 3)
	leader := rtc.awaitLeader(2 * time.Second)
	leader.SetBatching(1000, 10*time.Second) // never fires on its own

	done := make(chan error, 1)
	go func() {
		done <- leader.Put(context.Background(), mkEvent("pending-at-close"))
	}()

	// Give the Put a moment to enqueue before we close everything.
	time.Sleep(50 * time.Millisecond)
	rtc.closeAll()

	select {
	case err := <-done:
		if err != nil {
			t.Logf("Put returned error on close (acceptable): %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Put() never returned after Close() - batchLoop's final flush must be hanging")
	}
}
