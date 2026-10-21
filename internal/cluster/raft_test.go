package cluster

import (
	"fmt"
	"net"
	"sync"
	"testing"
	"time"
)

// Election timeouts for the multi-node test clusters. They used to be
// 60-120ms, which is fine on an idle laptop but on a loaded CI runner
// (-race, 2 shared cores) a delayed heartbeat is enough to trigger a
// spurious election and make an in-flight Apply/Put fail with "not leader".
// 10x the heartbeat interval keeps tests deterministic; deliberately tiny
// timeouts (single-node tests, TestJoiningNodeNeverSelfElects) are left alone.
const (
	testElectionMin = 250 * time.Millisecond
	testElectionMax = 500 * time.Millisecond
)

// freePort asks the OS for an unused TCP port by briefly binding to :0,
// then releasing it - the standard trick for handing a deterministic,
// collision-free address to a server that hasn't started listening yet.
func freePort(t *testing.T) int {
	t.Helper()
	// The OS may hand the same just-released port back on the next call, and
	// makePeers/other tests ask for many ports before any of them is bound;
	// remember every port handed out so two nodes never get the same one.
	for {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		port := l.Addr().(*net.TCPAddr).Port
		l.Close()
		usedPortsMu.Lock()
		dup := usedPorts[port]
		usedPorts[port] = true
		usedPortsMu.Unlock()
		if !dup {
			return port
		}
	}
}

var (
	usedPortsMu sync.Mutex
	usedPorts   = map[int]bool{}
)

// applyRetry proposes data on whoever is the current leader, retrying
// through leadership changes (a loaded runner can trigger a spurious
// election at any moment - that's normal Raft, and a client is expected to
// retry). Because "not leader" can be returned for a write that the next
// leader still commits, callers must tolerate (consecutive) duplicates.
func (tc *testCluster) applyRetry(data []byte) error {
	var err error
	for end := time.Now().Add(20 * time.Second); time.Now().Before(end); time.Sleep(30 * time.Millisecond) {
		l := tc.awaitLeader(10 * time.Second)
		if err = l.Apply(data, 3*time.Second); err == nil {
			return nil
		}
		if err != ErrNotLeader && err != ErrApplyTimeout {
			return err
		}
	}
	return err
}

// dedupeConsecutive collapses immediate repeats (a retried write that the
// first leader had in fact committed - see applyRetry).
func dedupeConsecutive(in [][]byte) [][]byte {
	var out [][]byte
	for i, e := range in {
		if i > 0 && string(e) == string(in[i-1]) {
			continue
		}
		out = append(out, e)
	}
	return out
}

func makePeers(t *testing.T, n int) []Peer {
	t.Helper()
	peers := make([]Peer, n)
	for i := 0; i < n; i++ {
		peers[i] = Peer{
			ID:         fmt.Sprintf("node-%d", i+1),
			Addr:       fmt.Sprintf("127.0.0.1:%d", freePort(t)),
			ClientAddr: fmt.Sprintf("127.0.0.1:%d", freePort(t)),
		}
	}
	return peers
}

type testCluster struct {
	t       *testing.T
	nodes   map[string]*Node
	applied map[string][][]byte
	mu      sync.Mutex
	peers   []Peer
	dataDir string

	snapThreshold, snapTrailing int // 0 = compaction off

	// partition fault injection: nodes in different groups can't talk.
	// nil/empty = fully connected. Guarded by mu.
	group map[string]int
}

func newTestCluster(t *testing.T, n int) *testCluster {
	tc := &testCluster{
		t: t, nodes: make(map[string]*Node), applied: make(map[string][][]byte),
		peers: makePeers(t, n), dataDir: t.TempDir(),
	}
	for _, p := range tc.peers {
		tc.start(p.ID)
	}
	return tc
}

func (tc *testCluster) start(id string) {
	tc.t.Helper()
	node, err := NewNode(Config{
		NodeID: id, Peers: tc.peers, DataDir: tc.dataDir + "/" + id,
		ElectionTimeoutMin: testElectionMin, ElectionTimeoutMax: testElectionMax,
		HeartbeatInterval: 20 * time.Millisecond,
		SnapshotThreshold: tc.snapThreshold, SnapshotTrailing: tc.snapTrailing,
		LinkFilter: tc.linkOK(id),
		ApplyFn: func(data []byte) {
			tc.mu.Lock()
			tc.applied[id] = append(tc.applied[id], append([]byte(nil), data...))
			tc.mu.Unlock()
		},
	})
	if err != nil {
		tc.t.Fatalf("starting node %s: %v", id, err)
	}
	tc.mu.Lock()
	tc.nodes[id] = node
	tc.mu.Unlock()
}

func (tc *testCluster) stop(id string) {
	tc.mu.Lock()
	n := tc.nodes[id]
	delete(tc.nodes, id)
	tc.mu.Unlock()
	if n != nil {
		n.Close()
	}
}

func (tc *testCluster) closeAll() {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	for _, n := range tc.nodes {
		n.Close()
	}
}

func (tc *testCluster) live() []*Node {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	out := make([]*Node, 0, len(tc.nodes))
	for _, n := range tc.nodes {
		out = append(out, n)
	}
	return out
}

// awaitLeader polls until exactly one live node reports itself leader (and
// every other live node agrees that's who's leader), or t fails on timeout.
func (tc *testCluster) awaitLeader(timeout time.Duration) *Node {
	tc.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var leader *Node
		agree := true
		for _, n := range tc.live() {
			if n.IsLeader() {
				if leader != nil {
					agree = false // two leaders at once would be a real bug
				}
				leader = n
			}
		}
		if leader != nil && agree {
			// Confirm every other live node's view of who's leader
			// matches, not just that exactly one node thinks it's
			// leader itself.
			allAgree := true
			for _, n := range tc.live() {
				if n == leader {
					continue
				}
				if p, ok := n.LeaderPeer(); !ok || p.ID != leader.cfg.NodeID {
					allAgree = false
					break
				}
			}
			if allAgree {
				return leader
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	tc.t.Fatalf("no single agreed-upon leader within %s", timeout)
	return nil
}

func TestElectsExactlyOneLeader(t *testing.T) {
	tc := newTestCluster(t, 3)
	defer tc.closeAll()

	leader := tc.awaitLeader(2 * time.Second)
	t.Logf("elected leader: %s (term %d)", leader.cfg.NodeID, leader.Term())
}

func TestApplyReplicatesToAllNodes(t *testing.T) {
	tc := newTestCluster(t, 3)
	defer tc.closeAll()

	tc.awaitLeader(2 * time.Second)

	const n = 20
	for i := 0; i < n; i++ {
		data := []byte(fmt.Sprintf("entry-%d", i))
		if err := tc.applyRetry(data); err != nil {
			t.Fatalf("Apply(%d): %v", i, err)
		}
	}

	// Give followers a moment to catch up via their own heartbeat-borne
	// replication (Apply only guarantees the *leader* has applied by the
	// time it returns, not every follower).
	deadline := time.Now().Add(5 * time.Second)
	for {
		tc.mu.Lock()
		ok := true
		for id := range tc.nodes {
			if len(dedupeConsecutive(tc.applied[id])) != n {
				ok = false
			}
		}
		tc.mu.Unlock()
		if ok || time.Now().After(deadline) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	tc.mu.Lock()
	defer tc.mu.Unlock()
	for id, entries := range tc.applied {
		entries = dedupeConsecutive(entries)
		if len(entries) != n {
			t.Errorf("node %s: applied %d entries, want %d", id, len(entries), n)
			continue
		}
		for i, e := range entries {
			want := fmt.Sprintf("entry-%d", i)
			if string(e) != want {
				t.Errorf("node %s entry %d: got %q, want %q", id, i, e, want)
			}
		}
	}
}

func TestFollowerRejectsApply(t *testing.T) {
	tc := newTestCluster(t, 3)
	defer tc.closeAll()

	leader := tc.awaitLeader(2 * time.Second)
	for _, n := range tc.live() {
		if n == leader {
			continue
		}
		if err := n.Apply([]byte("x"), 200*time.Millisecond); err != ErrNotLeader {
			t.Errorf("follower %s Apply: got %v, want ErrNotLeader", n.cfg.NodeID, err)
		}
	}
}

func TestLeaderFailoverContinuesCommitting(t *testing.T) {
	tc := newTestCluster(t, 3)
	defer tc.closeAll()

	leader1 := tc.awaitLeader(2 * time.Second)
	if err := leader1.Apply([]byte("before-failover"), 2*time.Second); err != nil {
		t.Fatalf("Apply before failover: %v", err)
	}

	oldLeaderID := leader1.cfg.NodeID
	t.Logf("killing leader %s (term %d)", oldLeaderID, leader1.Term())
	tc.stop(oldLeaderID)

	// A new leader must emerge from the remaining 2 nodes.
	deadline := time.Now().Add(3 * time.Second)
	var leader2 *Node
	for time.Now().Before(deadline) {
		for _, n := range tc.live() {
			if n.IsLeader() {
				leader2 = n
			}
		}
		if leader2 != nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if leader2 == nil {
		t.Fatal("no new leader elected after killing the old one")
	}
	if leader2.cfg.NodeID == oldLeaderID {
		t.Fatalf("new leader is the node we killed - impossible, test bug")
	}
	t.Logf("new leader: %s (term %d)", leader2.cfg.NodeID, leader2.Term())
	if leader2.Term() <= leader1.Term() {
		t.Errorf("new leader's term %d should be > old leader's term %d", leader2.Term(), leader1.Term())
	}

	if err := leader2.Apply([]byte("after-failover"), 2*time.Second); err != nil {
		t.Fatalf("Apply after failover: %v", err)
	}

	// leader2.Apply() having returned only guarantees a majority
	// *committed* the entry (which, with one node dead, necessarily means
	// every surviving node did) - not that every follower's own applyLoop
	// goroutine has run yet (up to its ~50ms poll interval away), so poll
	// rather than asserting immediately.
	hasEntry := func(id string) bool {
		for _, e := range tc.applied[id] {
			if string(e) == "after-failover" {
				return true
			}
		}
		return false
	}
	convergeDeadline := time.Now().Add(2 * time.Second)
	for {
		tc.mu.Lock()
		allFound := true
		for id := range tc.nodes {
			if id != oldLeaderID && !hasEntry(id) {
				allFound = false
			}
		}
		tc.mu.Unlock()
		if allFound || time.Now().After(convergeDeadline) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	tc.mu.Lock()
	defer tc.mu.Unlock()
	for id := range tc.nodes {
		if id == oldLeaderID {
			continue // dead, won't have the post-failover entry
		}
		if !hasEntry(id) {
			t.Errorf("node %s never applied the post-failover entry (applied: %v)", id, tc.applied[id])
		}
	}
}

func TestRejoinedNodeCatchesUp(t *testing.T) {
	tc := newTestCluster(t, 3)
	defer tc.closeAll()

	leader := tc.awaitLeader(2 * time.Second)
	var followerID string
	for _, n := range tc.live() {
		if n != leader {
			followerID = n.cfg.NodeID
			break
		}
	}

	t.Logf("stopping follower %s", followerID)
	tc.stop(followerID)

	for i := 0; i < 5; i++ {
		if err := tc.applyRetry([]byte(fmt.Sprintf("while-down-%d", i))); err != nil {
			t.Fatalf("Apply while follower down: %v", err)
		}
	}

	t.Logf("restarting follower %s", followerID)
	tc.start(followerID)

	deadline := time.Now().Add(3 * time.Second)
	for {
		tc.mu.Lock()
		got := len(tc.applied[followerID])
		tc.mu.Unlock()
		if got >= 5 || time.Now().After(deadline) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	tc.mu.Lock()
	defer tc.mu.Unlock()
	if len(tc.applied[followerID]) < 5 {
		t.Errorf("rejoined follower only caught up on %d/5 entries: %v", len(tc.applied[followerID]), tc.applied[followerID])
	}
}

// TestApplyThroughput isn't a correctness check - it's here to put a real
// number on this package's documented "one log entry per Put(), not
// batched" trade-off, so that number lives next to the code it describes
// rather than only in a README that can drift. See the main README's
// "Clustered collector (Raft)" Verified behavior entry for the live,
// end-to-end measurement this corroborates.
func TestApplyThroughput(t *testing.T) {
	tc := newTestCluster(t, 3)
	defer tc.closeAll()
	leader := tc.awaitLeader(2 * time.Second)

	const n = 300
	start := time.Now()
	for i := 0; i < n; i++ {
		if err := leader.Apply([]byte("x"), 5*time.Second); err != nil {
			t.Fatalf("Apply %d: %v", i, err)
		}
	}
	elapsed := time.Since(start)
	t.Logf("%d sequential Apply() calls (3-node cluster, majority=2, localhost loopback) in %s = %.0f/sec",
		n, elapsed, float64(n)/elapsed.Seconds())
}

// linkOK returns the LinkFilter for node id: a call is allowed unless the
// two ends are currently in different partition groups.
func (tc *testCluster) linkOK(id string) func(from, toAddr string) bool {
	return func(_, toAddr string) bool {
		tc.mu.Lock()
		defer tc.mu.Unlock()
		if len(tc.group) == 0 {
			return true
		}
		for _, p := range tc.peers {
			if p.Addr == toAddr || p.ClientAddr == toAddr {
				return tc.group[id] == tc.group[p.ID]
			}
		}
		return true // addresses of nodes added later: not partitioned
	}
}

func (tc *testCluster) setPartition(g map[string]int) {
	tc.mu.Lock()
	tc.group = g
	tc.mu.Unlock()
}
