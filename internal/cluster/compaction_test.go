package cluster

import (
	"fmt"
	"testing"
	"time"
)

func newCompactingCluster(t *testing.T, n, threshold, trailing int) *testCluster {
	tc := &testCluster{
		t: t, nodes: make(map[string]*Node), applied: make(map[string][][]byte),
		peers: makePeers(t, n), dataDir: t.TempDir(),
		snapThreshold: threshold, snapTrailing: trailing,
	}
	for _, p := range tc.peers {
		tc.start(p.ID)
	}
	return tc
}

func (tc *testCluster) appliedCount(id string) int {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	return len(tc.applied[id])
}

func applyN(t *testing.T, tc *testCluster, from, to int) {
	t.Helper()
	for i := from; i < to; i++ {
		var err error
		for try := 0; try < 50; try++ {
			l := tc.awaitLeader(5 * time.Second)
			if err = l.Apply([]byte(fmt.Sprintf("e%d", i)), 3*time.Second); err == nil {
				break
			}
			time.Sleep(20 * time.Millisecond)
		}
		if err != nil {
			t.Fatalf("apply %d: %v", i, err)
		}
	}
}

func waitFor(t *testing.T, d time.Duration, what string, cond func() bool) {
	t.Helper()
	end := time.Now().Add(d)
	for time.Now().Before(end) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func TestCompactionBoundsTheLog(t *testing.T) {
	tc := newCompactingCluster(t, 3, 20, 5)
	defer tc.closeAll()
	applyN(t, tc, 0, 300)
	for _, n := range tc.live() {
		n := n
		waitFor(t, 5*time.Second, "log compaction on "+n.cfg.NodeID, func() bool {
			return n.SnapshotIndex() > 0 && n.LogEntries() < 40
		})
	}
	for id := range tc.nodes {
		id := id
		waitFor(t, 5*time.Second, "all entries applied on "+id, func() bool { return tc.appliedCount(id) == 300 })
	}
	// Order preserved on every node.
	tc.mu.Lock()
	defer tc.mu.Unlock()
	for id, got := range tc.applied {
		for i, d := range got {
			if string(d) != fmt.Sprintf("e%d", i) {
				t.Fatalf("%s applied[%d]=%q, want e%d", id, i, d, i)
			}
		}
	}
}

// A restart after compaction must resume from the snapshot boundary (not
// replay the whole history) and rejoin normally.
func TestRestartAfterCompaction(t *testing.T) {
	tc := newCompactingCluster(t, 3, 20, 5)
	defer tc.closeAll()
	applyN(t, tc, 0, 200)
	leader := tc.awaitLeader(3 * time.Second)
	victim := ""
	for id := range tc.nodes {
		if id != leader.cfg.NodeID {
			victim = id
			break
		}
	}
	waitFor(t, 5*time.Second, "victim applied everything", func() bool { return tc.appliedCount(victim) == 200 })
	waitFor(t, 5*time.Second, "victim compacted", func() bool { return tc.nodes[victim].SnapshotIndex() > 0 })
	before := tc.appliedCount(victim)
	snapBefore := tc.nodes[victim].SnapshotIndex()
	tc.stop(victim)
	tc.start(victim)

	n := tc.nodes[victim]
	if n.SnapshotIndex() < snapBefore {
		t.Fatalf("snapshot boundary regressed across restart: %d -> %d", snapBefore, n.SnapshotIndex())
	}
	applyN(t, tc, 200, 220)
	waitFor(t, 5*time.Second, "restarted node to apply the new entries", func() bool {
		return tc.appliedCount(victim) >= before+20
	})
	if replayed := tc.appliedCount(victim) - before - 20; replayed > 30 {
		t.Errorf("restart re-applied %d old entries; compaction should bound the replay to the retained tail", replayed)
	}
}

// A follower that was down while the leader compacted past its position
// must be brought up to date with InstallSnapshot.
func TestLaggingFollowerCatchesUpViaSnapshot(t *testing.T) {
	tc := newCompactingCluster(t, 3, 20, 5)
	defer tc.closeAll()
	applyN(t, tc, 0, 10)
	leader := tc.awaitLeader(3 * time.Second)
	victim := ""
	for id := range tc.nodes {
		if id != leader.cfg.NodeID {
			victim = id
			break
		}
	}
	waitFor(t, 5*time.Second, "victim applied the first 10", func() bool { return tc.appliedCount(victim) == 10 })
	tc.stop(victim)
	applyN(t, tc, 10, 300) // far past the victim's position; leader compacts
	waitFor(t, 5*time.Second, "leader compaction past the victim", func() bool { return tc.awaitLeader(3*time.Second).SnapshotIndex() > 100 })

	tc.start(victim)
	n := tc.nodes[victim]
	waitFor(t, 8*time.Second, "victim to receive a snapshot", func() bool { return n.SnapshotIndex() > 10 })

	applyN(t, tc, 300, 310)
	waitFor(t, 8*time.Second, "victim to apply post-snapshot entries", func() bool {
		tc.mu.Lock()
		defer tc.mu.Unlock()
		got := tc.applied[victim]
		return len(got) > 0 && string(got[len(got)-1]) == "e309"
	})
	// Its local state machine deliberately does NOT contain the skipped
	// range (documented trade-off) - but it must not be re-applying all 300.
	if c := tc.appliedCount(victim); c >= 300 {
		t.Errorf("victim applied %d entries; the compacted range should have been skipped", c)
	}
	// And it must be a full participant: stop the other two's leader and
	// the cluster still commits with the victim in the quorum.
	l := tc.awaitLeader(3 * time.Second)
	if l.cfg.NodeID != victim {
		tc.stop(l.cfg.NodeID)
	} else {
		for id := range tc.nodes {
			if id != victim {
				tc.stop(id)
				break
			}
		}
	}
	applyN(t, tc, 310, 315)
}

// A brand-new member added long after compaction started must catch up via
// snapshot + tail and then take part normally.
func TestNewMemberJoinsViaSnapshot(t *testing.T) {
	tc := newCompactingCluster(t, 3, 20, 5)
	defer tc.closeAll()
	applyN(t, tc, 0, 200)
	leader := tc.awaitLeader(3 * time.Second)
	waitFor(t, 5*time.Second, "leader compaction", func() bool { return leader.SnapshotIndex() > 50 })

	newPeer := Peer{ID: "node-4", Addr: fmt.Sprintf("127.0.0.1:%d", freePort(t)), ClientAddr: fmt.Sprintf("127.0.0.1:%d", freePort(t))}
	joiner, err := NewNode(Config{
		Joining: true, Self: newPeer, DataDir: tc.dataDir + "/node-4",
		ElectionTimeoutMin: 60 * time.Millisecond, ElectionTimeoutMax: 120 * time.Millisecond,
		HeartbeatInterval: 20 * time.Millisecond, SnapshotThreshold: 20, SnapshotTrailing: 5,
		ApplyFn: func(d []byte) {
			tc.mu.Lock()
			tc.applied["node-4"] = append(tc.applied["node-4"], append([]byte(nil), d...))
			tc.mu.Unlock()
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	tc.mu.Lock()
	tc.nodes["node-4"] = joiner
	tc.mu.Unlock()

	if err := leader.AddServer(newPeer, 10*time.Second); err != nil {
		t.Fatalf("AddServer after compaction: %v", err)
	}
	for _, n := range tc.live() {
		awaitMembers(t, n, []string{"node-1", "node-2", "node-3", "node-4"}, 5*time.Second)
	}
	if joiner.SnapshotIndex() == 0 {
		t.Error("joiner never installed a snapshot")
	}
	applyN(t, tc, 200, 210)
	waitFor(t, 5*time.Second, "joiner applies new entries", func() bool {
		tc.mu.Lock()
		defer tc.mu.Unlock()
		g := tc.applied["node-4"]
		return len(g) > 0 && string(g[len(g)-1]) == "e209"
	})
	if c := tc.appliedCount("node-4"); c >= 200 {
		t.Errorf("joiner applied %d entries; should have skipped the compacted history", c)
	}
}

// The membership in effect must survive its config entries being compacted
// away - including across a restart, where the bootstrap Peers list is
// stale (3 nodes) but the cluster has grown to 4.
func TestMembershipSurvivesCompactionAndRestart(t *testing.T) {
	tc := newCompactingCluster(t, 3, 20, 5)
	defer tc.closeAll()
	applyN(t, tc, 0, 5)
	leader := tc.awaitLeader(3 * time.Second)
	newPeer := Peer{ID: "node-4", Addr: fmt.Sprintf("127.0.0.1:%d", freePort(t)), ClientAddr: fmt.Sprintf("127.0.0.1:%d", freePort(t))}
	joiner, err := NewNode(Config{
		Joining: true, Self: newPeer, DataDir: tc.dataDir + "/node-4",
		ElectionTimeoutMin: 60 * time.Millisecond, ElectionTimeoutMax: 120 * time.Millisecond,
		HeartbeatInterval: 20 * time.Millisecond, SnapshotThreshold: 20, SnapshotTrailing: 5,
		ApplyFn: func(d []byte) {},
	})
	if err != nil {
		t.Fatal(err)
	}
	tc.mu.Lock()
	tc.nodes["node-4"] = joiner
	tc.mu.Unlock()
	if err := leader.AddServer(newPeer, 10*time.Second); err != nil {
		t.Fatal(err)
	}
	applyN(t, tc, 5, 150) // push the config entries out of the retained log
	victim := ""
	for _, id := range []string{"node-1", "node-2", "node-3"} {
		if id != tc.awaitLeader(3*time.Second).cfg.NodeID {
			victim = id
			break
		}
	}
	waitFor(t, 5*time.Second, "victim compacted past the config entries", func() bool {
		n := tc.nodes[victim]
		return n.SnapshotIndex() > 10 && n.LogEntries() < 30
	})
	tc.stop(victim)
	tc.start(victim) // bootstraps from the stale 3-peer list
	awaitMembers(t, tc.nodes[victim], []string{"node-1", "node-2", "node-3", "node-4"}, 5*time.Second)
	applyN(t, tc, 150, 155)
}

// A snapshot older than what a node already has (a leader that compacted
// less than this follower did, or a delayed message) must be a no-op and
// must not roll the follower's boundary back or discard its log. Found by
// a live run: a follower whose own snapshot index was 228 accepted a
// leader's index-190 snapshot and wiped 150 retained entries.
func TestStaleSnapshotDoesNotRegressFollower(t *testing.T) {
	tc := newCompactingCluster(t, 3, 20, 5)
	defer tc.closeAll()
	applyN(t, tc, 0, 200)
	leader := tc.awaitLeader(3 * time.Second)
	var f *Node
	for _, n := range tc.live() {
		if n != leader {
			f = n
			break
		}
	}
	waitFor(t, 5*time.Second, "follower compacted", func() bool { return f.SnapshotIndex() > 50 })
	snap := f.SnapshotIndex()
	f.mu.Lock()
	term := f.currentTerm
	lastBefore := f.lastIndexLocked()
	f.mu.Unlock()

	var reply InstallSnapshotReply
	err := (*raftRPC)(f).InstallSnapshot(&InstallSnapshotArgs{
		Term: term, LeaderID: leader.cfg.NodeID, LastIncludedIndex: snap - 30, LastIncludedTerm: term,
		Membership: Membership{Phase: PhaseStable, New: tc.peers},
	}, &reply)
	if err != nil || !reply.Success {
		t.Fatalf("stale snapshot: err=%v reply=%+v", err, reply)
	}
	if f.SnapshotIndex() < snap {
		t.Fatalf("snapshot index regressed %d -> %d", snap, f.SnapshotIndex())
	}
	// Background compaction may legitimately advance snapIndex and shrink
	// the retained slice, but the log's last index must never go down: a
	// wrongly-installed stale snapshot would reset it to the boundary.
	f.mu.Lock()
	lastAfter := f.lastIndexLocked()
	f.mu.Unlock()
	if lastAfter < lastBefore {
		t.Fatalf("last log index fell %d -> %d after a stale snapshot", lastBefore, lastAfter)
	}
}

// ListenAddr must change what the node binds without changing what it
// advertises (container/pod DNS names are dialed by peers but need not be
// bindable locally).
func TestListenAddrOverridesBind(t *testing.T) {
	adv := fmt.Sprintf("127.0.0.1:%d", freePort(t))
	bind := fmt.Sprintf("127.0.0.1:%d", freePort(t))
	cbind := fmt.Sprintf("127.0.0.1:%d", freePort(t))
	peer := Peer{ID: "solo", Addr: adv, ClientAddr: "127.0.0.1:1"} // advertised client addr deliberately unbindable-in-test
	n, err := NewNode(Config{
		NodeID: "solo", Peers: []Peer{peer}, DataDir: t.TempDir(),
		ListenAddr: bind, ClientListenAddr: cbind,
		ElectionTimeoutMin: 30 * time.Millisecond, ElectionTimeoutMax: 60 * time.Millisecond,
		HeartbeatInterval: 10 * time.Millisecond, ApplyFn: func([]byte) {},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()
	if got := n.listener.Addr().String(); got != bind {
		t.Fatalf("raft listener bound %s, want %s", got, bind)
	}
	if got := n.clientListener.Addr().String(); got != cbind {
		t.Fatalf("client listener bound %s, want %s", got, cbind)
	}
}
