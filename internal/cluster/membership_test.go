package cluster

import (
	"fmt"
	"testing"
	"time"
)

// awaitMembers polls until the given node reports exactly wantIDs as its
// current voting members (order-independent), or t fails on timeout. Used
// throughout this file instead of asserting immediately after an
// AddServer/RemoveServer call returns, since a membership change
// committing on the leader doesn't mean every node's own applyLoop has
// run yet (same asynchronous-follower-catchup reasoning raft_test.go's
// TestLeaderFailoverContinuesCommitting documents).
func awaitMembers(t *testing.T, n *Node, wantIDs []string, timeout time.Duration) {
	t.Helper()
	want := map[string]bool{}
	for _, id := range wantIDs {
		want[id] = true
	}
	deadline := time.Now().Add(timeout)
	var last []string
	for time.Now().Before(deadline) {
		got := n.MemberIDs()
		last = got
		if sameSet(got, want) {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("node %s: members = %v, want %v (phase=%s)", n.cfg.NodeID, last, wantIDs, n.MembershipPhase())
}

func sameSet(got []string, want map[string]bool) bool {
	if len(got) != len(want) {
		return false
	}
	for _, id := range got {
		if !want[id] {
			return false
		}
	}
	return true
}

// TestAddServerJoinsLiveCluster starts a 3-node cluster, starts a 4th node
// standalone in Joining mode (no restart of the existing 3 - the whole
// point), adds it via AddServer, and confirms every node (including the
// new one) ends up agreeing on a 4-member cluster that the new node
// actually participates in (receives committed entries).
func TestAddServerJoinsLiveCluster(t *testing.T) {
	tc := newTestCluster(t, 3)
	defer tc.closeAll()
	leader := tc.awaitLeader(2 * time.Second)

	if err := tc.applyRetry([]byte("before-join")); err != nil {
		t.Fatalf("Apply before join: %v", err)
	}

	newPeer := Peer{
		ID: "node-4", Addr: fmt.Sprintf("127.0.0.1:%d", freePort(t)),
		ClientAddr: fmt.Sprintf("127.0.0.1:%d", freePort(t)),
	}
	joiner, err := NewNode(Config{
		Joining: true, Self: newPeer, DataDir: tc.dataDir + "/node-4",
		ElectionTimeoutMin: testElectionMin, ElectionTimeoutMax: testElectionMax,
		HeartbeatInterval: 20 * time.Millisecond,
		ApplyFn: func(data []byte) {
			tc.mu.Lock()
			tc.applied["node-4"] = append(tc.applied["node-4"], append([]byte(nil), data...))
			tc.mu.Unlock()
		},
	})
	if err != nil {
		t.Fatalf("starting joining node: %v", err)
	}
	tc.mu.Lock()
	tc.nodes["node-4"] = joiner
	tc.mu.Unlock()

	if err := leader.AddServer(newPeer, 5*time.Second); err != nil {
		t.Fatalf("AddServer: %v", err)
	}

	wantIDs := []string{"node-1", "node-2", "node-3", "node-4"}
	for _, n := range tc.live() {
		awaitMembers(t, n, wantIDs, 3*time.Second)
	}

	// The new node must be a real participant, not just "on the member
	// list": a fresh Apply() after it joined should replicate to it too.
	if err := tc.applyRetry([]byte("after-join")); err != nil {
		t.Fatalf("Apply after join: %v", err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		tc.mu.Lock()
		got := len(tc.applied["node-4"])
		tc.mu.Unlock()
		if got >= 2 || time.Now().After(deadline) { // before-join + after-join
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	tc.mu.Lock()
	defer tc.mu.Unlock()
	if len(tc.applied["node-4"]) < 2 {
		t.Errorf("new node only applied %d/2 entries after joining: %v", len(tc.applied["node-4"]), tc.applied["node-4"])
	}
}

// TestRemoveServerLeavesLiveCluster adds a 4th node (so removing one still
// leaves a healthy 3-node majority), then removes an original member and
// confirms the cluster converges on the smaller membership and keeps
// committing without the removed node.
func TestRemoveServerLeavesLiveCluster(t *testing.T) {
	tc := newTestCluster(t, 3)
	defer tc.closeAll()
	leader := tc.awaitLeader(2 * time.Second)

	newPeer := Peer{
		ID: "node-4", Addr: fmt.Sprintf("127.0.0.1:%d", freePort(t)),
		ClientAddr: fmt.Sprintf("127.0.0.1:%d", freePort(t)),
	}
	joiner, err := NewNode(Config{
		Joining: true, Self: newPeer, DataDir: tc.dataDir + "/node-4",
		ElectionTimeoutMin: testElectionMin, ElectionTimeoutMax: testElectionMax,
		HeartbeatInterval: 20 * time.Millisecond,
		ApplyFn: func(data []byte) {
			tc.mu.Lock()
			tc.applied["node-4"] = append(tc.applied["node-4"], append([]byte(nil), data...))
			tc.mu.Unlock()
		},
	})
	if err != nil {
		t.Fatalf("starting joining node: %v", err)
	}
	tc.mu.Lock()
	tc.nodes["node-4"] = joiner
	tc.mu.Unlock()
	if err := leader.AddServer(newPeer, 5*time.Second); err != nil {
		t.Fatalf("AddServer: %v", err)
	}
	awaitMembers(t, leader, []string{"node-1", "node-2", "node-3", "node-4"}, 3*time.Second)

	// Remove an original member that isn't the current leader (removing
	// the leader itself is covered by TestLeaderStepsDownAfterRemovingSelf).
	var victim string
	for _, id := range []string{"node-1", "node-2", "node-3"} {
		if id != leader.cfg.NodeID {
			victim = id
			break
		}
	}
	if err := leader.RemoveServer(victim, 5*time.Second); err != nil {
		t.Fatalf("RemoveServer(%s): %v", victim, err)
	}
	tc.stop(victim) // the removed node's own process would normally be torn down by an operator

	wantIDs := []string{"node-1", "node-2", "node-3", "node-4"}
	final := make([]string, 0, 3)
	for _, id := range wantIDs {
		if id != victim {
			final = append(final, id)
		}
	}
	for _, n := range tc.live() {
		awaitMembers(t, n, final, 3*time.Second)
	}

	// The 3 remaining nodes must still be able to commit - confirms the
	// cluster's majority math now uses the smaller membership, not a
	// stale one still counting the removed node.
	if err := tc.applyRetry([]byte("after-remove")); err != nil {
		t.Fatalf("Apply after remove: %v", err)
	}
}

// TestJoiningNodeNeverSelfElects starts a lone node in Joining mode (no
// peers it can reach at all) and confirms it never promotes itself to
// leader, however long its election timeout has been firing uncontested -
// the whole safety property Config.Joining exists for.
func TestJoiningNodeNeverSelfElects(t *testing.T) {
	dir := t.TempDir()
	self := Peer{
		ID: "lonely", Addr: fmt.Sprintf("127.0.0.1:%d", freePort(t)),
		ClientAddr: fmt.Sprintf("127.0.0.1:%d", freePort(t)),
	}
	n, err := NewNode(Config{
		Joining: true, Self: self, DataDir: dir,
		ElectionTimeoutMin: 20 * time.Millisecond, ElectionTimeoutMax: 40 * time.Millisecond,
		HeartbeatInterval: 10 * time.Millisecond,
		ApplyFn:           func(data []byte) {},
	})
	if err != nil {
		t.Fatalf("starting joining node: %v", err)
	}
	defer n.Close()

	// Let many election timeouts fire uncontested.
	time.Sleep(500 * time.Millisecond)

	if n.IsLeader() {
		t.Fatal("a joining node with no cluster must never elect itself leader")
	}
	if n.State() != "follower" {
		t.Errorf("joining node state = %s, want follower", n.State())
	}
}

// TestChangeMembershipRejectsConcurrentChange confirms the standard Raft
// restriction - no two membership changes in flight at once - is actually
// enforced, not just documented.
func TestChangeMembershipRejectsConcurrentChange(t *testing.T) {
	tc := newTestCluster(t, 3)
	defer tc.closeAll()
	leader := tc.awaitLeader(2 * time.Second)

	newPeer := Peer{
		ID: "node-4", Addr: fmt.Sprintf("127.0.0.1:%d", freePort(t)),
		ClientAddr: fmt.Sprintf("127.0.0.1:%d", freePort(t)),
	}

	// Mark a change in flight directly (bypassing the catch-up delay a
	// real AddServer would incur) to deterministically hit the "already
	// in progress" path without racing a real concurrent call.
	leader.mu.Lock()
	leader.membershipChangeInFlight = true
	leader.mu.Unlock()
	defer func() {
		leader.mu.Lock()
		leader.membershipChangeInFlight = false
		leader.mu.Unlock()
	}()

	err := leader.changeMembership(append(leader.MemberIDsAsPeers(), newPeer), time.Second)
	if err != ErrMembershipChangeInProgress {
		t.Errorf("changeMembership while one is in flight: got %v, want ErrMembershipChangeInProgress", err)
	}
}

// TestLeaderStepsDownAfterRemovingSelf confirms Raft §6.2: a leader that
// removes itself from the cluster steps down to follower once the new
// (self-excluding) configuration commits, rather than continuing to
// manage a group it's no longer a member of.
func TestLeaderStepsDownAfterRemovingSelf(t *testing.T) {
	tc := newTestCluster(t, 3)
	defer tc.closeAll()
	leader := tc.awaitLeader(2 * time.Second)
	leaderID := leader.cfg.NodeID

	if err := leader.RemoveServer(leaderID, 5*time.Second); err != nil {
		t.Fatalf("RemoveServer(self): %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && leader.IsLeader() {
		time.Sleep(10 * time.Millisecond)
	}
	if leader.IsLeader() {
		t.Error("leader did not step down after removing itself from the cluster")
	}

	// The remaining two nodes must still elect a leader and keep
	// committing, now as a 2-node majority-of-2 cluster.
	var others []*Node
	for _, n := range tc.live() {
		if n.cfg.NodeID != leaderID {
			others = append(others, n)
		}
	}
	deadline = time.Now().Add(3 * time.Second)
	var newLeader *Node
	for time.Now().Before(deadline) {
		for _, n := range others {
			if n.IsLeader() {
				newLeader = n
			}
		}
		if newLeader != nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if newLeader == nil {
		t.Fatal("no new leader elected among the 2 remaining nodes")
	}
	if err := newLeader.Apply([]byte("after-self-removal"), 2*time.Second); err != nil {
		t.Fatalf("Apply on 2-node cluster after self-removal: %v", err)
	}
}

// TestRemovedNodeDoesNotDisruptCluster is a regression test for a real
// bug caught in live (not unit) testing: a node removed via RemoveServer
// stops being replicated to the instant Cnew is proposed (it's no longer
// in votingPeersLocked), so if its process keeps running, its own
// election timer eventually fires with nothing to suppress it. Without
// (a) RequestVote rejecting a non-member candidate BEFORE adopting its
// term/stepping down, and (b) the leader's courtesy push of the final
// Cnew entry so the removed node learns to stop calling elections at all
// (see changeMembership and electionLoop's "departed" check), the removed
// node would keep sending ever-higher-term RequestVotes forever, forcing
// the real leader to repeatedly step down and re-elect - a live
// production cluster becoming unstable purely because a decommissioned
// process was never killed.
func TestRemovedNodeDoesNotDisruptCluster(t *testing.T) {
	tc := newTestCluster(t, 3)
	defer tc.closeAll()
	leader := tc.awaitLeader(2 * time.Second)
	leaderIDBefore := leader.cfg.NodeID

	var victim *Node
	for _, n := range tc.live() {
		if n != leader {
			victim = n
			break
		}
	}
	victimID := victim.cfg.NodeID

	if err := leader.RemoveServer(victimID, 5*time.Second); err != nil {
		t.Fatalf("RemoveServer: %v", err)
	}
	// Deliberately do NOT stop the victim's process - that's the whole
	// point: it's still running, just no longer a cluster member.

	// Give the victim's own election timeout (60-120ms per
	// newTestCluster) several chances to fire before checking - this is
	// exactly the window in which the bug manifested live.
	time.Sleep(500 * time.Millisecond)

	if leader.cfg.NodeID != leaderIDBefore {
		t.Errorf("leader identity changed from %s - something disrupted it after an unrelated node was removed", leaderIDBefore)
	}
	if !leader.IsLeader() {
		t.Errorf("original leader %s is no longer leader after removing an unrelated node (state=%s, term=%d) - the removed node likely disrupted it", leaderIDBefore, leader.State(), leader.Term())
	}
	termAfter := leader.Term()
	if termAfter > 3 {
		// Each legitimate step of this test (initial election, the joint
		// entry, the stable entry) touches the term at most trivially;
		// anything large means repeated disruptive re-elections happened.
		t.Errorf("leader's term climbed to %d after an unrelated removal and 500ms of settle time - looks like repeated disruptive elections, not a quiet removal", termAfter)
	}

	// The cluster must still be able to commit with the leader unchanged.
	if err := tc.applyRetry([]byte("still-healthy")); err != nil {
		t.Fatalf("Apply after removal settled: %v", err)
	}

	// The victim itself should have learned it was removed (courtesy
	// push) and stopped calling elections - not strictly required for
	// cluster health (RequestVote's membership check alone prevents
	// disruption), but confirms the "stops cleanly" half of the fix, not
	// just the "harmless" half.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && victim.MembershipPhase() != "stable" {
		time.Sleep(20 * time.Millisecond)
	}
	if ids := victim.MemberIDs(); containsID(toPeers(ids), victimID) {
		t.Errorf("removed node %s still believes it's a cluster member: %v", victimID, ids)
	}
}

func toPeers(ids []string) []Peer {
	peers := make([]Peer, len(ids))
	for i, id := range ids {
		peers[i] = Peer{ID: id}
	}
	return peers
}

// MemberIDsAsPeers is a small test helper exposing the current voting
// peer list (not just IDs) without reaching into Node internals from the
// test file.
func (n *Node) MemberIDsAsPeers() []Peer {
	n.mu.Lock()
	defer n.mu.Unlock()
	return append([]Peer(nil), n.votingPeersLocked()...)
}
