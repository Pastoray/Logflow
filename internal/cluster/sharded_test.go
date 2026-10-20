package cluster

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"logflow/internal/channel"
)

// newShardOfOne builds a single-node ReplicatedChannel that is its own
// one-member Raft group - it becomes leader immediately (quorum of 1), so
// tests that only care about ShardedChannel's own routing/fan-in logic
// (not Raft election itself, which raft_test.go and channel_test.go
// already cover thoroughly) don't need to wait on elections or spin up
// real multi-node groups per shard.
func newShardOfOne(t *testing.T, dataDir, id string) *ReplicatedChannel {
	t.Helper()
	local := channel.NewMemoryChannel("shard-"+id, 10000, 1)
	peer := Peer{ID: id, Addr: fmt.Sprintf("127.0.0.1:%d", freePort(t)), ClientAddr: fmt.Sprintf("127.0.0.1:%d", freePort(t))}
	rc, err := NewReplicatedChannel("sharded-test-"+id, local, Config{
		NodeID: id, Peers: []Peer{peer}, DataDir: dataDir + "/" + id,
		ElectionTimeoutMin: 30 * time.Millisecond, ElectionTimeoutMax: 60 * time.Millisecond,
		HeartbeatInterval: 10 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("starting shard-of-one %s: %v", id, err)
	}
	return rc
}

// newTestShardedChannel builds a ShardedChannel out of n single-node
// shards (see newShardOfOne) and waits for every shard to elect itself
// leader before returning, so callers can Put() immediately.
//
// Ownership bookkeeping: ReplicatedChannel.Close has no sync.Once guard
// (unlike Node.Close), so it panics ("close of closed channel") if called
// twice - a shard must be closed exactly once, by whichever of
// ShardedChannel.Close or this function's own early-exit cleanup actually
// ends up owning it. Each shard is tracked in `owned` and registered for
// cleanup (t.Cleanup) the moment it's created, before waiting on its
// (near-instant, quorum-of-1) self-election - not after the whole loop
// finishes - so a t.Fatalf partway through (which unwinds via
// runtime.Goexit, running only already-registered cleanup) can't leak an
// earlier iteration's live node into a later test sharing this process
// (see the identical leak this same after-the-loop-defer mistake caused in
// TestShardedChannelIndependentRaftGroups, caught via its term number
// climbing into the thousands from an orphaned background node). Once
// every shard is successfully elected, ownership transfers to the
// returned ShardedChannel (its own Close() will close every shard), so
// `owned` is cleared and the per-shard cleanups become no-ops.
func newTestShardedChannel(t *testing.T, n int, keyFn ShardKeyFunc) *ShardedChannel {
	t.Helper()
	dataDir := t.TempDir()
	shards := make([]*ReplicatedChannel, n)
	owned := make([]*ReplicatedChannel, 0, n)
	t.Cleanup(func() {
		for _, rc := range owned {
			rc.Close()
		}
	})
	for i := 0; i < n; i++ {
		rc := newShardOfOne(t, dataDir, fmt.Sprintf("shard-%d-node", i))
		owned = append(owned, rc)
		deadline := time.Now().Add(2 * time.Second)
		for !rc.node.IsLeader() {
			if time.Now().After(deadline) {
				t.Fatalf("shard %d never elected itself leader", i)
			}
			time.Sleep(5 * time.Millisecond)
		}
		shards[i] = rc
	}
	owned = nil // every shard elected; ShardedChannel.Close (via the caller's own defer) now owns closing them
	return NewShardedChannel("sharded-test", shards, keyFn)
}

func mkShardEvent(body string) *channel.Event {
	return &channel.Event{Body: []byte(body), Timestamp: time.Now()}
}

// TestShardedChannelRoundRobinDistribution checks that with no ShardKeyFunc,
// sequential Put() calls spread evenly across every shard rather than all
// landing on one - the whole point of round-robin routing.
func TestShardedChannelRoundRobinDistribution(t *testing.T) {
	sc := newTestShardedChannel(t, 4, nil)
	defer sc.Close()

	const perShard = 25
	total := perShard * 4
	for i := 0; i < total; i++ {
		if err := sc.Put(context.Background(), mkShardEvent(fmt.Sprintf("e%d", i))); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}

	for i, shard := range sc.shards {
		st := shard.Stats()
		if st.PutCount != perShard {
			t.Errorf("shard %d: PutCount = %d, want %d (uneven round-robin spread)", i, st.PutCount, perShard)
		}
	}
}

// TestShardedChannelKeyRouting checks that events sharing a ShardKeyFunc
// key always land on the same shard (sticky routing), while different
// keys are free to land on different shards - i.e. routing is a function
// of the key, not of call order.
func TestShardedChannelKeyRouting(t *testing.T) {
	keyFn := func(e *channel.Event) string {
		return string(e.Body[:2]) // "k0", "k1", ... prefix used as the routing key
	}
	sc := newTestShardedChannel(t, 3, keyFn)
	defer sc.Close()

	firstShardForKey := map[string]int{}
	for round := 0; round < 10; round++ {
		for k := 0; k < 6; k++ {
			key := fmt.Sprintf("k%d", k)
			e := mkShardEvent(key + fmt.Sprintf("-round%d", round))
			idx := sc.shardIndex(e)
			if prev, ok := firstShardForKey[key]; ok {
				if prev != idx {
					t.Fatalf("key %q routed to shard %d on round 0 but shard %d on round %d - routing not sticky", key, prev, idx, round)
				}
			} else {
				firstShardForKey[key] = idx
			}
		}
	}

	distinct := map[int]bool{}
	for _, idx := range firstShardForKey {
		distinct[idx] = true
	}
	if len(distinct) < 2 {
		t.Errorf("all 6 keys hashed to the same %d shard(s) out of 3 - suspiciously degenerate hashing", len(distinct))
	}
}

// TestShardedChannelKeyRoutingEmptyKeyFallsBackToRoundRobin checks that an
// event for which keyFn returns "" doesn't get stuck on shard 0 (or any
// fixed shard) but falls back to the round-robin counter, same as a nil
// keyFn would.
func TestShardedChannelKeyRoutingEmptyKeyFallsBackToRoundRobin(t *testing.T) {
	keyFn := func(e *channel.Event) string { return "" }
	sc := newTestShardedChannel(t, 4, keyFn)
	defer sc.Close()

	seen := map[int]bool{}
	for i := 0; i < 40; i++ {
		seen[sc.shardIndex(mkShardEvent(fmt.Sprintf("e%d", i)))] = true
	}
	if len(seen) != 4 {
		t.Errorf("empty-key events landed on %d distinct shard(s), want all 4 (round-robin fallback not working)", len(seen))
	}
}

// TestShardedChannelTakeAggregatesAllShards puts events across every shard
// (round-robin) and confirms Take() eventually surfaces every single one
// exactly once, from whichever shard fanIn happens to pull it from - no
// loss, no duplication, across the whole set.
func TestShardedChannelTakeAggregatesAllShards(t *testing.T) {
	sc := newTestShardedChannel(t, 3, nil)
	defer sc.Close()

	const total = 60
	want := map[string]bool{}
	for i := 0; i < total; i++ {
		body := fmt.Sprintf("e%d", i)
		want[body] = true
		if err := sc.Put(context.Background(), mkShardEvent(body)); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}

	got := map[string]int{}
	deadline := time.Now().Add(5 * time.Second)
	for len(got) < total && time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		e, err := sc.Take(ctx)
		cancel()
		if err != nil {
			continue
		}
		got[string(e.Body)]++
	}

	if len(got) != total {
		t.Fatalf("Take() surfaced %d distinct events, want %d", len(got), total)
	}
	for body, count := range got {
		if count != 1 {
			t.Errorf("event %q delivered %d times, want exactly 1", body, count)
		}
		if !want[body] {
			t.Errorf("event %q was never Put()", body)
		}
	}
}

// TestShardedChannelStatsAggregates checks Stats() sums every shard's
// counters rather than, say, reporting only one shard's or overwriting
// instead of adding.
//
// It drains every event it Put() before checking TakeCount, rather than
// stopping partway through: each shard's fanIn goroutine (see fanIn's doc
// comment) eagerly calls the underlying shard's Take() in a loop and only
// then hands the result to sc.out, so a shard's TakeCount can tick up
// slightly ahead of how many events the caller has actually drained from
// ShardedChannel.Take() while events remain available - that's expected
// (nothing is lost; the event just sits in a goroutine's local variable
// until delivered), but it means partial-drain counts aren't deterministic
// and shouldn't be asserted on.
func TestShardedChannelStatsAggregates(t *testing.T) {
	sc := newTestShardedChannel(t, 3, nil)
	defer sc.Close()

	const total = 9
	for i := 0; i < total; i++ {
		if err := sc.Put(context.Background(), mkShardEvent(fmt.Sprintf("e%d", i))); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}
	for i := 0; i < total; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_, err := sc.Take(ctx)
		cancel()
		if err != nil {
			t.Fatalf("Take %d: %v", i, err)
		}
	}

	st := sc.Stats()
	if st.PutCount != total {
		t.Errorf("aggregate PutCount = %d, want %d", st.PutCount, total)
	}
	if st.TakeCount != total {
		t.Errorf("aggregate TakeCount = %d, want %d", st.TakeCount, total)
	}
	if st.Capacity != 3*10000 {
		t.Errorf("aggregate Capacity = %d, want %d (sum of each shard's 10000)", st.Capacity, 3*10000)
	}
}

// TestShardedChannelCloseIsPromptAndIdempotentPerShard checks Close()
// returns promptly (fanIn goroutines actually exit, no hang) and closes
// every underlying shard.
func TestShardedChannelCloseIsPromptAndIdempotentPerShard(t *testing.T) {
	sc := newTestShardedChannel(t, 3, nil)

	done := make(chan error, 1)
	go func() { done <- sc.Close() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Close() did not return within 3s - fanIn goroutine likely stuck")
	}

	// Take() after Close should return promptly with an error, not block.
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	if _, err := sc.Take(ctx); err == nil {
		t.Error("Take() after Close() returned nil error, want an error")
	}
}

// TestShardedChannelShardIndexOutOfRange checks AddServer/RemoveServer
// reject an out-of-range shard index cleanly instead of panicking - this
// is the path handleClusterMembership relies on to turn a bad ?shard=
// query parameter into a 4xx instead of crashing the agent.
func TestShardedChannelShardIndexOutOfRange(t *testing.T) {
	sc := newTestShardedChannel(t, 2, nil)
	defer sc.Close()

	if err := sc.AddServer(5, Peer{ID: "x", Addr: "127.0.0.1:1"}, time.Second); err == nil {
		t.Error("AddServer with out-of-range shard index returned nil error, want an error")
	}
	if err := sc.RemoveServer(-1, "x", time.Second); err == nil {
		t.Error("RemoveServer with out-of-range shard index returned nil error, want an error")
	}
	if sc.ShardCount() != 2 {
		t.Errorf("ShardCount() = %d, want 2", sc.ShardCount())
	}
}

// --- Real multi-Raft-group integration test ---

// shardGroup is one shard's own multi-node Raft group, built the same way
// replicatedTestCluster in channel_test.go builds a single group - this
// test just builds several of them and wires them into one ShardedChannel,
// to confirm shards really are independent Raft groups (can each elect
// their own leader, possibly on different "nodes") rather than secretly
// sharing state.
type shardGroup struct {
	peers []Peer
	rcs   map[string]*ReplicatedChannel
}

func newShardGroup(t *testing.T, dataDir string, nodes int) *shardGroup {
	t.Helper()
	sg := &shardGroup{peers: makePeers(t, nodes), rcs: make(map[string]*ReplicatedChannel)}
	for _, p := range sg.peers {
		local := channel.NewMemoryChannel("sg-"+p.ID, 10000, 1)
		// Wider election-timeout/heartbeat margins than the other tests in
		// this file (which use 60-120ms/20ms): this test keeps every
		// member of 2 real 3-node Raft groups running for its whole
		// duration (see TestShardedChannelIndependentRaftGroups - its
		// ShardedChannel only owns one RC per shard, the leader at
		// wiring-time, so a spurious re-election elsewhere in the group
		// would leave that RC's Take() correctly blocked in standby and
		// nothing else draining the new leader, the same way a real
		// deployment would need another physical node's pipeline to pick
		// up the slack). A wide margin makes a spurious election under
		// scheduler/-race jitter rare without slowing down the initial
		// election much.
		rc, err := NewReplicatedChannel("sg", local, Config{
			NodeID: p.ID, Peers: sg.peers, DataDir: dataDir + "/" + p.ID,
			ElectionTimeoutMin: 400 * time.Millisecond, ElectionTimeoutMax: 800 * time.Millisecond,
			HeartbeatInterval: 30 * time.Millisecond,
		})
		if err != nil {
			t.Fatalf("starting shard-group node %s: %v", p.ID, err)
		}
		sg.rcs[p.ID] = rc
	}
	return sg
}

func (sg *shardGroup) leader(t *testing.T, timeout time.Duration) *ReplicatedChannel {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, rc := range sg.rcs {
			if rc.node.IsLeader() {
				return rc
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("shard group: no leader elected within %s", timeout)
	return nil
}

func (sg *shardGroup) closeAll() {
	for _, rc := range sg.rcs {
		rc.Close()
	}
}

// TestShardedChannelIndependentRaftGroups builds a ShardedChannel out of 2
// shards, each a real 3-node Raft group (6 Node instances total), and
// confirms: (1) each shard elects its own leader independently (not
// necessarily the "same-numbered" node in both groups - there's no
// coupling), (2) writes round-robined across the ShardedChannel actually
// replicate through each shard's own Raft log and are delivered with zero
// loss end to end via ShardedChannel.Take().
func TestShardedChannelIndependentRaftGroups(t *testing.T) {
	dataDir := t.TempDir()
	const numShards = 2
	groups := make([]*shardGroup, numShards)
	shards := make([]*ReplicatedChannel, numShards)
	leaderIDs := make([]string, numShards)

	// Register each group's cleanup immediately as it's created, in the
	// same loop iteration - not after the whole loop finishes. leader()
	// below can itself t.Fatalf on a timeout, which unwinds via
	// runtime.Goexit and only runs defers already registered by then; a
	// single defer added after the loop would leave every earlier
	// iteration's live Raft goroutines (3 real nodes each) running forever
	// in the background, to be discovered only as mysteriously climbing
	// term numbers in some *later*, unrelated test sharing this process
	// (go test -count reruns the whole binary's tests sequentially in one
	// process) - exactly the kind of leak this comment is here to prevent
	// regressing.
	for i := 0; i < numShards; i++ {
		groups[i] = newShardGroup(t, fmt.Sprintf("%s/shard-%d", dataDir, i), 3)
		defer groups[i].closeAll()
		leader := groups[i].leader(t, 5*time.Second)
		leaderIDs[i] = leader.node.cfg.NodeID
		shards[i] = leader // use the already-elected leader's own RC; Put() would forward anyway if it weren't
		// Remove the chosen leader from its group's own map so the
		// deferred closeAll() above (which closes whatever's left in
		// each group, i.e. the followers) never double-closes it -
		// ShardedChannel takes ownership of shards[] and closes them
		// itself via sc.Close().
		delete(groups[i].rcs, leaderIDs[i])
	}

	if leaderIDs[0] == "" || leaderIDs[1] == "" {
		t.Fatal("expected both shards to elect a leader")
	}

	sc := NewShardedChannel("integration-test", shards, nil)
	defer sc.Close()

	// putWithRetry tolerates a transient "cluster: not leader" / no-leader
	// error, same as any real client of a clustered channel must: a brief
	// re-election (e.g. a borderline-short election timeout racing
	// scheduler jitter when the whole suite runs in parallel under -race)
	// can leave a shard briefly leaderless between this Put and the last
	// one, exactly like the pre-existing timing sensitivity documented for
	// TestApplyThroughput in raft_test.go - it is not specific to sharding.
	putWithRetry := func(e *channel.Event) error {
		var lastErr error
		for attempt := 0; attempt < 20; attempt++ {
			if err := sc.Put(context.Background(), e); err != nil {
				lastErr = err
				time.Sleep(50 * time.Millisecond)
				continue
			}
			return nil
		}
		return lastErr
	}

	const total = 40
	want := map[string]bool{}
	for i := 0; i < total; i++ {
		body := fmt.Sprintf("e%d", i)
		want[body] = true
		if err := putWithRetry(mkShardEvent(body)); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}

	got := map[string]int{}
	deadline := time.Now().Add(15 * time.Second)
	for len(got) < total && time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		e, err := sc.Take(ctx)
		cancel()
		if err != nil {
			continue
		}
		got[string(e.Body)]++
	}

	if len(got) != total {
		t.Fatalf("delivered %d/%d distinct events through 2 independent Raft groups", len(got), total)
	}
	for body, count := range got {
		if count != 1 {
			t.Errorf("event %q delivered %d times, want exactly 1", body, count)
		}
		if !want[body] {
			t.Errorf("event %q was never Put()", body)
		}
	}

	// Confirm both shards actually did independent, non-trivial Raft work
	// (term >= 1, each has its own 3-member view) rather than one shard
	// silently doing all the work.
	for i, rc := range shards {
		if term := rc.ClusterTerm(); term < 1 {
			t.Errorf("shard %d: ClusterTerm() = %d, want >= 1", i, term)
		}
		if members := rc.ClusterMembers(); len(members) != 3 {
			t.Errorf("shard %d: ClusterMembers() = %v, want 3 members", i, members)
		}
	}
}

// TestShardedChannelPutRoutesToCorrectShardInstance is a focused
// regression check that shardIndex() is actually consulted by Put() (not
// hardcoded to shard 0): Put many events with a key that hashes to a
// specific shard and confirm only that shard's PutCount increases.
func TestShardedChannelPutRoutesToCorrectShardInstance(t *testing.T) {
	sc := newTestShardedChannel(t, 5, nil)
	defer sc.Close()

	e := mkShardEvent("probe")
	target := sc.shardIndex(e)

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			idx := sc.shardIndex(mkShardEvent("probe-concurrent"))
			sc.shards[idx].Put(context.Background(), mkShardEvent("x"))
		}()
	}
	wg.Wait()

	if err := sc.shards[target].Put(context.Background(), e); err != nil {
		t.Fatalf("Put to target shard %d: %v", target, err)
	}
	if st := sc.shards[target].Stats(); st.PutCount < 1 {
		t.Errorf("target shard %d PutCount = %d, want >= 1", target, st.PutCount)
	}
}
