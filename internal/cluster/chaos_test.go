package cluster

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"
)

// TestMinorityLeaderCannotCommit: a leader cut off from the majority must
// not be able to commit; the majority elects a new leader and keeps going;
// after healing, the old leader's un-replicated entry is discarded and
// every node converges on the same history.
func TestMinorityLeaderCannotCommit(t *testing.T) {
	tc := newTestCluster(t, 5)
	defer tc.closeAll()
	old := tc.awaitLeader(3 * time.Second)
	if err := old.Apply([]byte("before"), 2*time.Second); err != nil {
		t.Fatal(err)
	}
	// Isolate the old leader alone.
	g := map[string]int{}
	for _, p := range tc.peers {
		g[p.ID] = 1
	}
	g[old.cfg.NodeID] = 0
	tc.setPartition(g)

	if err := old.Apply([]byte("doomed"), 600*time.Millisecond); err == nil {
		t.Fatal("isolated leader committed an entry without a majority")
	}
	// The majority side elects its own leader and commits.
	var nl *Node
	waitFor(t, 5*time.Second, "majority-side leader", func() bool {
		for _, n := range tc.live() {
			if n != old && n.IsLeader() {
				nl = n
				return true
			}
		}
		return false
	})
	if err := nl.Apply([]byte("after"), 3*time.Second); err != nil {
		t.Fatalf("majority leader could not commit: %v", err)
	}

	tc.setPartition(nil)
	waitFor(t, 8*time.Second, "old leader to step down and converge", func() bool {
		return !old.IsLeader() && tc.appliedCount(old.cfg.NodeID) == 2
	})
	tc.mu.Lock()
	defer tc.mu.Unlock()
	for id, got := range tc.applied {
		if len(got) != 2 || string(got[0]) != "before" || string(got[1]) != "after" {
			t.Fatalf("%s history = %q; want [before after] (the doomed entry must never apply)", id, got)
		}
	}
}

// TestChaosPartitions hammers a 5-node cluster with writers while the
// network is randomly partitioned and healed, then checks the Raft
// safety properties: every acknowledged write is applied everywhere, all
// nodes apply the same sequence, and no term ever has two leaders.
func TestChaosPartitions(t *testing.T) {
	tc := newTestCluster(t, 5)
	defer tc.closeAll()
	tc.awaitLeader(3 * time.Second)
	rng := rand.New(rand.NewSource(42))

	stop := make(chan struct{})
	var wg sync.WaitGroup
	var ackMu sync.Mutex
	acked := map[string]bool{}

	// Writers: keep proposing unique values on whoever claims leadership.
	for w := 0; w < 3; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				for _, n := range tc.live() {
					if !n.IsLeader() {
						continue
					}
					v := fmt.Sprintf("w%d-%d", w, i)
					if n.Apply([]byte(v), 400*time.Millisecond) == nil {
						ackMu.Lock()
						acked[v] = true
						ackMu.Unlock()
					}
					break
				}
				time.Sleep(5 * time.Millisecond)
			}
		}(w)
	}

	// Election-safety observer: (term -> leader) must be a function.
	leaderOf := map[uint64]string{}
	var violation string
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			case <-time.After(5 * time.Millisecond):
			}
			for _, n := range tc.live() {
				n.mu.Lock()
				isL, term := n.state == Leader, n.currentTerm
				n.mu.Unlock()
				if !isL {
					continue
				}
				if prev, ok := leaderOf[term]; ok && prev != n.cfg.NodeID {
					violation = fmt.Sprintf("term %d has two leaders: %s and %s", term, prev, n.cfg.NodeID)
				}
				leaderOf[term] = n.cfg.NodeID
			}
		}
	}()

	// Chaos: random partitions every ~300-700ms for ~8s.
	end := time.Now().Add(8 * time.Second)
	for time.Now().Before(end) {
		g := map[string]int{}
		switch rng.Intn(3) {
		case 0: // isolate one random node
			victim := tc.peers[rng.Intn(5)].ID
			for _, p := range tc.peers {
				g[p.ID] = 1
			}
			g[victim] = 0
		case 1: // 2 / 3 split
			perm := rng.Perm(5)
			for k, idx := range perm {
				g[tc.peers[idx].ID] = k / 2 % 2
			}
		case 2: // heal
			g = nil
		}
		tc.setPartition(g)
		time.Sleep(time.Duration(300+rng.Intn(400)) * time.Millisecond)
	}
	tc.setPartition(nil)
	close(stop)
	wg.Wait()

	if violation != "" {
		t.Fatal(violation)
	}
	ackMu.Lock()
	nAcked := len(acked)
	ackMu.Unlock()
	if nAcked == 0 {
		t.Fatal("no writes were acknowledged at all - the test exercised nothing")
	}
	t.Logf("%d writes acknowledged across %d terms", nAcked, len(leaderOf))

	// Convergence: all nodes apply the same sequence.
	waitFor(t, 15*time.Second, "all nodes to converge on one history", func() bool {
		tc.mu.Lock()
		defer tc.mu.Unlock()
		var ref [][]byte
		for _, p := range tc.peers {
			got := tc.applied[p.ID]
			if ref == nil {
				ref = got
				continue
			}
			if len(got) != len(ref) {
				return false
			}
		}
		return len(ref) >= nAcked
	})
	tc.mu.Lock()
	defer tc.mu.Unlock()
	ref := tc.applied[tc.peers[0].ID]
	for _, p := range tc.peers[1:] {
		got := tc.applied[p.ID]
		for i := range ref {
			if string(got[i]) != string(ref[i]) {
				t.Fatalf("%s diverges at position %d: %q vs %q", p.ID, i, got[i], ref[i])
			}
		}
	}
	seen := map[string]int{}
	for _, d := range ref {
		seen[string(d)]++
	}
	ackMu.Lock()
	defer ackMu.Unlock()
	for v := range acked {
		if seen[v] != 1 {
			t.Fatalf("acknowledged write %q appears %d times in the applied history (want exactly 1)", v, seen[v])
		}
	}
}

// A fresh leader must commit on its own (Raft §8 no-op) without waiting for
// a client write, so entries inherited from earlier terms don't sit
// uncommitted indefinitely on a quiet cluster.
func TestNewLeaderCommitsNoopWithoutWrites(t *testing.T) {
	tc := newTestCluster(t, 3)
	defer tc.closeAll()
	l := tc.awaitLeader(3 * time.Second)
	waitFor(t, 3*time.Second, "leader's no-op to commit and apply", func() bool {
		l.mu.Lock()
		defer l.mu.Unlock()
		return l.commitIndex >= 1 && l.lastApplied >= 1
	})
	if n := tc.appliedCount(l.cfg.NodeID); n != 0 {
		t.Fatalf("no-op reached the state machine: %d entries applied", n)
	}
}
