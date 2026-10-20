// This file: ShardedChannel partitions one logical channel's events across
// N independent Raft groups (shards), each a full ReplicatedChannel with
// its own leader election, log, replication, and batching - completely
// unaware of any other shard. This is the other axis of scaling
// clustering can offer: ReplicatedChannel's own batching raised one
// group's ceiling roughly 7x (~113/sec to ~794/sec, see the main README's
// "Clustered collector (Raft)" Verified behavior), but it's still one
// leader's batched-commit rate - a single Raft group doesn't get faster
// by adding more nodes to it, only more durable. Sharding gets past that
// ceiling by running several such groups side by side: with S shards, S
// different nodes can each be leading a different shard at the same
// time, so aggregate write throughput scales roughly with S instead of
// being capped by any one leader.
//
// What sharding does NOT give you, and why that's a deliberate,
// documented trade-off rather than an oversight:
//
//   - No ordering across shards. Each shard is its own totally-ordered
//     Raft log; there was never a cross-shard ordering guarantee to
//     begin with (this project's channels never promised one across
//     concurrent Put() callers anyway), but it's worth stating plainly:
//     two events that land on different shards have no defined relative
//     order, ever, even if one was Put() before the other returned.
//   - No cross-shard atomicity. A single Put() call always goes to
//     exactly one shard - there's no multi-shard transaction.
//   - No automatic rebalancing. The shard count and each shard's peer
//     list are fixed at config time; splitting or merging shards, or
//     moving a key range from one shard to another, isn't implemented.
//     Each shard's own membership can still change live (AddServer/
//     RemoveServer - see ReplicatedChannel), but that changes who
//     replicates *that* shard, not how many shards there are.
//
// What routing a Put() to a shard DOES preserve: client transparency.
// Exactly like a single (non-sharded) ReplicatedChannel, any node in any
// shard can accept a client's write - ShardedChannel.Put just picks a
// shard first, then that shard's own ReplicatedChannel.Put does the
// usual "I'm leader, apply it" or "forward to whoever is" dance,
// completely unaware it's one of several shards. A gRPC source calling
// Put() never needs to know sharding is involved at all.

package cluster

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"sync"
	"sync/atomic"
	"time"

	"logflow/internal/channel"
)

// ShardKeyFunc extracts a routing key from an event. Events that return
// the same non-empty key always land on the same shard (consistent
// hashing by key, not by position) - use this to keep everything from one
// logical source on one shard's single ordered log, when that matters
// more than maximally even load spread. A nil ShardKeyFunc, or one that
// returns "" for a given event, falls back to round-robin: maximum write
// throughput spread, zero ordering preference.
type ShardKeyFunc func(e *channel.Event) string

// ShardInfo is one shard's point-in-time cluster state, the sharded
// analogue of the ClusterState/ClusterTerm/etc. methods a plain
// ReplicatedChannel exposes - see ShardedChannel.Shards.
type ShardInfo struct {
	Index    int
	State    string
	Term     uint64
	LeaderID string
	Members  []string
	Phase    string
}

// ShardedChannel implements channel.Channel by partitioning events across
// several independent ReplicatedChannel shards - see this file's package
// doc comment for the design and its trade-offs.
type ShardedChannel struct {
	name   string
	shards []*ReplicatedChannel
	keyFn  ShardKeyFunc
	rr     uint64 // round-robin counter, used when keyFn is nil or returns ""

	out    chan takeResult
	closed chan struct{}
	wg     sync.WaitGroup
}

type takeResult struct {
	event *channel.Event
	err   error
}

// NewShardedChannel wires shards (every shard's own ReplicatedChannel,
// already constructed and running - see agent.buildShardedChannel) behind
// a single channel.Channel. It takes ownership of shards: closing the
// returned ShardedChannel closes every one of them.
func NewShardedChannel(name string, shards []*ReplicatedChannel, keyFn ShardKeyFunc) *ShardedChannel {
	sc := &ShardedChannel{
		name: name, shards: shards, keyFn: keyFn,
		out: make(chan takeResult), closed: make(chan struct{}),
	}
	for _, s := range shards {
		sc.wg.Add(1)
		go sc.fanIn(s)
	}
	return sc
}

// fanIn repeatedly calls one shard's Take() and forwards whatever it
// yields into the shared out channel, so ShardedChannel.Take can read
// from "whichever shard has something ready" without the caller needing
// to know how many shards there are or poll each one itself. A short
// per-call timeout keeps this responsive to Close() and avoids one
// permanently-idle shard (e.g. a follower shard whose leader lives on a
// different physical node, or simply a quiet shard) blocking this
// goroutine from ever rechecking sc.closed.
func (sc *ShardedChannel) fanIn(s *ReplicatedChannel) {
	defer sc.wg.Done()
	for {
		select {
		case <-sc.closed:
			return
		default:
		}
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		e, err := s.Take(ctx)
		cancel()
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				continue // nothing ready on this shard within the window - normal, not an error
			}
			// Shard closed (channel.ErrClosed) or some other terminal
			// error: stop servicing this shard. sc.closed will also be
			// closed very shortly after in the normal Close() path (it
			// closes every shard), at which point every fanIn goroutine
			// exits anyway.
			return
		}
		select {
		case sc.out <- takeResult{event: e}:
		case <-sc.closed:
			return
		}
	}
}

func (sc *ShardedChannel) Name() string { return sc.name }

// Put picks a shard for e (via keyFn if set and it returns a key,
// otherwise round-robin) and delegates - see this file's package doc
// comment for what that shard assignment does and doesn't guarantee.
func (sc *ShardedChannel) Put(ctx context.Context, e *channel.Event) error {
	return sc.shards[sc.shardIndex(e)].Put(ctx, e)
}

func (sc *ShardedChannel) shardIndex(e *channel.Event) int {
	if sc.keyFn != nil {
		if key := sc.keyFn(e); key != "" {
			h := fnv.New32a()
			h.Write([]byte(key))
			return int(h.Sum32() % uint32(len(sc.shards)))
		}
	}
	n := atomic.AddUint64(&sc.rr, 1)
	return int(n % uint64(len(sc.shards)))
}

// Take returns whatever event any shard's fanIn goroutine produces next -
// see fanIn. There's no ordering across shards (by design - see this
// file's package doc comment), so which shard an event came from is not
// observable to the caller, same as a caller of a plain (non-sharded)
// channel never knows which internal mechanism produced an event.
func (sc *ShardedChannel) Take(ctx context.Context) (*channel.Event, error) {
	select {
	case r := <-sc.out:
		return r.event, r.err
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-sc.closed:
		return nil, channel.ErrClosed
	}
}

// Stats aggregates every shard's Stats() - the whole-channel view a
// caller (e.g. /stats, the Prometheus collector) expects, same shape as
// any non-sharded channel's Stats().
func (sc *ShardedChannel) Stats() channel.Stats {
	var agg channel.Stats
	for _, s := range sc.shards {
		st := s.Stats()
		agg.Capacity += st.Capacity
		agg.Size += st.Size
		agg.PutCount += st.PutCount
		agg.TakeCount += st.TakeCount
		agg.DroppedCount += st.DroppedCount
	}
	return agg
}

func (sc *ShardedChannel) Close() error {
	close(sc.closed)
	sc.wg.Wait() // let every fanIn goroutine notice and exit before closing the shards out from under them
	var firstErr error
	for _, s := range sc.shards {
		if err := s.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// ShardCount and Shards expose per-shard cluster state - the sharded
// analogue of ReplicatedChannel's ClusterState()/ClusterTerm()/etc.,
// surfaced the same way (via /stats and Prometheus) through the same
// type-assertion pattern.
func (sc *ShardedChannel) ShardCount() int { return len(sc.shards) }

func (sc *ShardedChannel) Shards() []ShardInfo {
	out := make([]ShardInfo, len(sc.shards))
	for i, s := range sc.shards {
		out[i] = ShardInfo{
			Index: i, State: s.ClusterState(), Term: s.ClusterTerm(),
			LeaderID: s.ClusterLeaderID(), Members: s.ClusterMembers(), Phase: s.ClusterPhase(),
		}
	}
	return out
}

// AddServer and RemoveServer change one shard's membership live - see
// ReplicatedChannel.AddServer/RemoveServer. A new physical node typically
// joins every shard it's meant to participate in, which means one call
// per shard (each shard needs that node's own address for *that* shard's
// Raft group - see this file's package doc comment on why shards don't
// share a peer list at all); there's deliberately no "add to every shard
// in one call" helper, since the per-shard addresses are never the same
// across shards and inventing one would just hide that.
func (sc *ShardedChannel) AddServer(shard int, p Peer, timeout time.Duration) error {
	if shard < 0 || shard >= len(sc.shards) {
		return fmt.Errorf("cluster: shard index %d out of range (have %d shard(s))", shard, len(sc.shards))
	}
	return sc.shards[shard].AddServer(p, timeout)
}

func (sc *ShardedChannel) RemoveServer(shard int, id string, timeout time.Duration) error {
	if shard < 0 || shard >= len(sc.shards) {
		return fmt.Errorf("cluster: shard index %d out of range (have %d shard(s))", shard, len(sc.shards))
	}
	return sc.shards[shard].RemoveServer(id, timeout)
}

// Spilled/PurgedUnconsumed passthrough (summed across shards), so a
// sharded channel keeps reporting the same /stats fields a non-sharded
// one does - same reasoning as ReplicatedChannel's own passthrough of
// these two.
func (sc *ShardedChannel) Spilled() int64 {
	var total int64
	for _, s := range sc.shards {
		total += s.Spilled()
	}
	return total
}

func (sc *ShardedChannel) PurgedUnconsumed() int64 {
	var total int64
	for _, s := range sc.shards {
		total += s.PurgedUnconsumed()
	}
	return total
}

// ApplyStuck/ApplyStuckSeconds/ApplyStallWarnings passthrough (OR'd/
// max'd/summed across shards, the shapes that make sense for each) - see
// ReplicatedChannel.putLocalWatched's doc comment. A sharded channel is
// "stuck" if any one of its independent shards is - one shard's local
// channel backing up doesn't block the others (they're independent Raft
// groups, see this file's doc comment), but it's still something an
// operator watching this sharded channel as a whole needs to see.
func (sc *ShardedChannel) ApplyStuck() bool {
	for _, s := range sc.shards {
		if s.ApplyStuck() {
			return true
		}
	}
	return false
}

func (sc *ShardedChannel) ApplyStuckSeconds() float64 {
	var max float64
	for _, s := range sc.shards {
		if v := s.ApplyStuckSeconds(); v > max {
			max = v
		}
	}
	return max
}

func (sc *ShardedChannel) ApplyStallWarnings() int64 {
	var total int64
	for _, s := range sc.shards {
		total += s.ApplyStallWarnings()
	}
	return total
}

// ClusterLogEntries sums the retained Raft log entries over all shards;
// ClusterSnapshotIndex reports the highest compaction boundary of any shard.
func (sc *ShardedChannel) ClusterLogEntries() int {
	total := 0
	for _, s := range sc.shards {
		total += s.ClusterLogEntries()
	}
	return total
}

func (sc *ShardedChannel) ClusterSnapshotIndex() uint64 {
	var max uint64
	for _, s := range sc.shards {
		if v := s.ClusterSnapshotIndex(); v > max {
			max = v
		}
	}
	return max
}
