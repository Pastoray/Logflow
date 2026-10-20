package cluster

import (
	"context"
	"fmt"
	"log"
	"net/rpc"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/protobuf/proto"

	"logflow/gen/logflowpb"
	"logflow/internal/channel"
	"logflow/internal/util"
)

const (
	defaultBatchSize   = 200
	defaultBatchLinger = 50 * time.Millisecond

	// applyStallWarnInterval: how long a single event's Put() to the
	// local channel may be pending before putLocalWatched starts treating
	// it as "stuck" - see that function's doc comment.
	applyStallWarnInterval = 2 * time.Second
)

// ReplicatedChannel implements channel.Channel by replicating every Put
// through a Raft group before it lands in a local, underlying channel
// (normally a durable FileChannel - see ClusterConfig's doc comment).
// Every replica ends up holding an identical copy of every committed
// event; only the current Raft leader's Take() actually drains it, so a
// collector's sinks only run meaningfully on the leader; see
// agent.go's leaderGatedSink.
//
// Put() calls are batched into one Raft log entry per batch (one
// logflowpb.EventBatch, the same message type sources/sinks already use
// on the wire), not proposed one at a time - an early version did one
// Apply() per Put(), which measured at ~113 commits/sec on a 3-node
// localhost cluster (see internal/cluster's test suite's
// TestApplyThroughput) because every single event paid for its own full
// replication round trip. Batching amortizes that round trip across
// BatchSize events the same way GRPCSink already amortizes a network
// round trip across a batch - this is the same trade-off, applied to the
// same kind of bottleneck, one layer further up the stack.
type ReplicatedChannel struct {
	name  string
	local channel.Channel
	node  *Node
	apply time.Duration

	batchSize   int
	batchLinger time.Duration

	pendMu     sync.Mutex
	pendEvents []*logflowpb.Event
	pendDone   []chan error

	flushCh chan struct{}
	closed  chan struct{}
	closeWG sync.WaitGroup

	fwdMu   sync.Mutex
	fwdConn *rpc.Client
	fwdAddr string

	// applyStuckSinceUnixNano/applyStallWarnings back ApplyStuck/
	// ApplyStuckSeconds/ApplyStallWarnings - see putLocalWatched.
	applyStuckSinceUnixNano int64
	applyStallWarnings      int64
	applyStallInterval      time.Duration
}

// NewReplicatedChannel wires local (the durable storage every replica
// writes committed entries into) behind a Raft group described by
// clusterCfg. local must not be written to by anything else - Raft's
// applier goroutine is its only writer from this point on.
func NewReplicatedChannel(name string, local channel.Channel, clusterCfg Config) (*ReplicatedChannel, error) {
	rc := &ReplicatedChannel{
		name: name, local: local, apply: 5 * time.Second,
		batchSize: defaultBatchSize, batchLinger: defaultBatchLinger,
		flushCh:            make(chan struct{}, 1),
		closed:             make(chan struct{}),
		applyStallInterval: applyStallWarnInterval,
	}
	clusterCfg.ApplyFn = rc.applyToLocal
	node, err := NewNode(clusterCfg)
	if err != nil {
		return nil, err
	}
	rc.node = node
	rc.closeWG.Add(1)
	go rc.batchLoop()
	return rc, nil
}

// SetApplyTimeout overrides the default 5s Put()/Apply() budget.
func (rc *ReplicatedChannel) SetApplyTimeout(d time.Duration) {
	if d > 0 {
		rc.apply = d
	}
}

// SetBatching overrides the default batch size (200 events) / linger
// (50ms). size <= 0 or linger <= 0 leaves that setting at its default.
func (rc *ReplicatedChannel) SetBatching(size int, linger time.Duration) {
	if size > 0 {
		rc.batchSize = size
	}
	if linger > 0 {
		rc.batchLinger = linger
	}
}

// SetApplyStallWarnInterval overrides how long a single event's Put() to
// the local channel may be pending before putLocalWatched starts treating
// it as "stuck" (default 2s). Lower it to get an earlier signal on a
// deployment where that matters more than noise; tests use this to avoid
// waiting out the real default.
func (rc *ReplicatedChannel) SetApplyStallWarnInterval(d time.Duration) {
	if d > 0 {
		rc.applyStallInterval = d
	}
}

func (rc *ReplicatedChannel) applyToLocal(data []byte) {
	var batch logflowpb.EventBatch
	if err := proto.Unmarshal(data, &batch); err != nil {
		// A corrupt log entry would be a bug in this package (every
		// entry we ever propose is produced by this type's own flush,
		// below), not bad input from the network - surfacing it as
		// dropped events rather than panicking the applier goroutine and
		// wedging the whole Raft group.
		return
	}
	for _, pe := range batch.Events {
		e := util.FromPBEvent(pe)
		rc.putLocalWatched(e)
	}
}

// putLocalWatched applies e to the local underlying channel.
// context.Background() is still used for the actual Put, deliberately - a
// committed Raft entry must eventually be applied, never silently
// abandoned (see this package's doc comment), so this can never give up
// on it the way a bounded-context retry loop would have to. That rules
// out the obvious "retry with a short per-attempt timeout" shape: every
// channel.Channel implementation's own Put() already counts a
// context-cancelled attempt as a genuine dropped event (FileChannel's and
// MemoryChannel's onDrop(), surfaced as dropped_total) - repeatedly
// cancelling and retrying would corrupt that metric with false drops for
// an entry that was never actually abandoned, which would be a worse bug
// than the one this exists to fix.
//
// What this adds instead is purely observability, around the one
// still-unbounded call: a watchdog that, if the Put hasn't returned
// within applyStallWarnInterval, marks this apply as "stuck" (surfaced
// via ApplyStuck/ApplyStuckSeconds/ApplyStallWarnings - the same
// type-assertion pattern /stats and the Prometheus collector already use
// for Spilled()/ClusterState()) and logs a rate-limited warning, so a
// permanently-backed-up local channel is now visible to an operator - in
// /stats, in Prometheus, in the logs - instead of a silent goroutine
// block with no external signal at all. It does not change when or
// whether the entry is applied, only whether anyone can see it's taking
// too long.
func (rc *ReplicatedChannel) putLocalWatched(e *channel.Event) {
	done := make(chan struct{})
	start := time.Now()
	go func() {
		defer close(done)
		_ = rc.local.Put(context.Background(), e)
	}()

	timer := time.NewTimer(rc.applyStallInterval)
	defer timer.Stop()
	stalled := false
	for {
		select {
		case <-done:
			if stalled {
				atomic.StoreInt64(&rc.applyStuckSinceUnixNano, 0)
				log.Printf("cluster:%s apply recovered after being stuck for %s", rc.name, time.Since(start).Round(time.Second))
			}
			return
		case <-timer.C:
			stalled = true
			atomic.StoreInt64(&rc.applyStuckSinceUnixNano, start.UnixNano())
			atomic.AddInt64(&rc.applyStallWarnings, 1)
			log.Printf("cluster:%s apply stuck: local channel Put() has been pending for %s - this node's contribution to Raft quorum is blocked until it drains", rc.name, time.Since(start).Round(time.Second))
			timer.Reset(rc.applyStallInterval)
		}
	}
}

// ApplyStuck/ApplyStuckSeconds/ApplyStallWarnings expose putLocalWatched's
// stall tracking - see its doc comment. ApplyStuck is a point-in-time
// snapshot (it can flip back to false between two calls), the same way
// ClusterState() is; a caller that needs the cumulative count of how many
// times it stalled uses ApplyStallWarnings instead.
func (rc *ReplicatedChannel) ApplyStuck() bool {
	return atomic.LoadInt64(&rc.applyStuckSinceUnixNano) != 0
}

func (rc *ReplicatedChannel) ApplyStuckSeconds() float64 {
	since := atomic.LoadInt64(&rc.applyStuckSinceUnixNano)
	if since == 0 {
		return 0
	}
	return time.Since(time.Unix(0, since)).Seconds()
}

func (rc *ReplicatedChannel) ApplyStallWarnings() int64 {
	return atomic.LoadInt64(&rc.applyStallWarnings)
}

func (rc *ReplicatedChannel) Name() string { return rc.name }

// Put enqueues e into the current batch and waits for that batch to be
// committed (or fail), or for ctx to be done - whichever first. Enqueuing
// never blocks on the network itself; only this wait does, so many
// concurrent Put() callers (e.g. several gRPC streams) naturally pile into
// the same in-flight batch rather than serializing on it.
func (rc *ReplicatedChannel) Put(ctx context.Context, e *channel.Event) error {
	done := rc.enqueue(util.ToPBEvent(e))
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (rc *ReplicatedChannel) enqueue(pe *logflowpb.Event) chan error {
	done := make(chan error, 1)
	rc.pendMu.Lock()
	rc.pendEvents = append(rc.pendEvents, pe)
	rc.pendDone = append(rc.pendDone, done)
	n := len(rc.pendEvents)
	first := n == 1
	rc.pendMu.Unlock()

	if first {
		// Bound how long this (and anything that joins it before the
		// batch flushes) waits, even under low, sub-BatchSize traffic -
		// a one-shot timer per batch, not a per-Put reset, so this costs
		// one timer per batch rather than one per event.
		time.AfterFunc(rc.batchLinger, rc.requestFlush)
	}
	if n >= rc.batchSize {
		rc.requestFlush()
	}
	return done
}

func (rc *ReplicatedChannel) requestFlush() {
	select {
	case rc.flushCh <- struct{}{}:
	default:
	}
}

// batchLoop serializes every flush through one goroutine, so batches are
// proposed to Raft strictly in the order Put() callers joined them -
// concurrent flushes would let two batches race to be "entry N", and
// nothing downstream needs that ordering).
func (rc *ReplicatedChannel) batchLoop() {
	defer rc.closeWG.Done()
	for {
		select {
		case <-rc.closed:
			rc.flush() // best-effort final flush of whatever's left
			return
		case <-rc.flushCh:
			rc.flush()
		}
	}
}

func (rc *ReplicatedChannel) flush() {
	rc.pendMu.Lock()
	events := rc.pendEvents
	dones := rc.pendDone
	rc.pendEvents = nil
	rc.pendDone = nil
	rc.pendMu.Unlock()

	if len(events) == 0 {
		return
	}

	data, err := proto.Marshal(&logflowpb.EventBatch{Events: events})
	if err != nil {
		for _, d := range dones {
			d <- err
		}
		return
	}

	if rc.node.IsLeader() {
		err = rc.node.Apply(data, rc.apply)
	} else {
		err = rc.forwardToLeader(data)
	}
	for _, d := range dones {
		d <- err
	}
}

// forwardToLeader lets any node in the group accept a client's Put without
// the client (the gRPC source) needing to know or care who's leader: a
// follower just relays the write to whoever it currently believes is
// leader, and redirects once more if that belief turns out to be stale
// (e.g. a leader change raced the forward). Bounded to a couple of hops,
// not an unbounded retry loop - a client-visible error after that is
// correct backpressure during an election, not a bug.
func (rc *ReplicatedChannel) forwardToLeader(data []byte) error {
	peer, ok := rc.node.LeaderPeer()
	if !ok {
		return ErrNoLeader
	}
	addr := peer.ClientAddr
	for hop := 0; hop < 2; hop++ {
		reply, err := rc.callApply(addr, data)
		if err != nil {
			return fmt.Errorf("cluster: forwarding to leader %s: %w", addr, err)
		}
		if !reply.NotLeader {
			if reply.Err != "" {
				return fmt.Errorf("cluster: leader rejected apply: %s", reply.Err)
			}
			return nil
		}
		if reply.NewLeaderAddr == "" {
			return ErrNoLeader
		}
		addr = reply.NewLeaderAddr
	}
	return fmt.Errorf("cluster: too many leader redirects, last tried %s", addr)
}

func (rc *ReplicatedChannel) callApply(addr string, data []byte) (*ClientApplyReply, error) {
	rc.fwdMu.Lock()
	if rc.fwdConn == nil || rc.fwdAddr != addr {
		if rc.fwdConn != nil {
			rc.fwdConn.Close()
		}
		c, err := rpc.Dial("tcp", addr)
		if err != nil {
			rc.fwdMu.Unlock()
			return nil, err
		}
		rc.fwdConn, rc.fwdAddr = c, addr
	}
	conn := rc.fwdConn
	rc.fwdMu.Unlock()

	var reply ClientApplyReply
	if err := conn.Call("Raft.ClientApply", &ClientApplyArgs{Data: data}, &reply); err != nil {
		rc.fwdMu.Lock()
		if rc.fwdConn == conn {
			rc.fwdConn = nil
		}
		rc.fwdMu.Unlock()
		conn.Close()
		return nil, err
	}
	return &reply, nil
}

func (rc *ReplicatedChannel) Take(ctx context.Context) (*channel.Event, error) {
	// Only the leader actively drains - see this type's doc comment.
	// A follower's Take blocks here (not busy-polling the underlying
	// channel) until either this node becomes leader or ctx is done,
	// which naturally pauses that node's sinks while it's on standby.
	for {
		if rc.node.IsLeader() {
			return rc.local.Take(ctx)
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func (rc *ReplicatedChannel) Stats() channel.Stats { return rc.local.Stats() }

func (rc *ReplicatedChannel) Close() error {
	close(rc.closed)
	rc.closeWG.Wait() // let batchLoop's final flush deliver every pending Put() its result
	rc.node.Close()
	rc.fwdMu.Lock()
	if rc.fwdConn != nil {
		rc.fwdConn.Close()
	}
	rc.fwdMu.Unlock()
	return rc.local.Close()
}

// --- cluster-specific introspection, surfaced by /stats and metrics via
// the same type-assertion pattern channel_collector.go already uses for
// Spilled()/PurgedUnconsumed(). ---

func (rc *ReplicatedChannel) ClusterState() string         { return rc.node.State() }
func (rc *ReplicatedChannel) ClusterLogEntries() int       { return rc.node.LogEntries() }
func (rc *ReplicatedChannel) ClusterSnapshotIndex() uint64 { return rc.node.SnapshotIndex() }
func (rc *ReplicatedChannel) ClusterTerm() uint64          { return rc.node.Term() }
func (rc *ReplicatedChannel) ClusterLeaderChanges() int64  { return rc.node.LeaderChanges() }
func (rc *ReplicatedChannel) ClusterLeaderID() string {
	if p, ok := rc.node.LeaderPeer(); ok {
		return p.ID
	}
	return ""
}

// ClusterMembers and ClusterPhase expose the live membership (see
// internal/cluster's "Membership changes" doc comment) the same way the
// rest of this block exposes leader/term state - via /stats and
// Prometheus, through the same type-assertion pattern.
func (rc *ReplicatedChannel) ClusterMembers() []string { return rc.node.MemberIDs() }
func (rc *ReplicatedChannel) ClusterPhase() string     { return rc.node.MembershipPhase() }

// AddServer and RemoveServer change cluster membership live, with no
// restart of any node - see Node.AddServer/RemoveServer. Callable against
// any replica in the group, not just the leader (a non-leader forwards
// the request).
func (rc *ReplicatedChannel) AddServer(p Peer, timeout time.Duration) error {
	return rc.node.AddServer(p, timeout)
}

func (rc *ReplicatedChannel) RemoveServer(id string, timeout time.Duration) error {
	return rc.node.RemoveServer(id, timeout)
}

// Spilled/PurgedUnconsumed passthrough, so a clustered file channel keeps
// reporting the same /stats fields a non-clustered one does.
func (rc *ReplicatedChannel) Spilled() int64 {
	if sp, ok := rc.local.(interface{ Spilled() int64 }); ok {
		return sp.Spilled()
	}
	return 0
}

func (rc *ReplicatedChannel) PurgedUnconsumed() int64 {
	if pu, ok := rc.local.(interface{ PurgedUnconsumed() int64 }); ok {
		return pu.PurgedUnconsumed()
	}
	return 0
}
