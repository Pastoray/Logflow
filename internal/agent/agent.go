// Package agent wires together the sources, channels, and sinks described
// by a Config into a running pipeline, and exposes an HTTP endpoint for
// metrics and health checks.
package agent

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"logflow/internal/channel"
	"logflow/internal/cluster"
	"logflow/internal/config"
	"logflow/internal/interceptor"
	"logflow/internal/metrics"
	"logflow/internal/router"
	"logflow/internal/security"
	"logflow/internal/sink"
	"logflow/internal/source"
)

type Agent struct {
	cfg     *config.Config
	metrics *metrics.Registry

	channels map[string]channel.Channel
	grpcSrcs []*source.GRPCSource
}

func New(cfg *config.Config) (*Agent, error) {
	a := &Agent{
		cfg:      cfg,
		metrics:  metrics.NewRegistry(),
		channels: make(map[string]channel.Channel),
	}
	for _, cc := range cfg.Channels {
		ch, err := buildChannel(cc)
		if err != nil {
			return nil, fmt.Errorf("channel %s: %w", cc.Name, err)
		}
		a.channels[cc.Name] = ch
	}
	// Channel size/capacity/put/take/dropped were previously only visible
	// on the human-readable /stats endpoint, not in /metrics at all - the
	// single most load-bearing set of numbers for this system (is a
	// channel backing up, is it near capacity, is it dropping) wasn't
	// queryable or alertable from Prometheus. This registers them as a
	// pull-based Collector so every scrape reads live Stats() rather than
	// going stale between scrapes the way a background-updated gauge would.
	a.metrics.Registerer().MustRegister(newChannelStatsCollector(a.channels))
	return a, nil
}

// buildSelector picks the channel-selection strategy for a source. With no
// explicit selector config, a single channel needs no fan-out logic and
// more than one defaults to replicating (send every event to all of them) -
// the same default Flume uses.
func buildSelector(sc config.SourceConfig, names []string) router.Selector {
	if sc.Selector == nil || sc.Selector.Type == "" || sc.Selector.Type == "replicating" {
		return &router.ReplicatingSelector{Channels: names}
	}
	return &router.MultiplexingSelector{
		Header:  sc.Selector.Header,
		Mapping: sc.Selector.Mapping,
		Default: sc.Selector.Default,
	}
}

func buildChannel(cc config.ChannelConfig) (channel.Channel, error) {
	if cc.ShardedCluster != nil {
		return buildShardedChannel(cc)
	}
	if cc.Cluster != nil {
		return buildClusteredChannel(cc)
	}
	switch cc.Type {
	case "", "memory":
		return channel.NewMemoryChannel(cc.Name, cc.Capacity, cc.Shards), nil
	case "file":
		retention := channel.RetentionPolicy{
			MaxAge: cc.RetentionMaxAge, MaxBytes: cc.RetentionMaxBytes, Interval: cc.CompactionInterval,
		}
		return newDurableLog(cc.Name, cc, retention, cc.Capacity)
	case "spillable":
		primary := channel.NewMemoryChannel(cc.Name+"-primary", cc.Capacity, cc.Shards)
		retention := channel.RetentionPolicy{
			MaxAge: cc.RetentionMaxAge, MaxBytes: cc.RetentionMaxBytes, Interval: cc.CompactionInterval,
		}
		overflow, err := newDurableLog(cc.Name+"-overflow", cc, retention, cc.OverflowCapacity)
		if err != nil {
			return nil, err
		}
		return channel.NewSpillableMemoryChannel(cc.Name, primary, overflow, cc.PollInterval), nil
	default:
		return nil, fmt.Errorf("unknown channel type %q", cc.Type)
	}
}

// buildClusteredChannel wraps the channel's normal underlying storage (file
// strongly recommended - see ClusterConfig's doc comment) in a
// cluster.ReplicatedChannel, replicating every Put across the configured
// Raft group. cc.Type still selects the underlying storage; cc.Cluster
// selects clustering on top of it.
func buildClusteredChannel(cc config.ChannelConfig) (channel.Channel, error) {
	local, err := buildChannel(config.ChannelConfig{
		Name: cc.Name, Type: cc.Type, Capacity: cc.Capacity, Shards: cc.Shards,
		Dir: cc.Dir, MaxSegmentSize: cc.MaxSegmentSize,
		RetentionMaxAge: cc.RetentionMaxAge, RetentionMaxBytes: cc.RetentionMaxBytes,
		CompactionInterval: cc.CompactionInterval,
	})
	if err != nil {
		return nil, fmt.Errorf("cluster %s: underlying channel: %w", cc.Name, err)
	}

	cfg := cc.Cluster
	clusterCfg, err := clusterConfigFrom(cfg)
	if err != nil {
		return nil, fmt.Errorf("cluster %s: %w", cc.Name, err)
	}
	rc, err := cluster.NewReplicatedChannel(cc.Name, local, clusterCfg)
	if err != nil {
		return nil, fmt.Errorf("cluster %s: %w", cc.Name, err)
	}
	if cfg.ApplyTimeout > 0 {
		rc.SetApplyTimeout(cfg.ApplyTimeout)
	}
	rc.SetBatching(cfg.BatchSize, cfg.BatchLinger)
	return rc, nil
}

// clusterConfigFrom translates one config.ClusterConfig (one shard's worth
// of Raft group description, or the whole group for a non-sharded
// cluster) into the cluster.Config NewReplicatedChannel/NewNode need.
// Shared by buildClusteredChannel (one call) and buildShardedChannel (one
// call per shard), so the Joining/bootstrap translation logic exists in
// exactly one place.
func clusterConfigFrom(cfg *config.ClusterConfig) (cluster.Config, error) {
	if cfg.Joining {
		if cfg.Self == nil {
			return cluster.Config{}, fmt.Errorf("joining requires cluster.self")
		}
		return cluster.Config{
			Joining:    true,
			Self:       cluster.Peer{ID: cfg.Self.ID, Addr: cfg.Self.Addr, ClientAddr: cfg.Self.ClientAddr},
			DataDir:    cfg.DataDir,
			ListenAddr: cfg.ListenAddr, ClientListenAddr: cfg.ClientListenAddr,
			ElectionTimeoutMin: cfg.ElectionTimeoutMin, ElectionTimeoutMax: cfg.ElectionTimeoutMax,
			HeartbeatInterval: cfg.HeartbeatInterval,
			SnapshotThreshold: cfg.SnapshotThreshold, SnapshotTrailing: cfg.SnapshotTrailing,
		}, nil
	}
	peers := make([]cluster.Peer, 0, len(cfg.Peers))
	for _, p := range cfg.Peers {
		peers = append(peers, cluster.Peer{ID: p.ID, Addr: p.Addr, ClientAddr: p.ClientAddr})
	}
	return cluster.Config{
		NodeID: cfg.NodeID, Peers: peers, DataDir: cfg.DataDir,
		ListenAddr: cfg.ListenAddr, ClientListenAddr: cfg.ClientListenAddr,
		ElectionTimeoutMin: cfg.ElectionTimeoutMin, ElectionTimeoutMax: cfg.ElectionTimeoutMax,
		HeartbeatInterval: cfg.HeartbeatInterval,
		SnapshotThreshold: cfg.SnapshotThreshold, SnapshotTrailing: cfg.SnapshotTrailing,
	}, nil
}

// buildShardedChannel builds one independent ReplicatedChannel per
// configured shard - each with its own underlying storage (a subdirectory
// of cc.Dir, so shards never collide on disk) and its own Raft group - and
// wires them behind a single cluster.ShardedChannel. See
// internal/cluster/sharded.go's doc comment for what sharding does and
// doesn't do.
func buildShardedChannel(cc config.ChannelConfig) (channel.Channel, error) {
	sc := cc.ShardedCluster
	if len(sc.Shards) == 0 {
		return nil, fmt.Errorf("sharded_cluster %s: at least one shard required", cc.Name)
	}
	shards := make([]*cluster.ReplicatedChannel, len(sc.Shards))
	for i := range sc.Shards {
		shardCfg := sc.Shards[i]
		local, err := buildChannel(config.ChannelConfig{
			Name: fmt.Sprintf("%s-shard-%d", cc.Name, i), Type: cc.Type, Capacity: cc.Capacity, Shards: cc.Shards,
			Dir: filepath.Join(cc.Dir, fmt.Sprintf("shard-%d", i)), MaxSegmentSize: cc.MaxSegmentSize,
			RetentionMaxAge: cc.RetentionMaxAge, RetentionMaxBytes: cc.RetentionMaxBytes,
			CompactionInterval: cc.CompactionInterval,
		})
		if err != nil {
			return nil, fmt.Errorf("sharded_cluster %s: shard %d: underlying channel: %w", cc.Name, i, err)
		}
		clusterCfg, err := clusterConfigFrom(&shardCfg)
		if err != nil {
			return nil, fmt.Errorf("sharded_cluster %s: shard %d: %w", cc.Name, i, err)
		}
		rc, err := cluster.NewReplicatedChannel(fmt.Sprintf("%s-shard-%d", cc.Name, i), local, clusterCfg)
		if err != nil {
			return nil, fmt.Errorf("sharded_cluster %s: shard %d: %w", cc.Name, i, err)
		}
		if shardCfg.ApplyTimeout > 0 {
			rc.SetApplyTimeout(shardCfg.ApplyTimeout)
		}
		rc.SetBatching(shardCfg.BatchSize, shardCfg.BatchLinger)
		shards[i] = rc
	}

	var keyFn cluster.ShardKeyFunc
	if header := sc.ShardKeyHeader; header != "" {
		keyFn = func(e *channel.Event) string { return e.Headers[header] }
	}
	return cluster.NewShardedChannel(cc.Name, shards, keyFn), nil
}

// Run starts every configured source and sink and the metrics/health HTTP
// server, and blocks until ctx is cancelled. Shutdown is then a controlled,
// staged drain rather than an all-at-once cancellation:
//
//  1. Sources are stopped first, so no new events are admitted.
//  2. Sinks keep running against buffered channel content until every
//     channel is empty or DrainTimeout elapses, whichever comes first.
//  3. Only then are sinks stopped, the HTTP server shut down, and channels
//     closed.
//
// If force is non-nil and a value arrives on it while a shutdown is in
// progress, the current wait (for sources to stop, or for the drain to
// finish) is abandoned immediately rather than waiting out its full
// timeout - the way a second Ctrl-C conventionally means "stop now,
// I understand this may lose buffered data."
func (a *Agent) Run(ctx context.Context, force <-chan struct{}) error {
	srcCtx, cancelSrc := context.WithCancel(context.Background())
	defer cancelSrc()
	sinkCtx, cancelSinks := context.WithCancel(context.Background())
	defer cancelSinks()

	var srcWG, sinkWG sync.WaitGroup

	httpSrv := a.startHTTP()

	for _, sc := range a.cfg.Sources {
		names := sc.Channels
		if sc.Channel != "" {
			names = append(names, sc.Channel)
		}
		chans := make([]channel.Channel, 0, len(names))
		chanMap := make(map[string]channel.Channel, len(names))
		for _, n := range names {
			ch, ok := a.channels[n]
			if !ok {
				return fmt.Errorf("source %s: unknown channel %s", sc.Name, n)
			}
			chans = append(chans, ch)
			chanMap[n] = ch
		}
		selector := buildSelector(sc, names)
		rt := router.New(chanMap, selector)

		chain, err := interceptor.Build(sc.Interceptors)
		if err != nil {
			return fmt.Errorf("source %s: %w", sc.Name, err)
		}

		switch sc.Type {
		case "grpc":
			gs := source.NewGRPCSource(sc.Name, a.cfg.AgentID, rt, chans, chain, a.metrics, sc.TLS, sc.AuthToken)
			if sc.AuthTokensFile != "" {
				ts, err := security.NewTokenStore(sc.AuthTokensFile)
				if err != nil {
					return fmt.Errorf("source %s: %w", sc.Name, err)
				}
				if err := a.metrics.Registerer().Register(ts); err != nil {
					return fmt.Errorf("source %s: %w", sc.Name, err)
				}
				gs.TokenStore = ts
			}
			a.grpcSrcs = append(a.grpcSrcs, gs)
			srcWG.Add(1)
			go func(gs *source.GRPCSource, addr string) {
				defer srcWG.Done()
				if err := gs.Serve(srcCtx, addr); err != nil && srcCtx.Err() == nil {
					log.Printf("source %s exited: %v", gs.Name, err)
				}
			}(gs, sc.ListenAddr)
		case "filetail":
			ft := &source.FileTailSource{
				Name: sc.Name, Path: sc.Path, Router: rt, Interceptors: chain,
				PollInterval: sc.PollInterval, Metrics: a.metrics,
			}
			srcWG.Add(1)
			go func() {
				defer srcWG.Done()
				if err := ft.Run(srcCtx); err != nil && srcCtx.Err() == nil {
					log.Printf("source %s exited: %v", sc.Name, err)
				}
			}()
		default:
			return fmt.Errorf("source %s: unknown type %q", sc.Name, sc.Type)
		}
	}

	for _, snk := range a.cfg.Sinks {
		ch, ok := a.channels[snk.Channel]
		if !ok {
			return fmt.Errorf("sink %s: unknown channel %s", snk.Name, snk.Channel)
		}
		runner, err := buildSinkRunner(snk, ch, a.metrics)
		if err != nil {
			return fmt.Errorf("sink %s: %w", snk.Name, err)
		}
		sinkWG.Add(1)
		go func(name string, r func(context.Context) error) {
			defer sinkWG.Done()
			if err := r(sinkCtx); err != nil && sinkCtx.Err() == nil {
				log.Printf("sink %s exited: %v", name, err)
			}
		}(snk.Name, runner)
	}

	for _, gc := range a.cfg.SinkGroups {
		ch, ok := a.channels[gc.Channel]
		if !ok {
			return fmt.Errorf("sink group %s: unknown channel %s", gc.Name, gc.Channel)
		}
		var runner func(context.Context) error
		switch gc.Processor {
		case "load_balance":
			g := sink.NewLoadBalanceGroup(sink.LoadBalanceGroupConfig{
				Name: gc.Name, Targets: gc.Targets, Selector: gc.Selector,
				BatchSize: gc.BatchSize, BatchLinger: gc.BatchLinger, Parallelism: gc.Parallelism,
				InFlightBatches: gc.InFlightBatches, MaxRetries: gc.MaxRetries,
				RetryBackoff: gc.RetryBackoff, Compression: gc.Compression,
				TLS: gc.TLS, AuthToken: gc.AuthToken,
				FileRollMaxBytes: gc.FileRollMaxBytes, FileRollInterval: gc.FileRollInterval,
			}, ch, a.metrics)
			runner = g.Run
		case "failover":
			g := sink.NewFailoverGroup(sink.FailoverGroupConfig{
				Name: gc.Name, Targets: gc.Targets,
				BatchSize: gc.BatchSize, BatchLinger: gc.BatchLinger,
				InFlightBatches: gc.InFlightBatches, MaxRetries: gc.MaxRetries,
				RetryBackoff: gc.RetryBackoff, Compression: gc.Compression,
				RecoveryProbeInterval: gc.RecoveryProbeInterval,
				TLS:                   gc.TLS, AuthToken: gc.AuthToken,
				FileRollMaxBytes: gc.FileRollMaxBytes, FileRollInterval: gc.FileRollInterval,
			}, ch, a.metrics)
			runner = g.Run
		default:
			return fmt.Errorf("sink group %s: unknown processor %q", gc.Name, gc.Processor)
		}
		sinkWG.Add(1)
		go func(name string, r func(context.Context) error) {
			defer sinkWG.Done()
			if err := r(sinkCtx); err != nil && sinkCtx.Err() == nil {
				log.Printf("sink group %s exited: %v", name, err)
			}
		}(gc.Name, runner)
	}

	log.Printf("agent %s running: %d source(s), %d channel(s), %d sink(s), %d sink group(s)",
		a.cfg.AgentID, len(a.cfg.Sources), len(a.cfg.Channels), len(a.cfg.Sinks), len(a.cfg.SinkGroups))

	<-ctx.Done()
	log.Printf("agent %s: shutdown initiated - stopping ingestion (no new events will be admitted)", a.cfg.AgentID)

	// Stage 1: stop sources first. gRPC sources GracefulStop and finish
	// any in-flight RPC; filetail stops polling. Bounded wait so a stuck
	// source can't block shutdown forever.
	cancelSrc()
	if !a.waitOrForce(&srcWG, force, 10*time.Second, "sources") {
		log.Printf("agent %s: proceeding to drain without waiting further for sources to fully stop", a.cfg.AgentID)
	}

	// Stage 2: let sinks keep draining whatever sources already admitted,
	// until every channel reports empty or DrainTimeout elapses.
	initial := a.totalBufferedEvents()
	if initial > 0 {
		log.Printf("agent %s: draining %d buffered event(s) across %d channel(s) (up to %s; per-channel overrides apply)...",
			a.cfg.AgentID, initial, len(a.channels), a.cfg.DrainTimeout)
	}
	drained, forced, timedOut := a.waitForDrain(force)
	remaining := a.totalBufferedEvents()
	switch {
	case initial == 0:
		// nothing to report
	case drained:
		log.Printf("agent %s: drain complete, all buffered events flushed", a.cfg.AgentID)
	case forced:
		log.Printf("agent %s: drain aborted by a second shutdown signal, %d event(s) left unflushed", a.cfg.AgentID, remaining)
	default:
		log.Printf("agent %s: drain timeout reached for channel(s) %s, %d event(s) left unflushed", a.cfg.AgentID, strings.Join(timedOut, ", "), remaining)
	}

	// Stage 3: stop sinks, then the HTTP server (kept up through the drain
	// so /stats stays available to observe progress), then close channels.
	cancelSinks()
	a.waitOrForce(&sinkWG, force, 10*time.Second, "sinks")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	_ = httpSrv.Shutdown(shutdownCtx)

	for _, ch := range a.channels {
		ch.Close()
	}
	log.Printf("agent %s: shutdown complete", a.cfg.AgentID)
	return nil
}

// totalBufferedEvents sums Size across every channel - the signal used to
// decide whether draining is complete.
func (a *Agent) totalBufferedEvents() int64 {
	var total int64
	for _, ch := range a.channels {
		total += ch.Stats().Size
	}
	return total
}

// waitOrForce waits for wg to finish, up to timeout, returning early if a
// value arrives on force. Returns true if wg finished normally.
func (a *Agent) waitOrForce(wg *sync.WaitGroup, force <-chan struct{}, timeout time.Duration, what string) bool {
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return true
	case <-force:
		log.Printf("agent %s: second shutdown signal received while waiting for %s to stop - not waiting further", a.cfg.AgentID, what)
		return false
	case <-time.After(timeout):
		log.Printf("agent %s: %s did not stop within %s - proceeding anyway", a.cfg.AgentID, what, timeout)
		return false
	}
}

// drainTimeoutFor returns the shutdown drain budget for one channel: its
// own drain_timeout if set, else the agent-wide one.
func (a *Agent) drainTimeoutFor(name string) time.Duration {
	for _, cc := range a.cfg.Channels {
		if cc.Name == name && cc.DrainTimeout > 0 {
			return cc.DrainTimeout
		}
	}
	return a.cfg.DrainTimeout
}

// waitForDrain polls until every channel is either empty or past ITS OWN
// drain budget (see drainTimeoutFor), or force fires. A channel that blew
// its budget stops being waited on without holding up channels that are
// still within theirs. Returns (drained = every channel actually empty,
// forced, names of channels that hit their timeout non-empty).
func (a *Agent) waitForDrain(force <-chan struct{}) (drained, forced bool, timedOut []string) {
	start := time.Now()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		pending := false
		timedOut = timedOut[:0]
		for name, ch := range a.channels {
			if ch.Stats().Size == 0 {
				continue
			}
			if time.Since(start) >= a.drainTimeoutFor(name) {
				timedOut = append(timedOut, name)
				continue
			}
			pending = true
		}
		if !pending {
			return len(timedOut) == 0, false, append([]string(nil), timedOut...)
		}
		select {
		case <-ticker.C:
		case <-force:
			return false, true, nil
		}
	}
}

type runnable interface {
	Run(context.Context) error
}

func buildSinkRunner(sc config.SinkConfig, ch channel.Channel, reg *metrics.Registry) (func(context.Context) error, error) {
	var r runnable
	switch sc.Type {
	case "grpc":
		r = sink.NewGRPCSink(sink.GRPCSinkConfig{
			Name: sc.Name, TargetAddr: sc.TargetAddr, BatchSize: sc.BatchSize,
			BatchLinger: sc.BatchLinger, Parallelism: sc.Parallelism,
			InFlightBatches: sc.InFlightBatches, MaxRetries: sc.MaxRetries,
			RetryBackoff: sc.RetryBackoff, Compression: sc.Compression,
			TLS: sc.TLS, AuthToken: sc.AuthToken,
		}, ch, reg)
	case "file":
		r = sink.NewFileSink(sink.FileSinkConfig{
			Name: sc.Name, Dir: sc.Dir, BatchSize: sc.BatchSize,
			BatchLinger: sc.BatchLinger, RollMaxBytes: sc.RollMaxBytes,
			RollInterval: sc.RollInterval,
		}, ch, reg)
	case "stdout":
		r = &sink.StdoutSink{Name: sc.Name, Channel: ch, BatchSize: sc.BatchSize, BatchLinger: sc.BatchLinger, Metrics: reg}
	case "null":
		r = &sink.NullSink{Name: sc.Name, Channel: ch, BatchSize: sc.BatchSize, BatchLinger: sc.BatchLinger, Metrics: reg}
	default:
		return nil, fmt.Errorf("unknown sink type %q", sc.Type)
	}
	return r.Run, nil
}

func (a *Agent) startHTTP() *http.Server {
	mux := http.NewServeMux()
	mux.Handle("/metrics", a.metrics.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})
	mux.HandleFunc("/stats", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "agent: %s\n", a.cfg.AgentID)
		for name, ch := range a.channels {
			st := ch.Stats()
			fmt.Fprintf(w, "channel %-15s size=%d/%d put=%d take=%d dropped=%d",
				name, st.Size, st.Capacity, st.PutCount, st.TakeCount, st.DroppedCount)
			if sp, ok := ch.(interface{ Spilled() int64 }); ok {
				fmt.Fprintf(w, " spilled=%d", sp.Spilled())
			}
			if pu, ok := ch.(interface{ PurgedUnconsumed() int64 }); ok {
				if n := pu.PurgedUnconsumed(); n > 0 {
					fmt.Fprintf(w, " purged_unconsumed=%d", n)
				}
			}
			if cs, ok := ch.(interface {
				ClusterState() string
				ClusterTerm() uint64
				ClusterLeaderID() string
				ClusterLeaderChanges() int64
			}); ok {
				fmt.Fprintf(w, " cluster_state=%s cluster_term=%d cluster_leader=%s cluster_leader_changes=%d",
					cs.ClusterState(), cs.ClusterTerm(), cs.ClusterLeaderID(), cs.ClusterLeaderChanges())
				if mi, ok := ch.(interface {
					ClusterMembers() []string
					ClusterPhase() string
				}); ok {
					fmt.Fprintf(w, " cluster_phase=%s cluster_members=%s",
						mi.ClusterPhase(), strings.Join(mi.ClusterMembers(), ","))
				}
			}
			if sh, ok := ch.(interface {
				ShardCount() int
				Shards() []cluster.ShardInfo
			}); ok {
				fmt.Fprintf(w, " shards=%d", sh.ShardCount())
				for _, si := range sh.Shards() {
					fmt.Fprintf(w, " shard[%d]_state=%s shard[%d]_term=%d shard[%d]_leader=%s shard[%d]_phase=%s shard[%d]_members=%s",
						si.Index, si.State, si.Index, si.Term, si.Index, si.LeaderID, si.Index, si.Phase, si.Index, strings.Join(si.Members, ","))
				}
			}
			if lg, ok := ch.(interface {
				ClusterLogEntries() int
				ClusterSnapshotIndex() uint64
			}); ok {
				fmt.Fprintf(w, " raft_log_entries=%d raft_snapshot_index=%d", lg.ClusterLogEntries(), lg.ClusterSnapshotIndex())
			}
			if aw, ok := ch.(interface {
				ApplyStuck() bool
				ApplyStuckSeconds() float64
				ApplyStallWarnings() int64
			}); ok {
				if aw.ApplyStuck() {
					fmt.Fprintf(w, " apply_stuck=true apply_stuck_seconds=%.0f", aw.ApplyStuckSeconds())
				}
				if n := aw.ApplyStallWarnings(); n > 0 {
					fmt.Fprintf(w, " apply_stall_warnings_total=%d", n)
				}
			}
			fmt.Fprintln(w)
		}
	})
	mux.HandleFunc("/cluster/add", func(w http.ResponseWriter, r *http.Request) {
		a.handleClusterMembership(w, r, true)
	})
	mux.HandleFunc("/cluster/remove", func(w http.ResponseWriter, r *http.Request) {
		a.handleClusterMembership(w, r, false)
	})
	srv := &http.Server{Addr: a.cfg.HTTPAddr, Handler: mux}
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("http server error: %v", err)
		}
	}()
	log.Printf("agent %s http (metrics/health/stats) on %s", a.cfg.AgentID, a.cfg.HTTPAddr)
	return srv
}

// clusterMember is the subset of cluster.ReplicatedChannel's live
// membership-change API a non-sharded channel needs to satisfy for
// /cluster/add and /cluster/remove to operate on it.
type clusterMember interface {
	AddServer(p cluster.Peer, timeout time.Duration) error
	RemoveServer(id string, timeout time.Duration) error
}

// shardedClusterMember is the sharded analogue of clusterMember -
// cluster.ShardedChannel's membership-change API, which takes an extra
// shard index since each shard is its own independent Raft group with its
// own peer list (see internal/cluster/sharded.go's doc comment).
type shardedClusterMember interface {
	AddServer(shard int, p cluster.Peer, timeout time.Duration) error
	RemoveServer(shard int, id string, timeout time.Duration) error
	ShardCount() int
}

// findClusterChannel resolves which clustered or sharded-clustered channel
// a /cluster/add or /cluster/remove request targets: the explicit
// ?channel= name if given, or the agent's sole clustered channel if
// there's exactly one (the common case - most agents have one clustered
// channel, if any). It returns the raw channel.Channel; the caller type-
// asserts it to clusterMember or shardedClusterMember as appropriate.
func (a *Agent) findClusterChannel(name string) (channel.Channel, error) {
	isClustered := func(ch channel.Channel) bool {
		if _, ok := ch.(clusterMember); ok {
			return true
		}
		_, ok := ch.(shardedClusterMember)
		return ok
	}
	if name != "" {
		ch, ok := a.channels[name]
		if !ok {
			return nil, fmt.Errorf("unknown channel %q", name)
		}
		if !isClustered(ch) {
			return nil, fmt.Errorf("channel %q is not clustered", name)
		}
		return ch, nil
	}
	var found channel.Channel
	for _, ch := range a.channels {
		if isClustered(ch) {
			if found != nil {
				return nil, fmt.Errorf("multiple clustered channels configured - specify ?channel=")
			}
			found = ch
		}
	}
	if found == nil {
		return nil, fmt.Errorf("no clustered channel configured")
	}
	return found, nil
}

// handleClusterMembership backs /cluster/add (add=true) and
// /cluster/remove (add=false): a live, no-restart way to grow or shrink a
// Raft group - see cluster.Node.AddServer/RemoveServer. Both are POST
// only, since they change cluster state.
//
// Non-sharded channel:
//
//	POST /cluster/add?id=collector-4&addr=10.0.0.4:8304&client_addr=10.0.0.4:8314[&channel=main]
//	POST /cluster/remove?id=collector-2[&channel=main]
//
// Sharded channel (&shard=N is required - each shard is its own
// independent Raft group, so the request must say which one):
//
//	POST /cluster/add?id=collector-4&addr=10.0.0.4:8304&client_addr=10.0.0.4:8314&shard=0[&channel=main]
//	POST /cluster/remove?id=collector-2&shard=0[&channel=main]
func (a *Agent) handleClusterMembership(w http.ResponseWriter, r *http.Request, add bool) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	ch, err := a.findClusterChannel(r.URL.Query().Get("channel"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	id := r.URL.Query().Get("id")
	if id == "" {
		http.Error(w, "id query parameter is required", http.StatusBadRequest)
		return
	}
	const membershipHTTPTimeout = 15 * time.Second

	scm, isSharded := ch.(shardedClusterMember)
	var shard int
	if isSharded {
		shardStr := r.URL.Query().Get("shard")
		if shardStr == "" {
			http.Error(w, "channel is sharded - shard query parameter is required", http.StatusBadRequest)
			return
		}
		n, err := strconv.Atoi(shardStr)
		if err != nil {
			http.Error(w, "shard query parameter must be an integer", http.StatusBadRequest)
			return
		}
		if n < 0 || n >= scm.ShardCount() {
			http.Error(w, fmt.Sprintf("shard %d out of range (have %d shard(s))", n, scm.ShardCount()), http.StatusBadRequest)
			return
		}
		shard = n
	}

	if add {
		addr := r.URL.Query().Get("addr")
		if addr == "" {
			http.Error(w, "addr query parameter is required", http.StatusBadRequest)
			return
		}
		p := cluster.Peer{ID: id, Addr: addr, ClientAddr: r.URL.Query().Get("client_addr")}
		if isSharded {
			if err := scm.AddServer(shard, p, membershipHTTPTimeout); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			fmt.Fprintf(w, "added %s (%s) to shard %d\n", id, addr, shard)
			return
		}
		cm := ch.(clusterMember)
		if err := cm.AddServer(p, membershipHTTPTimeout); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		fmt.Fprintf(w, "added %s (%s)\n", id, addr)
		return
	}

	if isSharded {
		if err := scm.RemoveServer(shard, id, membershipHTTPTimeout); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		fmt.Fprintf(w, "removed %s from shard %d\n", id, shard)
		return
	}
	cm := ch.(clusterMember)
	if err := cm.RemoveServer(id, membershipHTTPTimeout); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	fmt.Fprintf(w, "removed %s\n", id)
}

// newDurableLog builds the file-backed log for a file channel or a
// spillable channel's overflow: one WAL, or wal_shards parallel ones.
func newDurableLog(name string, cc config.ChannelConfig, retention channel.RetentionPolicy, capacity int) (interface {
	channel.Channel
	PurgedUnconsumed() int64
}, error) {
	if cc.WALShards > 1 {
		return channel.NewShardedFileChannel(name, cc.Dir, cc.WALShards, capacity, cc.MaxSegmentSize, retention)
	}
	return channel.NewFileChannel(name, cc.Dir, capacity, cc.MaxSegmentSize, retention)
}
