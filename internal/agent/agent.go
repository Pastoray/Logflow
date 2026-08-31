package agent

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"logflow/internal/channel"
	"logflow/internal/config"
	"logflow/internal/interceptor"
	"logflow/internal/metrics"
	"logflow/internal/router"
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
	return a, nil
}

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
	switch cc.Type {
	case "", "memory":
		return channel.NewMemoryChannel(cc.Name, cc.Capacity, cc.Shards), nil
	case "file":
		retention := channel.RetentionPolicy{
			MaxAge: cc.RetentionMaxAge, MaxBytes: cc.RetentionMaxBytes, Interval: cc.CompactionInterval,
		}
		return channel.NewFileChannel(cc.Name, cc.Dir, cc.Capacity, cc.MaxSegmentSize, retention)
	case "spillable":
		primary := channel.NewMemoryChannel(cc.Name+"-primary", cc.Capacity, cc.Shards)
		retention := channel.RetentionPolicy{
			MaxAge: cc.RetentionMaxAge, MaxBytes: cc.RetentionMaxBytes, Interval: cc.CompactionInterval,
		}
		overflow, err := channel.NewFileChannel(cc.Name+"-overflow", cc.Dir, cc.OverflowCapacity, cc.MaxSegmentSize, retention)
		if err != nil {
			return nil, err
		}
		return channel.NewSpillableMemoryChannel(cc.Name, primary, overflow, cc.PollInterval), nil
	default:
		return nil, fmt.Errorf("unknown channel type %q", cc.Type)
	}
}

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

	cancelSrc()
	if !a.waitOrForce(&srcWG, force, 10*time.Second, "sources") {
		log.Printf("agent %s: proceeding to drain without waiting further for sources to fully stop", a.cfg.AgentID)
	}

	initial := a.totalBufferedEvents()
	if initial > 0 {
		log.Printf("agent %s: draining %d buffered event(s) across %d channel(s) (up to %s)...",
			a.cfg.AgentID, initial, len(a.channels), a.cfg.DrainTimeout)
	}
	drained, forced := a.waitForDrain(force, a.cfg.DrainTimeout)
	remaining := a.totalBufferedEvents()
	switch {
	case initial == 0:
	case drained:
		log.Printf("agent %s: drain complete, all buffered events flushed", a.cfg.AgentID)
	case forced:
		log.Printf("agent %s: drain aborted by a second shutdown signal, %d event(s) left unflushed", a.cfg.AgentID, remaining)
	default:
		log.Printf("agent %s: drain timeout (%s) reached, %d event(s) left unflushed", a.cfg.AgentID, a.cfg.DrainTimeout, remaining)
	}

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

func (a *Agent) totalBufferedEvents() int64 {
	var total int64
	for _, ch := range a.channels {
		total += ch.Stats().Size
	}
	return total
}

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

func (a *Agent) waitForDrain(force <-chan struct{}, timeout time.Duration) (drained, forced bool) {
	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		if a.totalBufferedEvents() == 0 {
			return true, false
		}
		if time.Now().After(deadline) {
			return false, false
		}
		select {
		case <-ticker.C:
		case <-force:
			return false, true
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
			fmt.Fprintln(w)
		}
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
