package sink

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	"logflow/internal/channel"
	"logflow/internal/config"
	"logflow/internal/metrics"
)

type LoadBalanceGroupConfig struct {
	Name            string
	Targets         []string
	Selector        string // round_robin | random
	BatchSize       int
	BatchLinger     time.Duration
	Parallelism     int
	InFlightBatches int
	MaxRetries      int
	RetryBackoff    time.Duration
	Compression     bool
	TLS             *config.TLSConfig
	AuthToken       string

	// Used only by "file:<dir>" targets - see FileTargetPrefix.
	FileRollMaxBytes int64
	FileRollInterval time.Duration
}

// LoadBalanceGroup is Flume's "load balancing sink processor": several
// gRPC backends share the work of draining one channel. It's implemented by
// running Parallelism independent GRPCSink workers, each permanently bound
// to one backend, and letting them all pull from the same channel
// concurrently - whichever worker is free next takes the next batch, which
// is itself a form of load balancing. For `round_robin`, workers are bound
// to targets in order (wrapping if there are more workers than targets);
// for `random`, each worker's target is chosen independently at random.
// A target of the form "file:<dir>" binds the worker to a rolling local
// file backend instead of a gRPC connection (see FileTargetPrefix), so a
// group can spread load across collectors AND a local spool.
// Any individual worker still reconnects/retries against its own target
// exactly like a standalone GRPCSink - a target being down only removes
// that fraction of capacity, it doesn't stall the others.
type LoadBalanceGroup struct {
	cfg     LoadBalanceGroupConfig
	channel channel.Channel
	metrics *metrics.Registry
}

func NewLoadBalanceGroup(cfg LoadBalanceGroupConfig, ch channel.Channel, reg *metrics.Registry) *LoadBalanceGroup {
	return &LoadBalanceGroup{cfg: cfg, channel: ch, metrics: reg}
}

func (g *LoadBalanceGroup) Run(ctx context.Context) error {
	if len(g.cfg.Targets) == 0 {
		return fmt.Errorf("sink group %s: no targets configured", g.cfg.Name)
	}
	var wg sync.WaitGroup
	for i := 0; i < g.cfg.Parallelism; i++ {
		target := g.pickTarget(i)
		if IsFileTarget(target) {
			fs := NewFileSink(FileSinkConfig{
				Name: fmt.Sprintf("%s-w%d", g.cfg.Name, i), Dir: FileTargetDir(target),
				BatchSize: g.cfg.BatchSize, BatchLinger: g.cfg.BatchLinger,
				RollMaxBytes: g.cfg.FileRollMaxBytes, RollInterval: g.cfg.FileRollInterval,
			}, g.channel, g.metrics)
			wg.Add(1)
			go func() {
				defer wg.Done()
				if err := fs.Run(ctx); err != nil {
					log.Printf("[loadbalance:%s] file backend %s: %v", g.cfg.Name, target, err)
				}
			}()
			continue
		}
		sub := NewGRPCSink(GRPCSinkConfig{
			Name:            fmt.Sprintf("%s/%s", g.cfg.Name, target),
			TargetAddr:      target,
			BatchSize:       g.cfg.BatchSize,
			BatchLinger:     g.cfg.BatchLinger,
			Parallelism:     1,
			InFlightBatches: g.cfg.InFlightBatches,
			MaxRetries:      g.cfg.MaxRetries,
			RetryBackoff:    g.cfg.RetryBackoff,
			Compression:     g.cfg.Compression,
			TLS:             g.cfg.TLS,
			AuthToken:       g.cfg.AuthToken,
		}, g.channel, g.metrics)
		wg.Add(1)
		go func() {
			defer wg.Done()
			sub.Run(ctx)
		}()
	}
	wg.Wait()
	return nil
}

func (g *LoadBalanceGroup) pickTarget(workerIdx int) string {
	if g.cfg.Selector == "random" {
		return g.cfg.Targets[rand.Intn(len(g.cfg.Targets))]
	}
	return g.cfg.Targets[workerIdx%len(g.cfg.Targets)]
}
