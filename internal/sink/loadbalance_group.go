package sink

import (
	"context"
	"fmt"
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
	Selector        string
	BatchSize       int
	BatchLinger     time.Duration
	Parallelism     int
	InFlightBatches int
	MaxRetries      int
	RetryBackoff    time.Duration
	Compression     bool
	TLS             *config.TLSConfig
	AuthToken       string
}

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
