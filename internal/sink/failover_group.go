package sink

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"

	"google.golang.org/grpc"

	"logflow/gen/logflowpb"
	"logflow/internal/channel"
	"logflow/internal/config"
	"logflow/internal/metrics"
	"logflow/internal/security"
)

type FailoverGroupConfig struct {
	Name            string
	Targets         []string
	BatchSize       int
	BatchLinger     time.Duration
	InFlightBatches int
	MaxRetries      int
	RetryBackoff    time.Duration
	Compression     bool
	TLS             *config.TLSConfig
	AuthToken       string

	RecoveryProbeInterval time.Duration
}

type FailoverGroup struct {
	cfg     FailoverGroupConfig
	channel channel.Channel
	metrics *metrics.Registry

	mu           sync.Mutex
	currentIdx   int
	downUntil    []time.Time
	activeCancel context.CancelFunc
}

func NewFailoverGroup(cfg FailoverGroupConfig, ch channel.Channel, reg *metrics.Registry) *FailoverGroup {
	return &FailoverGroup{
		cfg:       cfg,
		channel:   ch,
		metrics:   reg,
		downUntil: make([]time.Time, len(cfg.Targets)),
	}
}

func (g *FailoverGroup) Run(ctx context.Context) error {
	go g.recoveryProber(ctx)

	for ctx.Err() == nil {
		idx := g.getCurrentIdx()
		target := g.cfg.Targets[idx]

		subCtx, cancel := context.WithCancel(ctx)
		g.mu.Lock()
		g.activeCancel = cancel
		g.mu.Unlock()

		sub := NewGRPCSink(GRPCSinkConfig{
			Name: g.cfg.Name + "/" + target, TargetAddr: target,
			BatchSize: g.cfg.BatchSize, BatchLinger: g.cfg.BatchLinger,
			Parallelism: 1, InFlightBatches: g.cfg.InFlightBatches,
			MaxRetries: g.cfg.MaxRetries, RetryBackoff: g.cfg.RetryBackoff,
			Compression: g.cfg.Compression,
			TLS:         g.cfg.TLS, AuthToken: g.cfg.AuthToken,
		}, g.channel, g.metrics)

		log.Printf("[failover:%s] active backend -> %s (priority %d)", g.cfg.Name, target, idx)
		err := sub.DialAndStreamOnce(subCtx)
		cancel()

		if ctx.Err() != nil {
			return nil
		}
		if errors.Is(err, context.Canceled) {
			continue
		}

		log.Printf("[failover:%s] backend %s failed: %v", g.cfg.Name, target, err)
		g.markDown(idx)
		g.setCurrentIdx(g.nextHealthy(idx))
		time.Sleep(g.cfg.RetryBackoff)
	}
	return nil
}

func (g *FailoverGroup) getCurrentIdx() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.currentIdx
}

func (g *FailoverGroup) setCurrentIdx(idx int) {
	g.mu.Lock()
	g.currentIdx = idx
	g.mu.Unlock()
}

func (g *FailoverGroup) markDown(idx int) {
	g.mu.Lock()
	g.downUntil[idx] = time.Now().Add(g.cfg.RecoveryProbeInterval)
	g.mu.Unlock()
}

func (g *FailoverGroup) nextHealthy(idx int) int {
	g.mu.Lock()
	defer g.mu.Unlock()
	n := len(g.cfg.Targets)
	now := time.Now()
	for i := 1; i <= n; i++ {
		cand := (idx + i) % n
		if g.downUntil[cand].Before(now) {
			return cand
		}
	}
	return (idx + 1) % n
}

func (g *FailoverGroup) recoveryProber(ctx context.Context) {
	ticker := time.NewTicker(g.cfg.RecoveryProbeInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		cur := g.getCurrentIdx()
		if cur == 0 {
			continue
		}
		for idx := 0; idx < cur; idx++ {
			if g.pingHealthy(ctx, g.cfg.Targets[idx]) {
				g.mu.Lock()
				g.downUntil[idx] = time.Time{}
				g.currentIdx = idx
				if g.activeCancel != nil {
					g.activeCancel()
				}
				g.mu.Unlock()
				log.Printf("[failover:%s] promoting recovered backend %s (priority %d)", g.cfg.Name, g.cfg.Targets[idx], idx)
				break
			}
		}
	}
}

func (g *FailoverGroup) pingHealthy(ctx context.Context, target string) bool {
	dialCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	creds, err := security.ClientTransportCredentials(g.cfg.TLS)
	if err != nil {
		return false
	}
	opts := []grpc.DialOption{grpc.WithTransportCredentials(creds), grpc.WithBlock()}
	if g.cfg.AuthToken != "" {
		requireTLS := g.cfg.TLS != nil && g.cfg.TLS.Enabled
		opts = append(opts, grpc.WithPerRPCCredentials(security.TokenPerRPCCredentials{
			Token: g.cfg.AuthToken, RequireTLS: requireTLS,
		}))
	}
	conn, err := grpc.DialContext(dialCtx, target, opts...)
	if err != nil {
		return false
	}
	defer conn.Close()
	client := logflowpb.NewCollectorClient(conn)
	pingCtx, pingCancel := context.WithTimeout(ctx, 3*time.Second)
	defer pingCancel()
	resp, err := client.Ping(pingCtx, &logflowpb.PingRequest{})
	return err == nil && resp.GetOk()
}
