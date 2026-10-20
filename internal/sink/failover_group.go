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
	Targets         []string // priority order; index 0 is primary
	BatchSize       int
	BatchLinger     time.Duration
	InFlightBatches int
	MaxRetries      int
	RetryBackoff    time.Duration
	Compression     bool
	TLS             *config.TLSConfig
	AuthToken       string

	// How often to probe targets with higher priority than the one
	// currently active, to move traffic back once they recover.
	RecoveryProbeInterval time.Duration

	// Used only by "file:<dir>" targets - see FileTargetPrefix.
	FileRollMaxBytes int64
	FileRollInterval time.Duration
}

// FailoverGroup is Flume's "failover sink processor": exactly one backend
// is active at a time, chosen by priority (Targets[0] first). On failure
// the active backend is marked down and traffic moves to the next
// available one; a background prober periodically checks whether a
// higher-priority backend has come back, and promotes it immediately by
// cancelling the current stream so the drain loop reconnects to it - unlike
// LoadBalanceGroup, only one backend ever receives traffic at a time.
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

		log.Printf("[failover:%s] active backend -> %s (priority %d)", g.cfg.Name, target, idx)
		var err error
		if IsFileTarget(target) {
			fs := NewFileSink(FileSinkConfig{
				Name: g.cfg.Name, Dir: FileTargetDir(target),
				BatchSize: g.cfg.BatchSize, BatchLinger: g.cfg.BatchLinger,
				RollMaxBytes: g.cfg.FileRollMaxBytes, RollInterval: g.cfg.FileRollInterval,
			}, g.channel, g.metrics)
			err = fs.RunUntilError(subCtx)
			if err == nil && subCtx.Err() != nil {
				// Stopped by cancellation (shutdown or a promotion), not a
				// write failure - report it the way DialAndStreamOnce does
				// so the loop below tells the two apart.
				err = subCtx.Err()
			}
		} else {
			sub := NewGRPCSink(GRPCSinkConfig{
				Name: g.cfg.Name + "/" + target, TargetAddr: target,
				BatchSize: g.cfg.BatchSize, BatchLinger: g.cfg.BatchLinger,
				Parallelism: 1, InFlightBatches: g.cfg.InFlightBatches,
				MaxRetries: g.cfg.MaxRetries, RetryBackoff: g.cfg.RetryBackoff,
				Compression: g.cfg.Compression,
				TLS:         g.cfg.TLS, AuthToken: g.cfg.AuthToken,
			}, g.channel, g.metrics)
			err = sub.DialAndStreamOnce(subCtx)
		}
		cancel()

		if ctx.Err() != nil {
			return nil
		}
		if errors.Is(err, context.Canceled) {
			// A promotion cancelled us deliberately; loop immediately and
			// pick up the new currentIdx with no backoff or down-marking.
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

// nextHealthy returns the next target after idx (wrapping) that isn't
// currently in its down cooldown, or just idx+1 mod N if every target is
// down - we still have to try something.
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

// recoveryProber periodically pings every backend with strictly higher
// priority than the currently active one; if any responds, that target is
// promoted (made current) and the active stream is cancelled so the drain
// loop reconnects to it right away instead of waiting for the current one
// to fail on its own.
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
			continue // already on the top-priority target
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
	if IsFileTarget(target) {
		return fileTargetHealthy(FileTargetDir(target))
	}
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
