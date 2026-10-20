package sink

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/encoding/gzip" // registers "gzip" compressor
	"google.golang.org/grpc/keepalive"

	"logflow/gen/logflowpb"
	"logflow/internal/channel"
	"logflow/internal/config"
	"logflow/internal/metrics"
	"logflow/internal/security"
	"logflow/internal/util"
)

type GRPCSinkConfig struct {
	Name            string
	TargetAddr      string
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

// GRPCSink drains a channel, batches events, and forwards them to a
// downstream agent's Collector service over a bidirectional stream. Several
// worker goroutines each own their own stream so a slow/blocked stream on
// one worker doesn't stall the others (parallelism), and each worker keeps
// several batches "in flight" - sent but not yet acked - to overlap network
// latency with local batching (pipelining), which is what makes this
// meaningfully faster than a naive send-wait-send-wait loop.
type GRPCSink struct {
	cfg     GRPCSinkConfig
	channel channel.Channel
	metrics *metrics.Registry
}

func NewGRPCSink(cfg GRPCSinkConfig, ch channel.Channel, reg *metrics.Registry) *GRPCSink {
	return &GRPCSink{cfg: cfg, channel: ch, metrics: reg}
}

func (s *GRPCSink) Run(ctx context.Context) error {
	var wg sync.WaitGroup
	for i := 0; i < s.cfg.Parallelism; i++ {
		wg.Add(1)
		id := i
		go func() {
			defer wg.Done()
			s.workerLoop(ctx, id)
		}()
	}
	wg.Wait()
	return nil
}

func (s *GRPCSink) workerLoop(ctx context.Context, workerID int) {
	backoff := s.cfg.RetryBackoff
	for {
		if ctx.Err() != nil {
			return
		}
		conn, err := s.dial(ctx)
		if err != nil {
			log.Printf("[sink:%s#%d] dial %s failed: %v (retrying)", s.cfg.Name, workerID, s.cfg.TargetAddr, err)
			select {
			case <-time.After(backoff):
			case <-ctx.Done():
				return
			}
			continue
		}
		if err := s.streamLoop(ctx, conn, workerID); err != nil {
			log.Printf("[sink:%s#%d] stream ended: %v (reconnecting)", s.cfg.Name, workerID, err)
		}
		conn.Close()
		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			return
		}
	}
}

func (s *GRPCSink) dial(ctx context.Context) (*grpc.ClientConn, error) {
	creds, err := security.ClientTransportCredentials(s.cfg.TLS)
	if err != nil {
		return nil, fmt.Errorf("tls: %w", err)
	}
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(creds),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                20 * time.Second,
			Timeout:             10 * time.Second,
			PermitWithoutStream: true,
		}),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(32<<20),
			grpc.MaxCallSendMsgSize(32<<20),
		),
	}
	if s.cfg.Compression {
		opts = append(opts, grpc.WithDefaultCallOptions(grpc.UseCompressor(gzip.Name)))
	}
	if s.cfg.AuthToken != "" {
		requireTLS := s.cfg.TLS != nil && s.cfg.TLS.Enabled
		opts = append(opts, grpc.WithPerRPCCredentials(security.TokenPerRPCCredentials{
			Token: s.cfg.AuthToken, RequireTLS: requireTLS,
		}))
	}
	dialCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	return grpc.DialContext(dialCtx, s.cfg.TargetAddr, opts...)
}

type inFlightBatch struct {
	events []*channel.Event
	sentAt time.Time
}

// DialAndStreamOnce dials the target once and runs a single stream to
// completion (until ctx is cancelled or the stream errors), with none of
// the workerLoop's own reconnect/backoff. It's used directly by GRPCSink's
// self-contained retry loop (Run) and also reused by FailoverGroup, which
// wants to control reconnect/backoff/target-switching itself.
func (s *GRPCSink) DialAndStreamOnce(ctx context.Context) error {
	conn, err := s.dial(ctx)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()
	return s.streamLoop(ctx, conn, 0)
}

func (s *GRPCSink) streamLoop(ctx context.Context, conn *grpc.ClientConn, workerID int) error {
	client := logflowpb.NewCollectorClient(conn)
	stream, err := client.StreamEvents(ctx)
	if err != nil {
		return fmt.Errorf("open stream: %w", err)
	}

	sem := make(chan struct{}, s.cfg.InFlightBatches)
	var mu sync.Mutex
	pending := make(map[string]*inFlightBatch)

	ackErrCh := make(chan error, 1)
	go func() {
		for {
			ack, err := stream.Recv()
			if err != nil {
				ackErrCh <- err
				return
			}
			mu.Lock()
			ib, ok := pending[ack.GetBatchId()]
			if ok {
				delete(pending, ack.GetBatchId())
			}
			mu.Unlock()
			<-sem // free a pipeline slot
			if !ok {
				continue
			}
			if s.metrics != nil {
				// Round-trip latency from this batch's send to its ack
				// (success or failure) - the one piece channel/sink
				// counters alone can't tell you: whether forwarding is
				// fast, or merely not dropping anything while slow.
				s.metrics.Histogram("logflow_batch_send_duration_seconds").Observe(time.Since(ib.sentAt).Seconds())
			}
			if ack.GetSuccess() {
				if s.metrics != nil {
					s.metrics.Counter("logflow_events_forwarded_total").Add(float64(len(ib.events)))
				}
			} else {
				log.Printf("[sink:%s#%d] batch %s nacked: %s", s.cfg.Name, workerID, ack.GetBatchId(), ack.GetError())
				if s.metrics != nil {
					s.metrics.Counter("logflow_events_forward_failed_total").Add(float64(len(ib.events)))
				}
			}
		}
	}()

	batchNum := 0
	for {
		select {
		case <-ctx.Done():
			stream.CloseSend()
			return ctx.Err()
		case err := <-ackErrCh:
			return err
		default:
		}

		events := drainBatch(ctx, s.channel, s.cfg.BatchSize, s.cfg.BatchLinger)
		if len(events) == 0 {
			continue
		}
		batchNum++
		batchID := fmt.Sprintf("%s-%d-%d", s.cfg.Name, workerID, batchNum)

		pbEvents := make([]*logflowpb.Event, len(events))
		for i, e := range events {
			pbEvents[i] = util.ToPBEvent(e)
		}
		batch := &logflowpb.EventBatch{BatchId: batchID, Events: pbEvents}

		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			return ctx.Err()
		case err := <-ackErrCh:
			return err
		}

		mu.Lock()
		pending[batchID] = &inFlightBatch{events: events, sentAt: time.Now()}
		mu.Unlock()

		if err := s.sendWithRetry(stream, batch); err != nil {
			mu.Lock()
			delete(pending, batchID)
			mu.Unlock()
			<-sem
			s.requeueOrDrop(events)
			return fmt.Errorf("send batch: %w", err)
		}
	}
}

// requeueOrDrop makes a best-effort attempt to put events that could not be
// forwarded (e.g. the connection died mid-send) back onto the channel so a
// future stream attempt can retry them, rather than silently losing data.
// If the channel is already full, the event is counted as dropped.
func (s *GRPCSink) requeueOrDrop(events []*channel.Event) {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	for _, e := range events {
		if err := s.channel.Put(ctx, e); err != nil {
			if s.metrics != nil {
				s.metrics.Counter("logflow_events_dropped_total").Inc()
			}
		}
	}
}

func (s *GRPCSink) sendWithRetry(stream logflowpb.Collector_StreamEventsClient, batch *logflowpb.EventBatch) error {
	var err error
	backoff := s.cfg.RetryBackoff
	for attempt := 0; attempt <= s.cfg.MaxRetries; attempt++ {
		if err = stream.Send(batch); err == nil {
			return nil
		}
		if attempt < s.cfg.MaxRetries {
			time.Sleep(backoff)
		}
	}
	return err
}
