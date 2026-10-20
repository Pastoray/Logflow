// Package source contains Source implementations that admit events into the
// pipeline: a gRPC-based source (used both for external producers and for
// agent-to-agent forwarding) and a file-tailing source.
package source

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"

	"logflow/gen/logflowpb"
	"logflow/internal/channel"
	"logflow/internal/config"
	"logflow/internal/interceptor"
	"logflow/internal/metrics"
	"logflow/internal/router"
	"logflow/internal/security"
	"logflow/internal/util"
)

// GRPCSource implements logflowpb.CollectorServer. It accepts events either
// as a unary SendBatch call or, for higher throughput, as a bidirectional
// StreamEvents call where a client pipelines many batches without waiting
// for each ack individually. Every event runs through the interceptor
// chain and then the channel router before being acked, so a caller's ack
// reflects events that have actually been durably enqueued (or dropped by
// an interceptor, which still counts as "received").
type GRPCSource struct {
	logflowpb.UnimplementedCollectorServer

	Name         string
	Router       router.Putter
	Channels     []channel.Channel // every channel this source can reach, for stats reporting
	Interceptors interceptor.Chain
	AgentID      string
	Metrics      *metrics.Registry
	TLS          *config.TLSConfig
	AuthToken    string
	// TokenStore, if set, authenticates per-caller tokens and takes the
	// place of AuthToken.
	TokenStore *security.TokenStore

	startedAt time.Time
	server    *grpc.Server
}

func NewGRPCSource(name, agentID string, rt router.Putter, channels []channel.Channel, chain interceptor.Chain, reg *metrics.Registry, tlsCfg *config.TLSConfig, authToken string) *GRPCSource {
	return &GRPCSource{
		Name: name, Router: rt, Channels: channels, Interceptors: chain,
		AgentID: agentID, Metrics: reg, TLS: tlsCfg, AuthToken: authToken,
		startedAt: time.Now(),
	}
}

// Serve starts a dedicated gRPC server on addr for this source and blocks
// until the listener errors or ctx is cancelled.
func (s *GRPCSource) Serve(ctx context.Context, addr string) error {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("source %s listen: %w", s.Name, err)
	}

	creds, err := security.ServerTransportCredentials(s.TLS)
	if err != nil {
		return fmt.Errorf("source %s tls: %w", s.Name, err)
	}

	opts := []grpc.ServerOption{
		grpc.Creds(creds),
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time:    30 * time.Second,
			Timeout: 10 * time.Second,
		}),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             10 * time.Second,
			PermitWithoutStream: true,
		}),
		grpc.MaxRecvMsgSize(32 << 20),
		grpc.MaxSendMsgSize(32 << 20),
	}
	if s.TokenStore != nil {
		opts = append(opts,
			grpc.ChainUnaryInterceptor(security.UnaryServerAuthStore(s.TokenStore)),
			grpc.ChainStreamInterceptor(security.StreamServerAuthStore(s.TokenStore)),
		)
	} else if s.AuthToken != "" {
		opts = append(opts,
			grpc.ChainUnaryInterceptor(security.UnaryServerAuth(s.AuthToken)),
			grpc.ChainStreamInterceptor(security.StreamServerAuth(s.AuthToken)),
		)
	}

	srv := grpc.NewServer(opts...)
	s.server = srv
	logflowpb.RegisterCollectorServer(srv, s)

	go func() {
		<-ctx.Done()
		srv.GracefulStop()
	}()

	secDesc := ""
	if s.TLS != nil && s.TLS.Enabled {
		secDesc = " tls"
		if s.TLS.ClientAuth {
			secDesc += "+mtls"
		}
	}
	if s.TokenStore != nil {
		secDesc += " per-caller-token-auth"
	} else if s.AuthToken != "" {
		secDesc += " token-auth"
	}
	log.Printf("[source:%s] gRPC listening on %s%s", s.Name, addr, orNone(secDesc))
	return srv.Serve(lis)
}

func orNone(s string) string {
	if s == "" {
		return " (insecure)"
	}
	return " (" + s[1:] + ")"
}

// putBatch decodes and interceptor-processes every event in batch, then
// calls s.Router.Put for each one concurrently rather than one at a time.
//
// This matters beyond raw parallelism: a clustered channel
// (cluster.ReplicatedChannel) coalesces concurrently-pending Put() calls
// into a single Raft log entry (see internal/cluster/channel.go's
// batching), but only calls that are actually in flight at the same time
// can coalesce. Put()ing events from one batch sequentially - waiting for
// each one's Put to return before starting the next - means at most one
// event is ever pending at once, so each would pay out its own full
// BatchLinger wait individually instead of joining a shared batch; that
// made the first version of this method the accidental bottleneck
// upstream of clustering's own batching, not something clustering could
// fix on its own. Putting the whole batch's events concurrently restores
// the "many callers waiting on Put() at once" shape batching depends on
// (see internal/cluster's channel_test.go, which batches this same way:
// many goroutines calling Put() concurrently).
func (s *GRPCSource) putBatch(ctx context.Context, batch *logflowpb.EventBatch) (int64, error) {
	events := batch.GetEvents()
	errs := make([]error, len(events))

	var wg sync.WaitGroup
	for i, pe := range events {
		wg.Add(1)
		go func(i int, pe *logflowpb.Event) {
			defer wg.Done()
			body := pe.GetBody()
			if batch.GetBodyCompressed() {
				raw, err := util.GunzipBytes(body)
				if err != nil {
					errs[i] = status.Errorf(codes.InvalidArgument, "decompress event: %v", err)
					return
				}
				body = raw
			}
			e := &channel.Event{
				Body:      body,
				Headers:   pe.GetHeaders(),
				Timestamp: time.Unix(0, pe.GetTimestampUnixNano()),
			}
			if s.Interceptors != nil {
				e = s.Interceptors.Intercept(e)
				if e == nil {
					return // dropped by an interceptor, but still counts as received/acked
				}
			}
			if err := s.Router.Put(ctx, e); err != nil {
				errs[i] = status.Errorf(codes.ResourceExhausted, "channel put: %v", err)
				return
			}
			if s.Metrics != nil {
				s.Metrics.Counter("logflow_events_received_total").Inc()
			}
		}(i, pe)
	}
	wg.Wait()

	// Preserve putBatch's original contract: n is how many events were
	// actually received (processed, successfully or as an interceptor
	// drop) before the first error, and a non-nil error is the first one
	// encountered by index order - callers (SendBatch/StreamEvents) use n
	// to tell a caller how much of its batch landed.
	var n int64
	for i := range events {
		if errs[i] != nil {
			return n, errs[i]
		}
		n++
	}
	return n, nil
}

func (s *GRPCSource) SendBatch(ctx context.Context, batch *logflowpb.EventBatch) (*logflowpb.AckResponse, error) {
	n, err := s.putBatch(ctx, batch)
	if err != nil {
		return &logflowpb.AckResponse{BatchId: batch.GetBatchId(), Success: false, Error: err.Error(), EventsReceived: n}, nil
	}
	return &logflowpb.AckResponse{BatchId: batch.GetBatchId(), Success: true, EventsReceived: n}, nil
}

// StreamEvents lets a well-behaved client (typically a GRPCSink in a
// downstream agent) keep many batches "in flight" on one HTTP/2 connection:
// it doesn't have to wait for an ack before sending the next batch, and this
// handler acks each batch as soon as it has been fully enqueued.
func (s *GRPCSource) StreamEvents(stream logflowpb.Collector_StreamEventsServer) error {
	ctx := stream.Context()
	for {
		batch, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		n, perr := s.putBatch(ctx, batch)
		ack := &logflowpb.AckResponse{BatchId: batch.GetBatchId(), EventsReceived: n}
		if perr != nil {
			ack.Success = false
			ack.Error = perr.Error()
		} else {
			ack.Success = true
		}
		if err := stream.Send(ack); err != nil {
			return err
		}
	}
}

func (s *GRPCSource) GetStats(ctx context.Context, _ *logflowpb.StatsRequest) (*logflowpb.StatsResponse, error) {
	chStats := make(map[string]*logflowpb.ChannelStats, len(s.Channels))
	for _, ch := range s.Channels {
		st := ch.Stats()
		chStats[ch.Name()] = &logflowpb.ChannelStats{
			Capacity:     st.Capacity,
			Size:         st.Size,
			PutCount:     st.PutCount,
			TakeCount:    st.TakeCount,
			DroppedCount: st.DroppedCount,
		}
	}
	return &logflowpb.StatsResponse{
		AgentId:       s.AgentID,
		UptimeSeconds: int64(time.Since(s.startedAt).Seconds()),
		Channels:      chStats,
	}, nil
}

func (s *GRPCSource) Ping(ctx context.Context, req *logflowpb.PingRequest) (*logflowpb.PingResponse, error) {
	return &logflowpb.PingResponse{Ok: true, AgentId: s.AgentID, TimestampUnixNano: time.Now().UnixNano()}, nil
}
