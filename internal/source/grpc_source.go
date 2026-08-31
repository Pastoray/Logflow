package source

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
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

type GRPCSource struct {
	logflowpb.UnimplementedCollectorServer

	Name         string
	Router       router.Putter
	Channels     []channel.Channel
	Interceptors interceptor.Chain
	AgentID      string
	Metrics      *metrics.Registry
	TLS          *config.TLSConfig
	AuthToken    string

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
	if s.AuthToken != "" {
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
	if s.AuthToken != "" {
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

func (s *GRPCSource) putBatch(ctx context.Context, batch *logflowpb.EventBatch) (int64, error) {
	var n int64
	for _, pe := range batch.GetEvents() {
		body := pe.GetBody()
		if batch.GetBodyCompressed() {
			raw, err := util.GunzipBytes(body)
			if err != nil {
				return n, status.Errorf(codes.InvalidArgument, "decompress event: %v", err)
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
				n++
				continue
			}
		}
		if err := s.Router.Put(ctx, e); err != nil {
			return n, status.Errorf(codes.ResourceExhausted, "channel put: %v", err)
		}
		n++
		if s.Metrics != nil {
			s.Metrics.Counter("logflow_events_received_total").Inc()
		}
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
