package logflowpb

import (
	context "context"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

const _ = grpc.SupportPackageIsVersion8

const (
	Collector_StreamEvents_FullMethodName = "/logflow.v1.Collector/StreamEvents"
	Collector_SendBatch_FullMethodName    = "/logflow.v1.Collector/SendBatch"
	Collector_GetStats_FullMethodName     = "/logflow.v1.Collector/GetStats"
	Collector_Ping_FullMethodName         = "/logflow.v1.Collector/Ping"
)

type CollectorClient interface {
	StreamEvents(ctx context.Context, opts ...grpc.CallOption) (Collector_StreamEventsClient, error)

	SendBatch(ctx context.Context, in *EventBatch, opts ...grpc.CallOption) (*AckResponse, error)
	GetStats(ctx context.Context, in *StatsRequest, opts ...grpc.CallOption) (*StatsResponse, error)
	Ping(ctx context.Context, in *PingRequest, opts ...grpc.CallOption) (*PingResponse, error)
}

type collectorClient struct {
	cc grpc.ClientConnInterface
}

func NewCollectorClient(cc grpc.ClientConnInterface) CollectorClient {
	return &collectorClient{cc}
}

func (c *collectorClient) StreamEvents(ctx context.Context, opts ...grpc.CallOption) (Collector_StreamEventsClient, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	stream, err := c.cc.NewStream(ctx, &Collector_ServiceDesc.Streams[0], Collector_StreamEvents_FullMethodName, cOpts...)
	if err != nil {
		return nil, err
	}
	x := &collectorStreamEventsClient{ClientStream: stream}
	return x, nil
}

type Collector_StreamEventsClient interface {
	Send(*EventBatch) error
	Recv() (*AckResponse, error)
	grpc.ClientStream
}

type collectorStreamEventsClient struct {
	grpc.ClientStream
}

func (x *collectorStreamEventsClient) Send(m *EventBatch) error {
	return x.ClientStream.SendMsg(m)
}

func (x *collectorStreamEventsClient) Recv() (*AckResponse, error) {
	m := new(AckResponse)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func (c *collectorClient) SendBatch(ctx context.Context, in *EventBatch, opts ...grpc.CallOption) (*AckResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(AckResponse)
	err := c.cc.Invoke(ctx, Collector_SendBatch_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *collectorClient) GetStats(ctx context.Context, in *StatsRequest, opts ...grpc.CallOption) (*StatsResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(StatsResponse)
	err := c.cc.Invoke(ctx, Collector_GetStats_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *collectorClient) Ping(ctx context.Context, in *PingRequest, opts ...grpc.CallOption) (*PingResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(PingResponse)
	err := c.cc.Invoke(ctx, Collector_Ping_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

type CollectorServer interface {
	StreamEvents(Collector_StreamEventsServer) error

	SendBatch(context.Context, *EventBatch) (*AckResponse, error)
	GetStats(context.Context, *StatsRequest) (*StatsResponse, error)
	Ping(context.Context, *PingRequest) (*PingResponse, error)
	mustEmbedUnimplementedCollectorServer()
}

type UnimplementedCollectorServer struct {
}

func (UnimplementedCollectorServer) StreamEvents(Collector_StreamEventsServer) error {
	return status.Errorf(codes.Unimplemented, "method StreamEvents not implemented")
}

func (UnimplementedCollectorServer) SendBatch(context.Context, *EventBatch) (*AckResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method SendBatch not implemented")
}

func (UnimplementedCollectorServer) GetStats(context.Context, *StatsRequest) (*StatsResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetStats not implemented")
}

func (UnimplementedCollectorServer) Ping(context.Context, *PingRequest) (*PingResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Ping not implemented")
}
func (UnimplementedCollectorServer) mustEmbedUnimplementedCollectorServer() {}

type UnsafeCollectorServer interface {
	mustEmbedUnimplementedCollectorServer()
}

func RegisterCollectorServer(s grpc.ServiceRegistrar, srv CollectorServer) {
	s.RegisterService(&Collector_ServiceDesc, srv)
}

func _Collector_StreamEvents_Handler(srv interface{}, stream grpc.ServerStream) error {
	return srv.(CollectorServer).StreamEvents(&collectorStreamEventsServer{ServerStream: stream})
}

type Collector_StreamEventsServer interface {
	Send(*AckResponse) error
	Recv() (*EventBatch, error)
	grpc.ServerStream
}

type collectorStreamEventsServer struct {
	grpc.ServerStream
}

func (x *collectorStreamEventsServer) Send(m *AckResponse) error {
	return x.ServerStream.SendMsg(m)
}

func (x *collectorStreamEventsServer) Recv() (*EventBatch, error) {
	m := new(EventBatch)
	if err := x.ServerStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func _Collector_SendBatch_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(EventBatch)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(CollectorServer).SendBatch(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: Collector_SendBatch_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(CollectorServer).SendBatch(ctx, req.(*EventBatch))
	}
	return interceptor(ctx, in, info, handler)
}

func _Collector_GetStats_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(StatsRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(CollectorServer).GetStats(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: Collector_GetStats_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(CollectorServer).GetStats(ctx, req.(*StatsRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Collector_Ping_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(PingRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(CollectorServer).Ping(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: Collector_Ping_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(CollectorServer).Ping(ctx, req.(*PingRequest))
	}
	return interceptor(ctx, in, info, handler)
}

var Collector_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "logflow.v1.Collector",
	HandlerType: (*CollectorServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "SendBatch",
			Handler:    _Collector_SendBatch_Handler,
		},
		{
			MethodName: "GetStats",
			Handler:    _Collector_GetStats_Handler,
		},
		{
			MethodName: "Ping",
			Handler:    _Collector_Ping_Handler,
		},
	},
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "StreamEvents",
			Handler:       _Collector_StreamEvents_Handler,
			ServerStreams: true,
			ClientStreams: true,
		},
	},
	Metadata: "logflow.proto",
}
