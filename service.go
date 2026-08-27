// This file defines the benchmark gRPC service and its unary, streaming, and counter RPCs.
package main

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

const (
	unaryMethod      = "/benchmark.BenchmarkService/Unary"
	streamingMethod  = "/benchmark.BenchmarkService/Streaming"
	resetStatsMethod = "/benchmark.BenchmarkService/ResetStats"
	getStatsMethod   = "/benchmark.BenchmarkService/GetStats"
)

// benchmarkServiceServer lists the RPC methods implemented by each child process.
type benchmarkServiceServer interface {
	Unary(context.Context, *emptypb.Empty) (*wrapperspb.BytesValue, error)
	Streaming(grpc.ServerStream) error
	ResetStats(context.Context, *emptypb.Empty) (*emptypb.Empty, error)
	GetStats(context.Context, *emptypb.Empty) (*structpb.Struct, error)
}

// transferService serves one child's fixed payload and measurement counters.
type transferService struct {
	payload    []byte
	chunkBytes int
	tracker    *sendCounterTracker
}

// Unary returns the child's complete payload in one response message.
func (s *transferService) Unary(context.Context, *emptypb.Empty) (*wrapperspb.BytesValue, error) {
	s.tracker.recordAttempt(len(s.payload))
	return wrapperspb.Bytes(s.payload), nil
}

// Streaming sends the child's payload as sequential fixed-size Chunks.
func (s *transferService) Streaming(stream grpc.ServerStream) error {
	if err := stream.RecvMsg(&emptypb.Empty{}); err != nil {
		return err
	}
	for offset := 0; offset < len(s.payload); offset += s.chunkBytes {
		end := min(offset+s.chunkBytes, len(s.payload))
		s.tracker.recordAttempt(end - offset)
		if err := stream.SendMsg(wrapperspb.Bytes(s.payload[offset:end])); err != nil {
			return err
		}
		s.tracker.recordCompletion(end - offset)
	}
	return nil
}

// ResetStats starts a new child measurement window when no benchmark RPC is active.
func (s *transferService) ResetStats(context.Context, *emptypb.Empty) (*emptypb.Empty, error) {
	if !s.tracker.reset() {
		return nil, status.Error(codes.FailedPrecondition, "benchmark RPCs are still active")
	}
	return &emptypb.Empty{}, nil
}

// GetStats returns the child's application and transport counters.
func (s *transferService) GetStats(context.Context, *emptypb.Empty) (*structpb.Struct, error) {
	return s.tracker.snapshot().protobuf(), nil
}

// unaryHandler adapts the manually described unary RPC to transferService.
func unaryHandler(srv any, ctx context.Context, decode func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	request := &emptypb.Empty{}
	if err := decode(request); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(benchmarkServiceServer).Unary(ctx, request)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: unaryMethod}
	handler := func(ctx context.Context, request any) (any, error) {
		return srv.(benchmarkServiceServer).Unary(ctx, request.(*emptypb.Empty))
	}
	return interceptor(ctx, request, info, handler)
}

// streamingHandler adapts the manually described streaming RPC to transferService.
func streamingHandler(srv any, stream grpc.ServerStream) error {
	return srv.(benchmarkServiceServer).Streaming(stream)
}

// resetStatsHandler adapts the counter-reset RPC to transferService.
func resetStatsHandler(srv any, ctx context.Context, decode func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	request := &emptypb.Empty{}
	if err := decode(request); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(benchmarkServiceServer).ResetStats(ctx, request)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: resetStatsMethod}
	handler := func(ctx context.Context, request any) (any, error) {
		return srv.(benchmarkServiceServer).ResetStats(ctx, request.(*emptypb.Empty))
	}
	return interceptor(ctx, request, info, handler)
}

// getStatsHandler adapts the counter-snapshot RPC to transferService.
func getStatsHandler(srv any, ctx context.Context, decode func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	request := &emptypb.Empty{}
	if err := decode(request); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(benchmarkServiceServer).GetStats(ctx, request)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: getStatsMethod}
	handler := func(ctx context.Context, request any) (any, error) {
		return srv.(benchmarkServiceServer).GetStats(ctx, request.(*emptypb.Empty))
	}
	return interceptor(ctx, request, info, handler)
}

// serviceDescription registers the benchmark service without generated protobuf code.
var serviceDescription = grpc.ServiceDesc{
	ServiceName: "benchmark.BenchmarkService",
	HandlerType: (*benchmarkServiceServer)(nil),
	Methods: []grpc.MethodDesc{{
		MethodName: "Unary",
		Handler:    unaryHandler,
	}, {
		MethodName: "ResetStats",
		Handler:    resetStatsHandler,
	}, {
		MethodName: "GetStats",
		Handler:    getStatsHandler,
	}},
	Streams: []grpc.StreamDesc{{
		StreamName:    "Streaming",
		Handler:       streamingHandler,
		ServerStreams: true,
		ClientStreams: true,
	}},
	Metadata: "benchmark.proto",
}
