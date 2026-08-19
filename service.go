package main

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

const (
	unaryMethod     = "/benchmark.BenchmarkService/Unary"
	streamingMethod = "/benchmark.BenchmarkService/Streaming"
)

type benchmarkServiceServer interface {
	Unary(context.Context, *emptypb.Empty) (*wrapperspb.BytesValue, error)
	Streaming(grpc.ServerStream) error
}

type transferService struct {
	payload    []byte
	chunkBytes int
}

func (s *transferService) Unary(context.Context, *emptypb.Empty) (*wrapperspb.BytesValue, error) {
	return wrapperspb.Bytes(s.payload), nil
}

func (s *transferService) Streaming(stream grpc.ServerStream) error {
	if err := stream.RecvMsg(&emptypb.Empty{}); err != nil {
		return err
	}
	for offset := 0; offset < len(s.payload); offset += s.chunkBytes {
		end := min(offset+s.chunkBytes, len(s.payload))
		if err := stream.SendMsg(wrapperspb.Bytes(s.payload[offset:end])); err != nil {
			return err
		}
	}
	return nil
}

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

func streamingHandler(srv any, stream grpc.ServerStream) error {
	return srv.(benchmarkServiceServer).Streaming(stream)
}

var serviceDescription = grpc.ServiceDesc{
	ServiceName: "benchmark.BenchmarkService",
	HandlerType: (*benchmarkServiceServer)(nil),
	Methods: []grpc.MethodDesc{{
		MethodName: "Unary",
		Handler:    unaryHandler,
	}},
	Streams: []grpc.StreamDesc{{
		StreamName:    "Streaming",
		Handler:       streamingHandler,
		ServerStreams: true,
		ClientStreams: true,
	}},
	Metadata: "benchmark.proto",
}
