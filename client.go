package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func verifyPayload(payload []byte, expected byte) error {
	for i, value := range payload {
		if value != expected {
			return fmt.Errorf("payload byte %d is %d, expected %d", i, value, expected)
		}
	}
	return nil
}

type benchmarkClient struct {
	connection *grpc.ClientConn
	childIndex int
	expected   byte
}

func (c *benchmarkClient) unary(ctx context.Context, verify bool, started time.Time) (transferResult, error) {
	payload, firstResponse, err := c.unaryPayload(ctx, started)
	if err != nil {
		return transferResult{}, err
	}
	if verify {
		if err := verifyPayload(payload, c.expected); err != nil {
			return transferResult{}, err
		}
	}
	return transferResult{
		bytes:            len(payload),
		protobufBytes:    protobufBytesValueSize(len(payload)),
		responseMessages: 1,
		firstResponse:    firstResponse,
	}, nil
}

func (c *benchmarkClient) unaryPayload(ctx context.Context, started time.Time) ([]byte, time.Duration, error) {
	ctx = withChildIndex(ctx, c.childIndex)
	response := &wrapperspb.BytesValue{}
	if err := c.connection.Invoke(ctx, unaryMethod, &emptypb.Empty{}, response); err != nil {
		return nil, 0, err
	}
	return response.Value, time.Since(started), nil
}

var clientStreamingDescription = grpc.StreamDesc{
	StreamName:    "Streaming",
	ServerStreams: true,
	ClientStreams: true,
}

func (c *benchmarkClient) streaming(ctx context.Context, verify bool, started time.Time) (transferResult, error) {
	stream, err := c.openStreaming(ctx)
	if err != nil {
		return transferResult{}, err
	}

	result := transferResult{}
	for {
		payload, err := receiveStreamingChunk(stream)
		if errors.Is(err, io.EOF) {
			return result, nil
		}
		if err != nil {
			return transferResult{}, err
		}
		if result.firstResponse == 0 {
			result.firstResponse = time.Since(started)
		}
		if verify {
			if err := verifyPayload(payload, c.expected); err != nil {
				return transferResult{}, err
			}
		}
		result.bytes += len(payload)
		result.protobufBytes += protobufBytesValueSize(len(payload))
		result.responseMessages++
	}
}

func (c *benchmarkClient) openStreaming(ctx context.Context) (grpc.ClientStream, error) {
	ctx = withChildIndex(ctx, c.childIndex)
	stream, err := c.connection.NewStream(ctx, &clientStreamingDescription, streamingMethod)
	if err != nil {
		return nil, err
	}
	if err := stream.SendMsg(&emptypb.Empty{}); err != nil {
		return nil, err
	}
	if err := stream.CloseSend(); err != nil {
		return nil, err
	}
	return stream, nil
}

func receiveStreamingChunk(stream grpc.ClientStream) ([]byte, error) {
	response := &wrapperspb.BytesValue{}
	if err := stream.RecvMsg(response); err != nil {
		return nil, err
	}
	return response.Value, nil
}
