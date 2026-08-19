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
	ctx = withChildIndex(ctx, c.childIndex)
	response := &wrapperspb.BytesValue{}
	if err := c.connection.Invoke(ctx, unaryMethod, &emptypb.Empty{}, response); err != nil {
		return transferResult{}, err
	}
	if verify {
		if err := verifyPayload(response.Value, c.expected); err != nil {
			return transferResult{}, err
		}
	}
	return transferResult{bytes: len(response.Value), firstResponse: time.Since(started)}, nil
}

var clientStreamingDescription = grpc.StreamDesc{
	StreamName:    "Streaming",
	ServerStreams: true,
	ClientStreams: true,
}

func (c *benchmarkClient) streaming(ctx context.Context, verify bool, started time.Time) (transferResult, error) {
	ctx = withChildIndex(ctx, c.childIndex)
	stream, err := c.connection.NewStream(ctx, &clientStreamingDescription, streamingMethod)
	if err != nil {
		return transferResult{}, err
	}
	if err := stream.SendMsg(&emptypb.Empty{}); err != nil {
		return transferResult{}, err
	}
	if err := stream.CloseSend(); err != nil {
		return transferResult{}, err
	}

	result := transferResult{}
	for {
		response := &wrapperspb.BytesValue{}
		err := stream.RecvMsg(response)
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
			if err := verifyPayload(response.Value, c.expected); err != nil {
				return transferResult{}, err
			}
		}
		result.bytes += len(response.Value)
	}
}
