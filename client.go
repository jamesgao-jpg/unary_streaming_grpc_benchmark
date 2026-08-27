// This file defines the parent-side client and its unary and streaming RPC operations.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// verifyPayload checks that a synthetic full-transfer response contains the expected byte.
func verifyPayload(payload []byte, expected byte) error {
	for i, value := range payload {
		if value != expected {
			return fmt.Errorf("payload byte %d is %d, expected %d", i, value, expected)
		}
	}
	return nil
}

// benchmarkClient targets one child through the shared gRPC ClientConn.
type benchmarkClient struct {
	connection   *grpc.ClientConn
	childIndex   int
	expected     byte
	address      string
	transport    *connectionTracker
	grpcReceives *grpcReceiveTracker
}

// unary receives one complete child payload and optionally verifies its contents.
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

// unaryPayload invokes the child unary RPC and records time to response.
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

// streaming receives every Chunk from one child stream and aggregates transfer counters.
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

// openStreaming creates one child stream and sends its initial request message.
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

// receiveStreamingChunk receives one BytesValue Chunk from a child stream.
func receiveStreamingChunk(stream grpc.ClientStream) ([]byte, error) {
	response := &wrapperspb.BytesValue{}
	if err := stream.RecvMsg(response); err != nil {
		return nil, err
	}
	return response.Value, nil
}

// getSendCounters reads one child's application and transport counter snapshot.
func (c *benchmarkClient) getSendCounters(ctx context.Context) (sendCounterSnapshot, error) {
	response := &structpb.Struct{}
	if err := c.connection.Invoke(withChildIndex(ctx, c.childIndex), getStatsMethod, &emptypb.Empty{}, response); err != nil {
		return sendCounterSnapshot{}, err
	}
	return sendCounterSnapshotFromProtobuf(response)
}

// resetSendCounters starts a fresh measurement window on one child.
func (c *benchmarkClient) resetSendCounters(ctx context.Context) error {
	return c.connection.Invoke(
		withChildIndex(ctx, c.childIndex),
		resetStatsMethod,
		&emptypb.Empty{},
		&emptypb.Empty{},
	)
}

// waitForChildSendCounters waits until a child has no active benchmark RPCs.
func waitForChildSendCounters(parent context.Context, timeoutMS int, client *benchmarkClient) (sendCounterSnapshot, error) {
	ctx, cancel := context.WithTimeout(parent, time.Duration(timeoutMS)*time.Millisecond)
	defer cancel()
	for {
		counters, err := client.getSendCounters(ctx)
		if err != nil {
			return sendCounterSnapshot{}, err
		}
		if counters.activeRPCs == 0 {
			return counters, nil
		}
		select {
		case <-ctx.Done():
			return sendCounterSnapshot{}, ctx.Err()
		case <-time.After(time.Millisecond):
		}
	}
}

// resetChildSendCounters resets every child after all prior RPCs have stopped.
func resetChildSendCounters(parent context.Context, timeoutMS int, clients []*benchmarkClient) error {
	for _, client := range clients {
		for {
			ctx, cancel := context.WithTimeout(parent, time.Duration(timeoutMS)*time.Millisecond)
			_, err := waitForChildSendCounters(ctx, timeoutMS, client)
			if err == nil {
				err = client.resetSendCounters(ctx)
			}
			cancel()
			if err == nil {
				break
			}
			if status.Code(err) != codes.FailedPrecondition {
				return fmt.Errorf("child %d: %w", client.childIndex, err)
			}
		}
	}
	return nil
}

// collectChildSendCounters returns one stable post-workload snapshot per child.
func collectChildSendCounters(parent context.Context, timeoutMS int, clients []*benchmarkClient) ([]sendCounterSnapshot, error) {
	counters := make([]sendCounterSnapshot, len(clients))
	for _, client := range clients {
		childCounters, err := waitForChildSendCounters(parent, timeoutMS, client)
		if err != nil {
			return nil, fmt.Errorf("child %d: %w", client.childIndex, err)
		}
		counters[client.childIndex] = childCounters
	}
	return counters, nil
}
