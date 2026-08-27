// This file verifies child counter encoding, reset safety, and gRPC lifecycle accounting.
package main

import (
	"context"
	"testing"

	"google.golang.org/grpc/stats"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// TestSendCounterSnapshotProtobufRoundTrip verifies lossless in-band counter encoding.
func TestSendCounterSnapshotProtobufRoundTrip(t *testing.T) {
	expected := sendCounterSnapshot{
		attemptedMessages: 11,
		attemptedBytes:    22,
		completedMessages: 33,
		completedBytes:    44,
		activeRPCs:        55,
		transport: transportCounterSnapshot{
			connectionReadBytes:  66,
			connectionWriteBytes: 77,
			tcpBytesReceived:     88,
			tcpBytesSent:         99,
			tcpBytesAcked:        111,
			tcpNotSentBytes:      122,
			tcpInfoAvailable:     true,
		},
	}
	actual, err := sendCounterSnapshotFromProtobuf(expected.protobuf())
	if err != nil {
		t.Fatal(err)
	}
	if actual != expected {
		t.Fatalf("actual %+v, expected %+v", actual, expected)
	}
}

// TestSendCounterResetRequiresIdleTracker prevents measurement-window overlap.
func TestSendCounterResetRequiresIdleTracker(t *testing.T) {
	tracker := &sendCounterTracker{}
	tracker.recordAttempt(100)
	tracker.activeRPCs.Add(1)
	if tracker.reset() {
		t.Fatal("reset succeeded while an RPC was active")
	}
	tracker.activeRPCs.Add(-1)
	if !tracker.reset() {
		t.Fatal("reset failed after the tracker became idle")
	}
	if actual := tracker.snapshot(); actual != (sendCounterSnapshot{}) {
		t.Fatalf("reset snapshot %+v, expected all zeroes", actual)
	}
}

// TestBenchmarkStatsHandlerTracksUnaryLifecycle verifies active and completed unary counters.
func TestBenchmarkStatsHandlerTracksUnaryLifecycle(t *testing.T) {
	tracker := &sendCounterTracker{}
	handler := benchmarkStatsHandler{tracker: tracker}
	ctx := handler.TagRPC(context.Background(), &stats.RPCTagInfo{FullMethodName: unaryMethod})
	handler.HandleRPC(ctx, &stats.Begin{})

	service := transferService{payload: make([]byte, 128), tracker: tracker}
	response, err := service.Unary(context.Background(), &emptypb.Empty{})
	if err != nil {
		t.Fatal(err)
	}
	handler.HandleRPC(ctx, &stats.OutPayload{Payload: wrapperspb.Bytes(response.Value)})
	handler.HandleRPC(ctx, &stats.End{})

	expected := sendCounterSnapshot{
		attemptedMessages: 1,
		attemptedBytes:    128,
		completedMessages: 1,
		completedBytes:    128,
	}
	if actual := tracker.snapshot(); actual != expected {
		t.Fatalf("actual %+v, expected %+v", actual, expected)
	}
}
