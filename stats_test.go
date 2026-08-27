// This file verifies parent gRPC receive attribution and transport snapshot arithmetic.
package main

import (
	"context"
	"testing"

	grpcstats "google.golang.org/grpc/stats"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// TestParentStatsHandlerTracksApplicationAndGRPCBytes verifies all parent receive byte domains.
func TestParentStatsHandlerTracksApplicationAndGRPCBytes(t *testing.T) {
	receives := newGRPCReceiveTracker(2)
	handler := parentStatsHandler{receives: receives}
	ctx := withChildIndex(context.Background(), 1)
	ctx = handler.TagRPC(ctx, &grpcstats.RPCTagInfo{FullMethodName: streamingMethod})
	handler.HandleRPC(ctx, &grpcstats.InPayload{
		Payload:    wrapperspb.Bytes(make([]byte, 10)),
		Length:     12,
		WireLength: 17,
	})

	actual := receives.snapshot()[1]
	expected := grpcReceiveCounterSnapshot{
		messages:                1,
		applicationPayloadBytes: 10,
		protobufBytes:           12,
		grpcWireBytes:           17,
	}
	if actual != expected {
		t.Fatalf("actual %+v, expected %+v", actual, expected)
	}
}

// TestParentStatsHandlerDoesNotAttributeUntaggedRPCToChildZero rejects ambiguous attribution.
func TestParentStatsHandlerDoesNotAttributeUntaggedRPCToChildZero(t *testing.T) {
	receives := newGRPCReceiveTracker(1)
	handler := parentStatsHandler{receives: receives}
	ctx := handler.TagRPC(context.Background(), &grpcstats.RPCTagInfo{FullMethodName: streamingMethod})
	handler.HandleRPC(ctx, &grpcstats.InPayload{
		Payload:    wrapperspb.Bytes(make([]byte, 10)),
		Length:     12,
		WireLength: 17,
	})

	if actual := receives.snapshot()[0]; actual != (grpcReceiveCounterSnapshot{}) {
		t.Fatalf("untagged RPC was attributed to child zero: %+v", actual)
	}
}

// TestTransportCounterSinceKeepsNotSentAsGauge distinguishes deltas from TCP_NOTSENT state.
func TestTransportCounterSinceKeepsNotSentAsGauge(t *testing.T) {
	start := transportCounterSnapshot{
		connectionReadBytes:  10,
		connectionWriteBytes: 20,
		tcpBytesReceived:     30,
		tcpBytesSent:         40,
		tcpBytesAcked:        50,
		tcpNotSentBytes:      60,
		tcpInfoAvailable:     true,
	}
	end := transportCounterSnapshot{
		connectionReadBytes:  11,
		connectionWriteBytes: 22,
		tcpBytesReceived:     33,
		tcpBytesSent:         44,
		tcpBytesAcked:        55,
		tcpNotSentBytes:      66,
		tcpInfoAvailable:     true,
	}
	expected := transportCounterSnapshot{
		connectionReadBytes:  1,
		connectionWriteBytes: 2,
		tcpBytesReceived:     3,
		tcpBytesSent:         4,
		tcpBytesAcked:        5,
		tcpNotSentBytes:      66,
		tcpInfoAvailable:     true,
	}
	if actual := end.since(start); actual != expected {
		t.Fatalf("actual %+v, expected %+v", actual, expected)
	}
}
