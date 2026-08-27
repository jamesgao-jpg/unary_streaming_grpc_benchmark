// This file defines child-side application send counters and gRPC lifecycle tracking.
package main

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"

	"google.golang.org/grpc/stats"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

const (
	attemptedMessagesField = "attempted_messages"
	attemptedBytesField    = "attempted_bytes"
	completedMessagesField = "completed_messages"
	completedBytesField    = "completed_bytes"
	activeRPCsField        = "active_rpcs"
)

// sendCounterSnapshot is one stable child measurement-window result.
type sendCounterSnapshot struct {
	attemptedMessages uint64
	attemptedBytes    uint64
	completedMessages uint64
	completedBytes    uint64
	activeRPCs        uint64
	transport         transportCounterSnapshot
}

// sendCounterTracker records child send progress and its transport baseline.
type sendCounterTracker struct {
	attemptedMessages atomic.Uint64
	attemptedBytes    atomic.Uint64
	completedMessages atomic.Uint64
	completedBytes    atomic.Uint64
	activeRPCs        atomic.Int64
	transport         *connectionTracker
	transportMu       sync.Mutex
	transportBaseline transportCounterSnapshot
}

// recordAttempt records application payload submitted to a response send.
func (t *sendCounterTracker) recordAttempt(payloadBytes int) {
	t.attemptedMessages.Add(1)
	t.attemptedBytes.Add(uint64(payloadBytes))
}

// recordCompletion records application payload whose response send returned successfully.
func (t *sendCounterTracker) recordCompletion(payloadBytes int) {
	t.completedMessages.Add(1)
	t.completedBytes.Add(uint64(payloadBytes))
}

// snapshot returns current application counters and transport deltas.
func (t *sendCounterTracker) snapshot() sendCounterSnapshot {
	t.transportMu.Lock()
	transport := transportCounterSnapshot{}
	if t.transport != nil {
		transport = t.transport.snapshotAll().since(t.transportBaseline)
	}
	t.transportMu.Unlock()
	return sendCounterSnapshot{
		attemptedMessages: t.attemptedMessages.Load(),
		attemptedBytes:    t.attemptedBytes.Load(),
		completedMessages: t.completedMessages.Load(),
		completedBytes:    t.completedBytes.Load(),
		activeRPCs:        uint64(t.activeRPCs.Load()),
		transport:         transport,
	}
}

// reset clears counters and captures a new transport baseline while idle.
func (t *sendCounterTracker) reset() bool {
	if t.activeRPCs.Load() != 0 {
		return false
	}
	t.attemptedMessages.Store(0)
	t.attemptedBytes.Store(0)
	t.completedMessages.Store(0)
	t.completedBytes.Store(0)
	t.transportMu.Lock()
	if t.transport != nil {
		t.transportBaseline = t.transport.snapshotAll()
	}
	t.transportMu.Unlock()
	return true
}

// protobuf encodes a snapshot for the in-band GetStats RPC.
func (s sendCounterSnapshot) protobuf() *structpb.Struct {
	return &structpb.Struct{Fields: map[string]*structpb.Value{
		attemptedMessagesField:   structpb.NewStringValue(strconv.FormatUint(s.attemptedMessages, 10)),
		attemptedBytesField:      structpb.NewStringValue(strconv.FormatUint(s.attemptedBytes, 10)),
		completedMessagesField:   structpb.NewStringValue(strconv.FormatUint(s.completedMessages, 10)),
		completedBytesField:      structpb.NewStringValue(strconv.FormatUint(s.completedBytes, 10)),
		activeRPCsField:          structpb.NewStringValue(strconv.FormatUint(s.activeRPCs, 10)),
		"connection_read_bytes":  structpb.NewStringValue(strconv.FormatUint(s.transport.connectionReadBytes, 10)),
		"connection_write_bytes": structpb.NewStringValue(strconv.FormatUint(s.transport.connectionWriteBytes, 10)),
		"tcp_bytes_received":     structpb.NewStringValue(strconv.FormatUint(s.transport.tcpBytesReceived, 10)),
		"tcp_bytes_sent":         structpb.NewStringValue(strconv.FormatUint(s.transport.tcpBytesSent, 10)),
		"tcp_bytes_acked":        structpb.NewStringValue(strconv.FormatUint(s.transport.tcpBytesAcked, 10)),
		"tcp_not_sent_bytes":     structpb.NewStringValue(strconv.FormatUint(s.transport.tcpNotSentBytes, 10)),
		"tcp_info_available":     structpb.NewBoolValue(s.transport.tcpInfoAvailable),
	}}
}

// sendCounterSnapshotFromProtobuf decodes and validates a GetStats response.
func sendCounterSnapshotFromProtobuf(message *structpb.Struct) (sendCounterSnapshot, error) {
	if message == nil {
		return sendCounterSnapshot{}, fmt.Errorf("send counter response is nil")
	}
	values := make([]uint64, 11)
	for i, name := range []string{
		attemptedMessagesField,
		attemptedBytesField,
		completedMessagesField,
		completedBytesField,
		activeRPCsField,
		"connection_read_bytes",
		"connection_write_bytes",
		"tcp_bytes_received",
		"tcp_bytes_sent",
		"tcp_bytes_acked",
		"tcp_not_sent_bytes",
	} {
		field, ok := message.Fields[name]
		if !ok {
			return sendCounterSnapshot{}, fmt.Errorf("send counter response is missing %s", name)
		}
		value, err := strconv.ParseUint(field.GetStringValue(), 10, 64)
		if err != nil {
			return sendCounterSnapshot{}, fmt.Errorf("parse send counter %s: %w", name, err)
		}
		values[i] = value
	}
	tcpInfoAvailable, ok := message.Fields["tcp_info_available"]
	if !ok {
		return sendCounterSnapshot{}, errors.New("send counter response is missing tcp_info_available")
	}
	return sendCounterSnapshot{
		attemptedMessages: values[0],
		attemptedBytes:    values[1],
		completedMessages: values[2],
		completedBytes:    values[3],
		activeRPCs:        values[4],
		transport: transportCounterSnapshot{
			connectionReadBytes:  values[5],
			connectionWriteBytes: values[6],
			tcpBytesReceived:     values[7],
			tcpBytesSent:         values[8],
			tcpBytesAcked:        values[9],
			tcpNotSentBytes:      values[10],
			tcpInfoAvailable:     tcpInfoAvailable.GetBoolValue(),
		},
	}, nil
}

// rpcMethodContextKey stores the current gRPC method for lifecycle filtering.
type rpcMethodContextKey struct{}

// benchmarkStatsHandler records child benchmark RPC lifecycle events.
type benchmarkStatsHandler struct {
	tracker *sendCounterTracker
}

// TagRPC attaches the full method name to the stats context.
func (h benchmarkStatsHandler) TagRPC(ctx context.Context, info *stats.RPCTagInfo) context.Context {
	return context.WithValue(ctx, rpcMethodContextKey{}, info.FullMethodName)
}

// HandleRPC tracks active RPCs and unary response completion.
func (h benchmarkStatsHandler) HandleRPC(ctx context.Context, event stats.RPCStats) {
	method, _ := ctx.Value(rpcMethodContextKey{}).(string)
	if method != unaryMethod && method != streamingMethod {
		return
	}
	switch event := event.(type) {
	case *stats.Begin:
		h.tracker.activeRPCs.Add(1)
	case *stats.OutPayload:
		if method == unaryMethod {
			response, ok := event.Payload.(*wrapperspb.BytesValue)
			if ok {
				h.tracker.recordCompletion(len(response.Value))
			}
		}
	case *stats.End:
		h.tracker.activeRPCs.Add(-1)
	}
}

// TagConn leaves child connection contexts unchanged.
func (benchmarkStatsHandler) TagConn(ctx context.Context, _ *stats.ConnTagInfo) context.Context {
	return ctx
}

// HandleConn is intentionally empty because net.Conn wrappers collect transport counters.
func (benchmarkStatsHandler) HandleConn(context.Context, stats.ConnStats) {}
