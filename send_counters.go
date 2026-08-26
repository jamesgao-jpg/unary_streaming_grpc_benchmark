package main

import (
	"context"
	"fmt"
	"strconv"
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

type sendCounterSnapshot struct {
	attemptedMessages uint64
	attemptedBytes    uint64
	completedMessages uint64
	completedBytes    uint64
	activeRPCs        uint64
}

type sendCounterTracker struct {
	attemptedMessages atomic.Uint64
	attemptedBytes    atomic.Uint64
	completedMessages atomic.Uint64
	completedBytes    atomic.Uint64
	activeRPCs        atomic.Int64
}

func (t *sendCounterTracker) recordAttempt(payloadBytes int) {
	t.attemptedMessages.Add(1)
	t.attemptedBytes.Add(uint64(payloadBytes))
}

func (t *sendCounterTracker) recordCompletion(payloadBytes int) {
	t.completedMessages.Add(1)
	t.completedBytes.Add(uint64(payloadBytes))
}

func (t *sendCounterTracker) snapshot() sendCounterSnapshot {
	return sendCounterSnapshot{
		attemptedMessages: t.attemptedMessages.Load(),
		attemptedBytes:    t.attemptedBytes.Load(),
		completedMessages: t.completedMessages.Load(),
		completedBytes:    t.completedBytes.Load(),
		activeRPCs:        uint64(t.activeRPCs.Load()),
	}
}

func (t *sendCounterTracker) reset() bool {
	if t.activeRPCs.Load() != 0 {
		return false
	}
	t.attemptedMessages.Store(0)
	t.attemptedBytes.Store(0)
	t.completedMessages.Store(0)
	t.completedBytes.Store(0)
	return true
}

func (s sendCounterSnapshot) protobuf() *structpb.Struct {
	return &structpb.Struct{Fields: map[string]*structpb.Value{
		attemptedMessagesField: structpb.NewStringValue(strconv.FormatUint(s.attemptedMessages, 10)),
		attemptedBytesField:    structpb.NewStringValue(strconv.FormatUint(s.attemptedBytes, 10)),
		completedMessagesField: structpb.NewStringValue(strconv.FormatUint(s.completedMessages, 10)),
		completedBytesField:    structpb.NewStringValue(strconv.FormatUint(s.completedBytes, 10)),
		activeRPCsField:        structpb.NewStringValue(strconv.FormatUint(s.activeRPCs, 10)),
	}}
}

func sendCounterSnapshotFromProtobuf(message *structpb.Struct) (sendCounterSnapshot, error) {
	if message == nil {
		return sendCounterSnapshot{}, fmt.Errorf("send counter response is nil")
	}
	values := make([]uint64, 5)
	for i, name := range []string{
		attemptedMessagesField,
		attemptedBytesField,
		completedMessagesField,
		completedBytesField,
		activeRPCsField,
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
	return sendCounterSnapshot{
		attemptedMessages: values[0],
		attemptedBytes:    values[1],
		completedMessages: values[2],
		completedBytes:    values[3],
		activeRPCs:        values[4],
	}, nil
}

type rpcMethodContextKey struct{}

type benchmarkStatsHandler struct {
	tracker *sendCounterTracker
}

func (h benchmarkStatsHandler) TagRPC(ctx context.Context, info *stats.RPCTagInfo) context.Context {
	return context.WithValue(ctx, rpcMethodContextKey{}, info.FullMethodName)
}

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

func (benchmarkStatsHandler) TagConn(ctx context.Context, _ *stats.ConnTagInfo) context.Context {
	return ctx
}

func (benchmarkStatsHandler) HandleConn(context.Context, stats.ConnStats) {}
