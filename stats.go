// This file defines parent-side receive metrics, workflow reports, and console output.
package main

import (
	"context"
	"fmt"
	"math"
	"sort"
	"sync"
	"time"

	grpcstats "google.golang.org/grpc/stats"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// percentile returns the nearest-rank duration for a requested fraction.
func percentile(values []time.Duration, fraction float64) time.Duration {
	if len(values) == 0 {
		return 0
	}
	sorted := append([]time.Duration(nil), values...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	index := int(math.Ceil(fraction*float64(len(sorted)))) - 1
	return sorted[max(0, min(index, len(sorted)-1))]
}

// printReport emits latency, application, gRPC, transport, and Buffer metrics.
func printReport(report workflowReport, cfg benchmarkConfig, potentialBytesPerOperation int) {
	// Print high-level throughput and latency before detailed byte accounting.
	seconds := report.duration.Seconds()
	qps := float64(report.successes) / seconds
	throughputMiB := float64(report.bytes) / (1024 * 1024) / seconds
	firstP50 := "-"
	if len(report.firstResponses) > 0 {
		firstP50 = percentile(report.firstResponses, 0.50).String()
	}
	fmt.Printf("%-10s %8d %8d %10.2f %12.2f %12s %12s %12s %14s\n",
		report.mode,
		report.successes,
		report.errors,
		qps,
		throughputMiB,
		percentile(report.latencies, 0.50),
		percentile(report.latencies, 0.95),
		percentile(report.latencies, 0.99),
		firstP50,
	)
	if report.successes == 0 {
		return
	}

	// Report transfer savings against every child sending its complete result.
	potentialBytes := int64(potentialBytesPerOperation) * int64(report.successes)
	savedPercent := 100 * (1 - float64(report.bytes)/float64(potentialBytes))
	fmt.Printf("transfer mode=%s potential_bytes_per_operation=%d received_bytes_per_operation=%d protobuf_bytes_per_operation=%d response_messages_per_operation=%.2f received_units_per_operation=%.2f emitted_units_per_operation=%.2f unused_units_per_operation=%.2f saved_percent=%.2f\n",
		report.mode,
		potentialBytesPerOperation,
		report.bytes/int64(report.successes),
		report.protobufBytes/int64(report.successes),
		float64(report.responseMessages)/float64(report.successes),
		float64(report.receivedUnits)/float64(report.successes),
		float64(report.emittedUnits)/float64(report.successes),
		float64(report.receivedUnits-report.emittedUnits)/float64(report.successes),
		savedPercent,
	)

	// Expose each child's progress across application, gRPC, connection, and TCP boundaries.
	for childIndex, counters := range report.perChild {
		fmt.Printf("child_transfer mode=%s child=%d operations=%d received_messages=%d received_bytes=%d send_attempted_messages=%d send_attempted_bytes=%d send_completed_messages=%d send_completed_bytes=%d\n",
			report.mode,
			childIndex,
			report.successes,
			counters.receivedMessages,
			counters.receivedBytes,
			counters.send.attemptedMessages,
			counters.send.attemptedBytes,
			counters.send.completedMessages,
			counters.send.completedBytes,
		)
		fmt.Printf("grpc_receive mode=%s child=%d messages=%d application_payload_bytes=%d protobuf_bytes=%d grpc_wire_bytes=%d\n",
			report.mode,
			childIndex,
			counters.grpcReceive.messages,
			counters.grpcReceive.applicationPayloadBytes,
			counters.grpcReceive.protobufBytes,
			counters.grpcReceive.grpcWireBytes,
		)
		// net.Conn counters include gRPC, HTTP/2 framing, and connection-control traffic.
		// TCP_INFO separates bytes submitted to TCP from bytes acknowledged by the peer.
		fmt.Printf("transport mode=%s child=%d child_tcp_info_available=%t parent_tcp_info_available=%t child_connection_read_bytes=%d child_connection_write_bytes=%d child_tcp_received_bytes=%d child_tcp_sent_bytes=%d child_tcp_acked_bytes=%d child_tcp_not_sent_bytes=%d parent_connection_read_bytes=%d parent_connection_write_bytes=%d parent_tcp_received_bytes=%d parent_tcp_sent_bytes=%d parent_tcp_acked_bytes=%d parent_tcp_not_sent_bytes=%d\n",
			report.mode,
			childIndex,
			counters.send.transport.tcpInfoAvailable,
			counters.parentTransport.tcpInfoAvailable,
			counters.send.transport.connectionReadBytes,
			counters.send.transport.connectionWriteBytes,
			counters.send.transport.tcpBytesReceived,
			counters.send.transport.tcpBytesSent,
			counters.send.transport.tcpBytesAcked,
			counters.send.transport.tcpNotSentBytes,
			counters.parentTransport.connectionReadBytes,
			counters.parentTransport.connectionWriteBytes,
			counters.parentTransport.tcpBytesReceived,
			counters.parentTransport.tcpBytesSent,
			counters.parentTransport.tcpBytesAcked,
			counters.parentTransport.tcpNotSentBytes,
		)
	}

	// Quantify application-visible data left unused when ordered topK completes.
	consumedBytes := report.bytes
	if cfg.Workflow == orderedTopKWorkflow {
		consumedBytes = report.emittedUnits * int64(cfg.PerUnitBytes)
	}
	// This is data returned by Recv but still unused when topK is reached. The
	// total is summed across requests; it is not a point-in-time memory reading
	// and excludes unread bytes retained below the application in gRPC or TCP.
	retainedBytes := max(int64(0), report.bytes-consumedBytes)
	fmt.Printf("application_retention mode=%s received_payload_bytes=%d consumed_payload_bytes=%d unconsumed_payload_bytes_at_topk_total=%d unconsumed_payload_bytes_per_operation=%d\n",
		report.mode,
		report.bytes,
		consumedBytes,
		retainedBytes,
		retainedBytes/int64(report.successes),
	)
}

// childWorkflowCounters joins all measurement layers for one child.
type childWorkflowCounters struct {
	receivedMessages int64
	receivedBytes    int64
	send             sendCounterSnapshot
	grpcReceive      grpcReceiveCounterSnapshot
	parentTransport  transportCounterSnapshot
}

// workflowReport aggregates successful operations from one measured transport mode.
type workflowReport struct {
	mode             transferMode
	duration         time.Duration
	successes        int
	errors           int
	bytes            int64
	protobufBytes    int64
	responseMessages int64
	receivedUnits    int64
	emittedUnits     int64
	latencies        []time.Duration
	firstResponses   []time.Duration
	perChild         []childWorkflowCounters
}

// grpcReceiveCounterSnapshot records parent-side gRPC response delivery for one child.
type grpcReceiveCounterSnapshot struct {
	messages                uint64
	applicationPayloadBytes uint64
	protobufBytes           uint64
	grpcWireBytes           uint64
}

// grpcReceiveTracker safely accumulates per-child parent receive counters.
type grpcReceiveTracker struct {
	mu       sync.Mutex
	perChild []grpcReceiveCounterSnapshot
}

// newGRPCReceiveTracker allocates one receive counter slot per child.
func newGRPCReceiveTracker(children int) *grpcReceiveTracker {
	return &grpcReceiveTracker{perChild: make([]grpcReceiveCounterSnapshot, children)}
}

// reset clears all parent receive counters before a measurement window.
func (t *grpcReceiveTracker) reset() {
	t.mu.Lock()
	clear(t.perChild)
	t.mu.Unlock()
}

// snapshot returns a stable copy of all parent receive counters.
func (t *grpcReceiveTracker) snapshot() []grpcReceiveCounterSnapshot {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]grpcReceiveCounterSnapshot(nil), t.perChild...)
}

// parentRPCStatsContext identifies the child and method associated with one RPC.
type parentRPCStatsContext struct {
	childIndex int
	method     string
}

// parentRPCStatsContextKey stores parentRPCStatsContext in a gRPC stats context.
type parentRPCStatsContextKey struct{}

// parentStatsHandler records payloads delivered by gRPC to the parent process.
type parentStatsHandler struct {
	receives *grpcReceiveTracker
}

// TagRPC preserves the target child index and method for receive attribution.
func (h parentStatsHandler) TagRPC(ctx context.Context, info *grpcstats.RPCTagInfo) context.Context {
	childIndex := -1
	if taggedChildIndex, ok := ctx.Value(childContextIndexKey{}).(int); ok {
		childIndex = taggedChildIndex
	}
	return context.WithValue(ctx, parentRPCStatsContextKey{}, parentRPCStatsContext{
		childIndex: childIndex,
		method:     info.FullMethodName,
	})
}

// HandleRPC records incoming benchmark response sizes at the gRPC boundary.
func (h parentStatsHandler) HandleRPC(ctx context.Context, event grpcstats.RPCStats) {
	rpc, _ := ctx.Value(parentRPCStatsContextKey{}).(parentRPCStatsContext)
	if rpc.method != unaryMethod && rpc.method != streamingMethod {
		return
	}
	payload, ok := event.(*grpcstats.InPayload)
	if !ok || rpc.childIndex < 0 || rpc.childIndex >= len(h.receives.perChild) {
		return
	}
	response, ok := payload.Payload.(*wrapperspb.BytesValue)
	if !ok {
		return
	}
	h.receives.mu.Lock()
	counter := &h.receives.perChild[rpc.childIndex]
	counter.messages++
	counter.applicationPayloadBytes += uint64(len(response.Value))
	// Length is serialized protobuf bytes; WireLength adds gRPC framing but not HTTP/2 framing.
	counter.protobufBytes += uint64(payload.Length)
	counter.grpcWireBytes += uint64(payload.WireLength)
	h.receives.mu.Unlock()
}

// TagConn leaves parent connection contexts unchanged.
func (parentStatsHandler) TagConn(ctx context.Context, _ *grpcstats.ConnTagInfo) context.Context {
	return ctx
}

// HandleConn is intentionally empty because net.Conn wrappers collect transport counters.
func (parentStatsHandler) HandleConn(context.Context, grpcstats.ConnStats) {}
