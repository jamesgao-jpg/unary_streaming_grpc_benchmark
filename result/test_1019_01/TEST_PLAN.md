# Persistent Stream Versus Unary Request-Response Benchmark

Status: Proposed. The workload and repository placement are agreed; the new
workflow and runner have not been implemented or executed.

Date: 2026-10-19 (Asia/Shanghai).

## Objective

Compare repeated Unary RPCs with repeated request-response exchanges inside
one persistent bidirectional RPC. Both modes execute the same number of
logical operations, with identical request and response message types and
payloads. Measure whether keeping the RPC open improves QPS and latency.

This is a separate workload in the existing benchmark repository. It evaluates
reuse across requests, rather than reduction within a request. It contains no
ReduceStream, topK selection, early cancellation, or payload splitting.

The result does not establish how Milvus should implement per-request
cancellation, deadlines, errors, or recovery inside a shared stream.

## Topology and Environment

```text
10.15.9.42
  Benchmark parent process
    -> one shared gRPC channel
      -> child server process 0
      -> ...
      -> child server process N-1
```

Run the parent and children on .42 over loopback TCP, using
`/home/ubuntu/unary_streaming_grpc_benchmark`. No separate client host is used.
Reuse the existing child launcher, resolver, routing, connection management,
and gRPC settings. Establish connections before timing either mode.

The source inspected for this proposal is
`e8cf23f29c1615c45581cc1f761a0d4ffe30ce2b`; its `go.mod` pins gRPC-Go v1.80.0.
Record the actual implementation commit, working-tree diff, Go version,
configuration, and machine resources for the run. Use the same binary for
both modes. Keep compression and TLS disabled and static windows at zero,
as configured in the existing `config.yaml`.

## Operation Contract

An operation sends one request to every child and completes after all N
responses have been received. Every child returns P payload bytes in exactly
one protobuf response message. Use the existing `Empty` request and
`BytesValue` response types and the same deterministic per-child payload.

| Property | Unary | Persistent streaming |
| --- | --- | --- |
| One child exchange | Invoke one Unary RPC and receive its response | Send one request message and receive one response message |
| Response payload | P bytes | P bytes |
| Next operation | Invoke another Unary RPC | Reuse the same open RPC |
| Connection reuse | Enabled | Enabled |
| Outstanding work per worker and child | One request | One request |

Each of K workers owns one persistent stream to each child. A worker waits
for all N responses before starting its next operation. There is no
pipelining, unsolicited server push, or multiplexing of simultaneous requests
inside one stream. Use the same worker scheduling and fan-in procedure in
both modes.

The persistent server repeats `Recv(request)` followed by `Send(response)`.
It does not send another response until another request arrives. The client
keeps its send side open until all warm-up and measured exchanges finish.

For R measured operations in either mode:

| Quantity | Unary | Persistent streaming |
| --- | ---: | ---: |
| Response messages | R * N | R * N |
| Response payload bytes | R * N * P | R * N * P |
| RPCs carrying measured exchanges | R * N | K * N |
| RPCs started inside the measurement interval | R * N | 0 |

Persistent RPC creation occurs before timing. Report its setup and shutdown
duration separately. The stream lifetime is one benchmark phase, not the
server process lifetime.

## Initial Case

| Case | Children N | Concurrent operations K | Payload P per child and operation | Response messages per child and operation |
| --- | ---: | ---: | ---: | ---: |
| N1-P4K-K1 | 1 | 1 | 4 KiB | 1 |

Run this case first. Payload, fan-in, and concurrency sweeps will be specified
after reviewing the initial result. The existing `stream_chunk_bytes` value
does not split responses in this workload; the entire P-byte response is one
message.

## Measurement Procedure

1. Before formal measurement, run focused correctness tests, the existing Go
   tests with race detection, and build the benchmark on .42. Verify that
   existing `full_transfer` and `ordered_topk` workloads still execute.
2. Start the children and establish the shared channel. Verify payload size,
   contents, and response counts in both modes before measurement.
3. Calibrate R using an unreported three-second pilot per mode. Choose the
   same R for both modes, at least 10,000 operations, targeting 30 seconds
   using the faster pilot QPS. Round R up to a multiple of K and record it.
4. Run four paired repetitions with that fixed R. Alternate Unary-first and
   persistent-streaming-first so each order occurs twice.
5. Before each mode, warm every worker-child path for 100 exchanges. For
   persistent streaming, retain those same streams for measurement.
6. With workers idle after warm-up, capture counter baselines. Time exactly R
   completed operations. Start each operation's latency timer before its
   requests are issued and stop it after its final response is received.
7. Capture ending payload and transport counters while workers are idle,
   before stream shutdown. Compute deltas from the post-warm-up baselines.
8. Outside the timed interval, close the persistent send sides and receive
   normal EOF. Verify that all benchmark RPCs have stopped and release the
   connections and child processes. Any RPC error invalidates the repetition;
   do not reconnect or silently replace a persistent stream during timing.
9. Each timed interval must last at least 20 seconds. If calibration was too
   short, preserve the evidence, increase R, and repeat all four pairs with
   the new fixed R. Do not combine repetitions using different R values.

The existing counter reset and collection helpers wait for zero active RPCs.
They cannot be called unchanged while persistent streams remain open. Reset
only before opening streams, and use non-blocking counter snapshots and
deltas across the measured exchanges. Wait for zero active RPCs after closing
the streams. Capture connection counters before reporting RPCs add control
traffic, and document any unavoidable reporting traffic in those counters.

## Metrics and Interpretation

Primary metrics are completed-operation QPS, p50/p95/p99 operation latency,
and persistent-streaming/Unary ratios for each paired repetition. Report all
four repetitions, the combined medians, and results separated by mode order.

Validate per-child response message and payload counts, RPC creation counts,
payload contents, errors, and successful cleanup. Report setup and shutdown
duration separately from the measured operation latency.

Retain existing gRPC payload and connection byte diagnostics where available.
Label their scope explicitly: gRPC payload measurements and connection
read/write measurements have different coverage. Neither is a retained-memory
measurement. Do not add control RPCs between individual timed operations.

Unary operation completion includes successful RPC completion. A persistent
operation completes when its response message arrives; final RPC status is
checked when the stream closes. This difference is part of the comparison.
The result measures the combined effect of repeated RPC lifecycle work versus
repeated message exchanges, not stream creation alone. Loopback results do
not establish performance on a physical network or in Milvus.

## Acceptance Criteria and Artifacts

A repetition passes when both modes complete exactly R operations without
errors, response counts and bytes match the formulas above, verification
passes, persistent streams are not recreated during measurement, and shutdown
completes successfully. A valid experiment may show either mode faster or no
consistent difference; a streaming speedup is not an acceptance requirement.

Add an executable `run.sh` before execution. Store generated configurations,
pilot and measured logs, source and environment metadata, manifests, and
checksums under timestamped `logs/<UTC-run-id>/` directories. Preserve all
failed and superseded runs. Write verified findings to `report.md` and update
this plan's status after analysis. Keep this experiment separate from
`result/test_0921_01` and from `milvus_result/`.
