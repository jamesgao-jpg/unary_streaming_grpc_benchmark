# Handoff: Plain Query Streaming Benchmark

## Current State

- Tracking issue: https://github.com/jamesgao-jpg/unary_streaming_grpc_benchmark/issues/8
- Experiment plan commit: `b8da141c221d2268e8a20bef7515f2597105e9a5`
- Milvus branch: `codex/qv-reducestream-e2e-benchmark-20260915`
- Milvus commit: `9d3dd3019283df4cfe77a180e4433666f7faaa98`
- Milvus server: `ubuntu@10.15.9.42:/home/ubuntu/milvus-qv`
- Client: `ubuntu@10.15.2.233:/home/ubuntu/reducestream_perf`
- Successful run: `runs/20260916T125931Z-97545/`

## Verified Result

The N1 bounded Plain Query experiment passed correctness for 8,192 rows and
25,231,360 logical field bytes. Streaming/Batch QPS was `0.9834`; Streaming
p95 was `1.0303` times Batch p95. Median Proxy RSS was 1,312.48 MiB for
Streaming and 1,453.47 MiB for Batch, but whole-process RSS is secondary and
does not establish retained-memory scaling.

This is an N1 transport-overhead baseline. It neither proves nor disproves the
fan-in memory claim because one child leaves no redundant child result to stop.

## Next Experiment

Create a new `milvus_result/test_<MMDD>_<NN>/` with `TEST_PLAN.md` and
`run_test.sh`. Reuse this request and measurement procedure while sweeping
effective QueryNode children per vchannel through N1, N2, N4, N8, and N16 when
placement qualifies. Keep Query limit 8,192, PK plus 768d vector output,
Chunk size 1,024 Units, concurrency 1, and four balanced repetitions fixed.
Record observed WorkNode identities before accepting each topology.
