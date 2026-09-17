#!/usr/bin/env bash
# Runs the bounded Plain Query vector-payload fan-in Batch/Streaming sweep.

set -Eeuo pipefail

TEST_0917_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# Reuse the accepted cluster lifecycle, placement, sampling, and cleanup helpers.
source "$TEST_0917_DIR/../test_0915_02/run_test.sh"

SCRIPT_DIR="$TEST_0917_DIR"
TEST_REPO="$(cd "$SCRIPT_DIR/../.." && pwd)"
WORKSPACE_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
MILVUS_LOCAL_REPO="${MILVUS_LOCAL_REPO:-$WORKSPACE_ROOT/milvus-qv}"

RUN_ID="${RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)-$$}"
RUN_DIR="$SCRIPT_DIR/runs/$RUN_ID"
SERVER_RUN_DIR="/home/ubuntu/reducestream-e2e/$RUN_ID"
CLIENT_RUN_DIR="/home/ubuntu/reducestream-e2e/$RUN_ID"
COMPOSE_PROJECT="qv-query-fanin-$(printf '%s' "$RUN_ID" | tr '[:upper:]' '[:lower:]' | tr -cd '[:alnum:]-')"
COMPOSE_OVERRIDE="$SERVER_RUN_DIR/docker-compose.override.yml"
SERVER_ENV="$SERVER_RUN_DIR/milvus.env"
CLIENT_DRIVER="$CLIENT_RUN_DIR/query_driver.py"

REQUIRED_MILVUS_COMMIT="9d3dd3019283df4cfe77a180e4433666f7faaa98"
QUERY_LIMIT=8192
CHUNK_SIZE=1024
WARMUP_OPERATIONS=5
MIN_OPERATIONS=30
MIN_DURATION_SECONDS=60
REPETITIONS=4
TOPOLOGIES=(1 2 4 8 16)
COLLECTION_LOADED=false
PROXY_METRICS_PORT=$((METRICS_BASE + 4))

# log writes a test-specific progress message.
log() {
    printf '[test_0917_01] %s\n' "$*"
}

# start_role starts one native Milvus role with the Query Streaming controls.
start_role() {
    local name=$1 role=$2 metrics_port=$3 rpc_port=${4:-0}
    local log_level=${5:-info} query_streaming=${6:-unused}

    log "Starting $name, query streaming=$query_streaming"
    server_bash \
        "$name" "$role" "$metrics_port" "$rpc_port" "$log_level" \
        "$query_streaming" "$SERVER_RUN_DIR" "$SERVER_REPO" "$SERVER_ENV" <<'REMOTE'
set -Eeuo pipefail
name=$1
role=$2
metrics_port=$3
rpc_port=$4
log_level=$5
query_streaming=$6
run_dir=$7
repo=$8
env_file=$9

mkdir -p "$run_dir/logs" "$run_dir/pids" "$run_dir/local/$name"
(
    set -a
    source "$env_file"
    set +a
    export METRICS_PORT="$metrics_port"
    export LOCALSTORAGE_PATH="$run_dir/local/$name"
    export MILVUS_CONF_LOGLEVEL="$log_level"
    export MILVUS_CONF_LOGFORMAT=text
    if [[ "$role" == "querynode" ]]; then
        export MILVUS_CONF_QUERYNODE_PORT="$rpc_port"
    elif [[ "$role" == "streamingnode" ]]; then
        export MILVUS_CONF_STREAMINGNODE_PORT="$rpc_port"
    elif [[ "$role" == "proxy" ]]; then
        export MILVUS_CONF_PROXYQUERYVIEWENABLEQUERYSTREAMING="$query_streaming"
    fi
    export LD_LIBRARY_PATH="$repo/internal/core/output/lib:${LD_LIBRARY_PATH:-}"
    if [[ -f "$repo/internal/core/output/lib/libjemalloc.so" ]]; then
        export LD_PRELOAD="$repo/internal/core/output/lib/libjemalloc.so"
        export MALLOC_CONF="background_thread:true"
    fi
    cd "$repo"
    exec nohup setsid "$repo/bin/milvus" run "$role" --run-with-subprocess
) >"$run_dir/logs/$name.log" 2>&1 < /dev/null &
echo $! >"$run_dir/pids/$name.pid"
REMOTE
    wait_for_health "$name" "$metrics_port" 600
}

# start_proxy restarts Proxy in the requested Batch or Streaming mode.
start_proxy() {
    local mode=$1 log_level=${2:-info}
    local streaming=false
    [[ "$mode" == streaming ]] && streaming=true
    stop_pid_file "$SERVER_RUN_DIR/pids/proxy.pid"
    start_role proxy proxy "$PROXY_METRICS_PORT" 0 "$log_level" "$streaming"
    wait_for_tcp 10.15.9.42 "$MILVUS_PORT" 300
}

# write_server_configuration derives this run's fixed Query configuration.
write_server_configuration() {
    server "mkdir -p '$SERVER_RUN_DIR/logs' '$SERVER_RUN_DIR/pids' \
        '$SERVER_RUN_DIR/infra' '$SERVER_RUN_DIR/local'"
    server "cp '$SOURCE_SERVER_RUN_DIR/milvus.env' '$SERVER_ENV'; \
        sed -i \
          -e '/^MILVUS_CONF_PROXYQUERYVIEWENABLEQUERYSTREAMING=/d' \
          -e '/^MILVUS_CONF_PROXYQUERYVIEWQUERYSTREAMCHUNKSIZE=/d' \
          -e '/^MILVUS_CONF_QUERYCOORDQUERYVIEWTARGETROWSPERSHARDNODE=/d' \
          '$SERVER_ENV'; \
        printf '%s\n' \
          'MILVUS_CONF_PROXYQUERYVIEWQUERYSTREAMCHUNKSIZE=$CHUNK_SIZE' \
          'MILVUS_CONF_QUERYCOORDQUERYVIEWTARGETROWSPERSHARDNODE=$TARGET_ROWS_PER_SHARD_NODE' \
          >>'$SERVER_ENV'"

    ssh "${SSH_OPTIONS[@]}" "$SERVER_TARGET" "cat > '$COMPOSE_OVERRIDE'" <<EOF
services:
  minio:
    ports: !override
      - "127.0.0.1:${MINIO_PORT}:9000"
      - "127.0.0.1:${MINIO_CONSOLE_PORT}:9001"
EOF
    copy_from_server "$SERVER_ENV" "$RUN_DIR/milvus.env"
}

# install_client_driver installs the deterministic PyMilvus Query workload.
install_client_driver() {
    client "mkdir -p '$CLIENT_RUN_DIR'"
    ssh "${SSH_OPTIONS[@]}" "$CLIENT_TARGET" "cat > '$CLIENT_DRIVER'" <<'PY'
#!/usr/bin/env python3
# Executes and validates bounded Plain Query vector-payload requests.

import argparse
import hashlib
import json
import math
import resource
import struct
import time

from pymilvus import Collection, connections, utility


# connect waits for Proxy and establishes the process-wide PyMilvus connection.
def connect(args):
    deadline = time.monotonic() + 300
    while True:
        try:
            connections.connect(alias="default", host=args.host, port=args.port, timeout=10)
            utility.list_collections(timeout=10)
            return
        except Exception:
            connections.disconnect("default")
            if time.monotonic() >= deadline:
                raise
            time.sleep(2)


# write_json writes stable, reviewable experiment output.
def write_json(path, value):
    with open(path, "w", encoding="utf-8") as output:
        json.dump(value, output, indent=2, sort_keys=True)


# percentile returns the nearest-rank percentile from nanosecond samples.
def percentile(values, percentage):
    ordered = sorted(values)
    rank = max(0, math.ceil(percentage * len(ordered)) - 1)
    return ordered[rank]


# query_once executes one request and optionally hashes all returned field data.
def query_once(collection, args, include_hash=False, include_ids=False):
    rows = collection.query(
        expr="pk >= 0",
        output_fields=["pk", "vector"],
        limit=args.limit,
        consistency_level="Strong",
        timeout=600,
    )
    if len(rows) != args.limit:
        raise AssertionError(f"query returned {len(rows)} rows, expected {args.limit}")

    ids = [int(row["pk"]) for row in rows]
    vector_values = 0
    digest = hashlib.sha256() if include_hash else None
    for row in rows:
        vector = row["vector"]
        vector_values += len(vector)
        if len(vector) != 768:
            raise AssertionError(f"vector dimension is {len(vector)}, expected 768")
        if digest is not None:
            digest.update(struct.pack("<q", int(row["pk"])))
            for value in vector:
                digest.update(struct.pack("<f", float(value)))

    result = {
        "count": len(rows),
        "logicalFieldBytes": len(rows) * 8 + vector_values * 4,
    }
    if digest is not None:
        result["sha256"] = digest.hexdigest()
    if include_ids:
        result["ids"] = ids
    return result


# inspect verifies that the immutable collection has the expected schema and rows.
def inspect(args):
    connect(args)
    if not utility.has_collection(args.collection):
        raise RuntimeError(f"collection does not exist: {args.collection}")
    collection = Collection(args.collection)
    result = {
        "collection": args.collection,
        "entities": collection.num_entities,
        "schema": collection.schema.to_dict(),
        "indexes": [index.to_dict() for index in collection.indexes],
    }
    if result["entities"] != 1_000_000:
        raise AssertionError(f"collection has {result['entities']} entities, expected 1000000")
    write_json(args.output, result)


# release unloads the collection so segments redistribute over the new set.
def release(args):
    connect(args)
    Collection(args.collection).release(timeout=1800)
    deadline = time.monotonic() + 1800
    while utility.get_query_segment_info(args.collection, timeout=30):
        if time.monotonic() >= deadline:
            raise TimeoutError("segment placement remained after release")
        time.sleep(5)


# load loads the immutable collection with one replica.
def load(args):
    connect(args)
    collection = Collection(args.collection)
    collection.load(replica_number=1, timeout=7200)
    utility.wait_for_loading_complete(args.collection, timeout=7200)
    write_json(args.output, utility.loading_progress(args.collection))


# verify captures complete IDs and a deterministic ID/vector hash.
def verify(args):
    connect(args)
    collection = Collection(args.collection)
    started = time.monotonic_ns()
    result = query_once(collection, args, include_hash=True, include_ids=True)
    result.update({"mode": args.mode, "latencyNs": time.monotonic_ns() - started})
    write_json(args.output, result)


# benchmark measures repeated Query latency while validating every row count.
def benchmark(args):
    connect(args)
    collection = Collection(args.collection)
    for _ in range(args.warmup_operations):
        query_once(collection, args)

    latencies = []
    first_hash = None
    started = time.monotonic()
    while len(latencies) < args.min_operations or time.monotonic() - started < args.min_duration_seconds:
        operation_started = time.monotonic_ns()
        result = query_once(collection, args, include_hash=not latencies)
        latencies.append(time.monotonic_ns() - operation_started)
        if first_hash is None:
            first_hash = result["sha256"]
            logical_bytes = result["logicalFieldBytes"]

    elapsed = time.monotonic() - started
    write_json(args.output, {
        "case": args.case,
        "mode": args.mode,
        "repetition": args.repetition,
        "successfulOperations": len(latencies),
        "errors": 0,
        "elapsedSeconds": elapsed,
        "qps": len(latencies) / elapsed,
        "limit": args.limit,
        "logicalFieldBytesPerOperation": logical_bytes,
        "firstResultSha256": first_hash,
        "latencyNs": {
            "p50": percentile(latencies, 0.50),
            "p95": percentile(latencies, 0.95),
            "p99": percentile(latencies, 0.99),
            "max": max(latencies),
        },
        "clientMaxRssKiB": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
    })


# main parses one action and guarantees connection cleanup.
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=("inspect", "release", "load", "verify", "benchmark"))
    parser.add_argument("--host", required=True)
    parser.add_argument("--port", required=True)
    parser.add_argument("--collection", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--mode", default="batch")
    parser.add_argument("--case", default="FANIN-N1")
    parser.add_argument("--repetition", type=int, default=0)
    parser.add_argument("--limit", type=int, default=8192)
    parser.add_argument("--warmup-operations", type=int, default=5)
    parser.add_argument("--min-operations", type=int, default=30)
    parser.add_argument("--min-duration-seconds", type=int, default=60)
    args = parser.parse_args()
    try:
        globals()[args.action](args)
    finally:
        connections.disconnect("default")


if __name__ == "__main__":
    main()
PY
    client "chmod +x '$CLIENT_DRIVER'"
}

# record_proxy_environment preserves the active Query mode controls.
record_proxy_environment() {
    local output=$1
    server "pid=\$(cat '$SERVER_RUN_DIR/pids/proxy.pid'); \
        tr '\0' '\n' < /proc/\$pid/environ | \
        grep -E '^(MILVUS_CONF_PROXYQUERYVIEWENABLEQUERYSTREAMING|MILVUS_CONF_PROXYQUERYVIEWQUERYSTREAMCHUNKSIZE|METRICS_PORT=)' | sort" \
        >"$output"
}

# copy_query_logs preserves the active Proxy and all QueryNode logs.
copy_query_logs() {
    local output=$1
    mkdir -p "$output"
    copy_from_server "$SERVER_RUN_DIR/logs/proxy.log" "$output/proxy.log"
    local remote_log
    while IFS= read -r remote_log; do
        [[ -n "$remote_log" ]] || continue
        copy_from_server "$remote_log" "$output/$(basename "$remote_log")"
    done < <(server "ls '$SERVER_RUN_DIR'/logs/querynode_*.log 2>/dev/null | sort")
}

# capture_runtime stores Proxy metrics and memory profiles around one interval.
capture_runtime() {
    local remote_dir=$1 phase=$2
    server_bash "$remote_dir" "$phase" "$PROXY_METRICS_PORT" <<'REMOTE'
set -Eeuo pipefail
dir=$1
phase=$2
port=$3
mkdir -p "$dir"
curl --fail --silent "http://127.0.0.1:${port}/metrics" >"$dir/$phase.metrics"
curl --fail --silent "http://127.0.0.1:${port}/debug/pprof/heap" >"$dir/$phase.heap.pb.gz"
curl --fail --silent "http://127.0.0.1:${port}/debug/pprof/allocs" >"$dir/$phase.allocs.pb.gz"
curl --fail --silent "http://127.0.0.1:${port}/debug/pprof/goroutine?debug=1" \
    >"$dir/$phase.goroutines.txt"
REMOTE
}

# copy_runtime downloads one interval's runtime snapshots.
copy_runtime() {
    local remote_dir=$1 output=$2
    mkdir -p "$output"
    rsync -az -e "ssh -i '$SSH_KEY' -o BatchMode=yes" \
        "$SERVER_TARGET:$remote_dir/" "$output/"
}

# qualify_worknodes records the effective Query WorkNodes and validates them.
qualify_worknodes() {
    local case_dir=$1 remote_case_dir=$2 count=$3

    start_proxy streaming debug
    run_client_action "$(basename "$case_dir")-worknodes" \
        "verify --host 10.15.9.42 --port '$MILVUS_PORT' \
        --collection '$COLLECTION_NAME' --mode streaming --limit '$QUERY_LIMIT' \
        --output '$remote_case_dir/worknodes-probe.json'"
    copy_from_client "$remote_case_dir/worknodes-probe.json" "$case_dir/worknodes-probe.json"
    server "grep 'query view work nodes selected' '$SERVER_RUN_DIR/logs/proxy.log' | tail -n 1" \
        >"$case_dir/worknodes.log"
    [[ -s "$case_dir/worknodes.log" ]] || fail "WorkNode evidence is empty"
    validate_worknodes "$case_dir" "$count"
}

# run_correctness_gate compares complete Batch and Streaming Query results.
run_correctness_gate() {
    local case_name=$1 case_dir=$2 remote_case_dir=$3
    local mode

    for mode in batch streaming; do
        start_proxy "$mode" info
        run_client_action "$case_name-$mode-verify" \
            "verify --host 10.15.9.42 --port '$MILVUS_PORT' \
            --collection '$COLLECTION_NAME' --mode '$mode' --limit '$QUERY_LIMIT' \
            --output '$remote_case_dir/$mode-verify.json'"
        copy_from_client "$remote_case_dir/$mode-verify.json" "$case_dir/$mode-verify.json"
    done

    python3 - "$case_dir" <<'PY'
import json
import pathlib
import sys

root = pathlib.Path(sys.argv[1])
batch = json.load(open(root / "batch-verify.json", encoding="utf-8"))
streaming = json.load(open(root / "streaming-verify.json", encoding="utf-8"))
for name, value in (("batch", batch), ("streaming", streaming)):
    if value["count"] != 8192:
        raise SystemExit(f"{name} returned {value['count']} rows")
    if value["logicalFieldBytes"] != 8192 * (8 + 768 * 4):
        raise SystemExit(f"{name} logical field-byte count is unexpected")
if batch["ids"] != streaming["ids"]:
    raise SystemExit("Batch and Streaming ordered IDs differ")
if batch["sha256"] != streaming["sha256"]:
    raise SystemExit("Batch and Streaming ID/vector hashes differ")
json.dump({
    "status": "PASS",
    "count": batch["count"],
    "logicalFieldBytes": batch["logicalFieldBytes"],
    "sha256": batch["sha256"],
}, open(root / "correctness.json", "w", encoding="utf-8"), indent=2, sort_keys=True)
PY
}

# run_interval executes one timed mode interval and captures process evidence.
run_interval() {
    local case_name=$1 repetition=$2 mode=$3
    local label="$case_name/rep$repetition/$mode"
    local local_dir="$RUN_DIR/$label"
    local remote_client_dir="$CLIENT_RUN_DIR/$label"
    local remote_runtime_dir="$SERVER_RUN_DIR/runtime/$label"
    local remote_sample_dir="$SERVER_RUN_DIR/samples/$label"

    mkdir -p "$local_dir"
    client "mkdir -p '$remote_client_dir'"
    start_proxy "$mode" info
    record_proxy_environment "$local_dir/proxy-environment.txt"
    capture_runtime "$remote_runtime_dir" before
    capture_host_snapshot "$local_dir/server-before.txt"
    start_sampler "$remote_sample_dir"
    run_client_action "$case_name-rep$repetition-$mode" \
        "benchmark --host 10.15.9.42 --port '$MILVUS_PORT' \
        --collection '$COLLECTION_NAME' --case '$case_name' --mode '$mode' \
        --repetition '$repetition' --limit '$QUERY_LIMIT' \
        --warmup-operations '$WARMUP_OPERATIONS' \
        --min-operations '$MIN_OPERATIONS' \
        --min-duration-seconds '$MIN_DURATION_SECONDS' \
        --output '$remote_client_dir/benchmark.json'"
    stop_sampler
    capture_runtime "$remote_runtime_dir" after
    capture_host_snapshot "$local_dir/server-after.txt"
    copy_from_client "$remote_client_dir/benchmark.json" "$local_dir/benchmark.json"
    copy_from_server "$remote_sample_dir/processes.csv" "$local_dir/processes.csv"
    copy_runtime "$remote_runtime_dir" "$local_dir/runtime"
    copy_query_logs "$local_dir"
}

# run_cpu_profile captures one separate 30-second Proxy profile per mode.
run_cpu_profile() {
    local case_name=$1 mode=$2
    local label="$case_name/profiles/$mode"
    local local_dir="$RUN_DIR/$label"
    local remote_client_dir="$CLIENT_RUN_DIR/$label"
    local remote_profile="$SERVER_RUN_DIR/$label/cpu.pb.gz"
    mkdir -p "$local_dir"
    client "mkdir -p '$remote_client_dir'"
    start_proxy "$mode" info
    record_proxy_environment "$local_dir/proxy-environment.txt"
    server "mkdir -p '$(dirname "$remote_profile")'"
    server "curl --fail --silent \
        'http://127.0.0.1:$PROXY_METRICS_PORT/debug/pprof/profile?seconds=30' \
        >'$remote_profile'" &
    local profile_pid=$!
    sleep 1
    run_client_action "$case_name-profile-$mode" \
        "benchmark --host 10.15.9.42 --port '$MILVUS_PORT' \
        --collection '$COLLECTION_NAME' --case '$case_name-profile' --mode '$mode' \
        --limit '$QUERY_LIMIT' --warmup-operations 2 \
        --min-operations 10 --min-duration-seconds 35 \
        --output '$remote_client_dir/benchmark.json'"
    wait "$profile_pid"
    copy_from_server "$remote_profile" "$local_dir/cpu.pb.gz"
    copy_from_client "$remote_client_dir/benchmark.json" "$local_dir/benchmark.json"
    server "export PATH=/home/ubuntu/.local/go/bin:\$PATH; \
        cd '$SERVER_REPO' && go tool pprof -top '$SERVER_REPO/bin/milvus' '$remote_profile'" \
        >"$local_dir/cpu-top.txt"
    copy_query_logs "$local_dir"
}

# run_topology loads the collection, qualifies it, and measures one fan-in case.
run_topology() {
    local count=$1
    local case_name="FANIN-N$count"
    local case_dir="$RUN_DIR/$case_name"
    local remote_case_dir="$CLIENT_RUN_DIR/$case_name"
    local repetition mode
    local -a modes

    log "Preparing $case_name"
    mkdir -p "$case_dir"
    client "mkdir -p '$remote_case_dir'"
    run_client_action "$case_name-load" \
        "load --host 10.15.9.42 --port '$MILVUS_PORT' \
        --collection '$COLLECTION_NAME' --output '$remote_case_dir/load.json'"
    copy_from_client "$remote_case_dir/load.json" "$case_dir/load.json"
    COLLECTION_LOADED=true
    qualify_placement "$case_name" "$count" "$case_dir"
    qualify_worknodes "$case_dir" "$remote_case_dir" "$count"
    run_correctness_gate "$case_name" "$case_dir" "$remote_case_dir"

    for ((repetition = 1; repetition <= REPETITIONS; repetition++)); do
        if ((repetition % 2 == 1)); then
            modes=(batch streaming)
        else
            modes=(streaming batch)
        fi
        for mode in "${modes[@]}"; do
            log "Running $case_name repetition $repetition mode $mode"
            run_interval "$case_name" "$repetition" "$mode"
        done
    done

    run_cpu_profile "$case_name" batch
    run_cpu_profile "$case_name" streaming

    mkdir -p "$case_dir/server-logs"
    rsync -az -e "ssh -i '$SSH_KEY' -o BatchMode=yes" \
        "$SERVER_TARGET:$SERVER_RUN_DIR/logs/" "$case_dir/server-logs/"
    printf 'PASS\n' >"$case_dir/status.txt"
}

# build_summary validates all intervals and writes auditable aggregate inputs.
build_summary() {
    python3 - "$RUN_DIR" <<'PY'
import csv
import json
import pathlib
import statistics
import sys

run = pathlib.Path(sys.argv[1])
expected_logical = 8192 * (8 + 768 * 4)
rows = []
case_dirs = sorted(
    (path for path in run.glob("FANIN-N*") if path.is_dir()),
    key=lambda path: int(path.name.rsplit("N", 1)[1]),
)
for case_dir in case_dirs:
    correctness = json.load(open(case_dir / "correctness.json", encoding="utf-8"))
    if correctness["status"] != "PASS":
        raise SystemExit(f"correctness failed: {case_dir}")
    for path in sorted(case_dir.glob("rep*/*/benchmark.json")):
        data = json.load(open(path, encoding="utf-8"))
        if data["errors"] or data["successfulOperations"] < 30 or data["elapsedSeconds"] < 60:
            raise SystemExit(f"invalid timed interval: {path}")
        if data["firstResultSha256"] != correctness["sha256"]:
            raise SystemExit(f"result hash changed: {path}")
        if data["logicalFieldBytesPerOperation"] != expected_logical:
            raise SystemExit(f"logical field bytes changed: {path}")

        samples = list(csv.DictReader(open(path.parent / "processes.csv", encoding="utf-8")))
        by_time = {}
        for sample in samples:
            if sample["role"] == "proxy":
                values = by_time.setdefault(sample["timestamp"], [0.0, 0])
                values[0] += float(sample["pcpu"])
                values[1] += int(sample["rss_kib"])
        if not by_time:
            raise SystemExit(f"no Proxy samples: {path}")

        rows.append({
            "case": case_dir.name,
            "repetition": data["repetition"],
            "mode": data["mode"],
            "operations": data["successfulOperations"],
            "elapsed_seconds": data["elapsedSeconds"],
            "qps": data["qps"],
            "p50_ms": data["latencyNs"]["p50"] / 1e6,
            "p95_ms": data["latencyNs"]["p95"] / 1e6,
            "p99_ms": data["latencyNs"]["p99"] / 1e6,
            "max_ms": data["latencyNs"]["max"] / 1e6,
            "peak_proxy_rss_mib": max(value[1] for value in by_time.values()) / 1024,
            "median_proxy_cpu_percent": statistics.median(value[0] for value in by_time.values()),
            "path": str(path.relative_to(run)),
        })
if len(rows) != 40:
    raise SystemExit(f"found {len(rows)} timed intervals, expected 40")

with open(run / "manifest.tsv", "w", encoding="utf-8", newline="") as output:
    writer = csv.DictWriter(output, fieldnames=rows[0].keys(), delimiter="\t")
    writer.writeheader()
    writer.writerows(rows)

hashes = set()
summary = []
for case_dir in case_dirs:
    correctness = json.load(open(case_dir / "correctness.json", encoding="utf-8"))
    hashes.add(correctness["sha256"])
    values = {mode: [row for row in rows if row["case"] == case_dir.name and row["mode"] == mode]
              for mode in ("batch", "streaming")}
    batch_qps = statistics.median(row["qps"] for row in values["batch"])
    streaming_qps = statistics.median(row["qps"] for row in values["streaming"])
    batch_p95 = statistics.median(row["p95_ms"] for row in values["batch"])
    streaming_p95 = statistics.median(row["p95_ms"] for row in values["streaming"])
    summary.append({
        "case": case_dir.name,
        "batchMedianQps": batch_qps,
        "streamingMedianQps": streaming_qps,
        "streamingToBatchQps": streaming_qps / batch_qps,
        "batchMedianP95Ms": batch_p95,
        "streamingMedianP95Ms": streaming_p95,
        "streamingToBatchP95": streaming_p95 / batch_p95,
        "batchMedianPeakProxyRssMiB": statistics.median(row["peak_proxy_rss_mib"] for row in values["batch"]),
        "streamingMedianPeakProxyRssMiB": statistics.median(row["peak_proxy_rss_mib"] for row in values["streaming"]),
        "batchMedianProxyCpuPercent": statistics.median(row["median_proxy_cpu_percent"] for row in values["batch"]),
        "streamingMedianProxyCpuPercent": statistics.median(row["median_proxy_cpu_percent"] for row in values["streaming"]),
    })
if len(hashes) != 1:
    raise SystemExit("result hash differs across fan-in cases")
json.dump(summary, open(run / "summary.json", "w", encoding="utf-8"), indent=2, sort_keys=True)
json.dump({"cases": summary}, open(run / "report-input.json", "w", encoding="utf-8"), indent=2, sort_keys=True)
PY
}

# main executes preflight, per-case qualification, correctness, timed, profile,
# and aggregation phases for every fan-in topology.
main() {
    initialize_runner
    log "Run ID: $RUN_ID"
    preflight
    local local_head remote_head
    local_head=$(git -C "$MILVUS_LOCAL_REPO" rev-parse HEAD)
    remote_head=$(server "git -C '$SERVER_REPO' rev-parse HEAD")
    [[ "$local_head" == "$REQUIRED_MILVUS_COMMIT" ]] \
        || fail "local Milvus HEAD $local_head is not $REQUIRED_MILVUS_COMMIT"
    [[ "$remote_head" == "$REQUIRED_MILVUS_COMMIT" ]] \
        || fail "remote Milvus HEAD $remote_head is not $REQUIRED_MILVUS_COMMIT"

    write_server_configuration
    install_client_driver
    start_infrastructure
    start_base_roles
    start_querynodes 1
    start_proxy batch info

    run_client_action inspect \
        "inspect --host 10.15.9.42 --port '$MILVUS_PORT' \
        --collection '$COLLECTION_NAME' --output '$CLIENT_RUN_DIR/collection.json'"
    copy_from_client "$CLIENT_RUN_DIR/collection.json" "$RUN_DIR/collection.json"

    local count first=true
    for count in "${TOPOLOGIES[@]}"; do
        if [[ "$first" == false ]]; then
            run_client_action "FANIN-N$count-release" \
                "release --host 10.15.9.42 --port '$MILVUS_PORT' \
                --collection '$COLLECTION_NAME' --output '$CLIENT_RUN_DIR/release.json'"
            stop_querynodes
            start_querynodes "$count"
        fi
        first=false
        run_topology "$count"
    done

    build_summary
    log "All fan-in Batch-versus-Streaming Query intervals passed"
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
    main "$@"
fi
