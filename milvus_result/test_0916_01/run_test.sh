#!/usr/bin/env bash

set -Eeuo pipefail

THIS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# Reuse the accepted deployment, dataset, client, sampling, and cleanup code.
source "$THIS_DIR/../test_0915_02/run_test.sh"

SCRIPT_DIR="$THIS_DIR"
TEST_REPO="$(cd "$SCRIPT_DIR/../.." && pwd)"
WORKSPACE_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
MILVUS_LOCAL_REPO="${MILVUS_LOCAL_REPO:-$WORKSPACE_ROOT/milvus-qv}"

RUN_ID="${RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)-$$}"
RUN_DIR="$SCRIPT_DIR/runs/$RUN_ID"
SERVER_RUN_DIR="/home/ubuntu/reducestream-e2e/$RUN_ID"
CLIENT_RUN_DIR="/home/ubuntu/reducestream-e2e/$RUN_ID"
COMPOSE_PROJECT="qv-metrics-$(printf '%s' "$RUN_ID" | tr '[:upper:]' '[:lower:]' | tr -cd '[:alnum:]-')"
COMPOSE_OVERRIDE="$SERVER_RUN_DIR/docker-compose.override.yml"
SERVER_ENV="$SERVER_RUN_DIR/milvus.env"
CLIENT_DRIVER="$CLIENT_RUN_DIR/client_driver.py"

TOPOLOGIES=(1)
REPETITIONS=4
SEARCH_METRICS_ENABLED=false
COLLECTION_LOADED=false
PROXY_METRICS_PORT=$((METRICS_BASE + 4))

log() {
    printf '[test_0916_01] %s\n' "$*"
}

start_role() {
    local name=$1 role=$2 metrics_port=$3 rpc_port=${4:-0}
    local log_level=${5:-info} search_streaming=${6:-unused}
    local benchmark_metrics=${7:-$SEARCH_METRICS_ENABLED}

    log "Starting $name, search metrics=$benchmark_metrics"
    server_bash \
        "$name" "$role" "$metrics_port" "$rpc_port" "$log_level" \
        "$search_streaming" "$benchmark_metrics" "$SERVER_RUN_DIR" \
        "$SERVER_REPO" "$SERVER_ENV" <<'REMOTE'
set -Eeuo pipefail
name=$1
role=$2
metrics_port=$3
rpc_port=$4
log_level=$5
search_streaming=$6
benchmark_metrics=$7
run_dir=$8
repo=$9
env_file=${10}

mkdir -p "$run_dir/logs" "$run_dir/pids" "$run_dir/local/$name"
(
    set -a
    source "$env_file"
    set +a
    export METRICS_PORT="$metrics_port"
    export LOCALSTORAGE_PATH="$run_dir/local/$name"
    export MILVUS_CONF_LOGLEVEL="$log_level"
    export MILVUS_CONF_LOGFORMAT=text
    export MILVUS_CONF_PROXYQUERYVIEWENABLESEARCHBENCHMARKMETRICS="$benchmark_metrics"
    if [[ "$role" == "querynode" ]]; then
        export MILVUS_CONF_QUERYNODE_PORT="$rpc_port"
    elif [[ "$role" == "streamingnode" ]]; then
        export MILVUS_CONF_STREAMINGNODE_PORT="$rpc_port"
    elif [[ "$role" == "proxy" ]]; then
        export MILVUS_CONF_PROXYQUERYVIEWENABLESEARCHSTREAMING="$search_streaming"
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

start_proxy() {
    local mode=$1 log_level=${2:-info}
    local streaming=false
    [[ "$mode" == streaming ]] && streaming=true
    stop_pid_file "$SERVER_RUN_DIR/pids/proxy.pid"
    start_role proxy proxy "$PROXY_METRICS_PORT" 0 "$log_level" \
        "$streaming" "$SEARCH_METRICS_ENABLED"
    wait_for_tcp 10.15.9.42 "$MILVUS_PORT" 300
}

release_collection() {
    [[ "$COLLECTION_LOADED" == true ]] || return 0
    run_client_action release \
        "release --host 10.15.9.42 --port '$MILVUS_PORT' \
        --collection '$COLLECTION_NAME' --output '$CLIENT_RUN_DIR/release.json'"
    COLLECTION_LOADED=false
}

prepare_cell() {
    local mode=$1 metrics_enabled=$2 label=$3
    release_collection
    stop_querynodes
    stop_pid_file "$SERVER_RUN_DIR/pids/proxy.pid"
    SEARCH_METRICS_ENABLED=$metrics_enabled
    server "rm -f '$SERVER_RUN_DIR/logs/proxy.log' '$SERVER_RUN_DIR'/logs/querynode_*.log"
    start_querynodes 1
    start_proxy "$mode" info
    client "mkdir -p '$CLIENT_RUN_DIR/$label'"
    run_client_action "$label-load" \
        "load --host 10.15.9.42 --port '$MILVUS_PORT' \
        --collection '$COLLECTION_NAME' --output '$CLIENT_RUN_DIR/$label/load.json'"
    COLLECTION_LOADED=true
}

copy_cell_logs() {
    local output=$1
    mkdir -p "$output"
    copy_from_server "$SERVER_RUN_DIR/logs/proxy.log" "$output/proxy.log"
    copy_from_server "$SERVER_RUN_DIR/logs/querynode_1.log" "$output/querynode.log"
}

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

copy_runtime() {
    local remote_dir=$1 output=$2
    mkdir -p "$output"
    rsync -az -e "ssh -i '$SSH_KEY' -o BatchMode=yes" \
        "$SERVER_TARGET:$remote_dir/" "$output/"
}

record_process_environment() {
    local output=$1
    server "pid=\$(cat '$SERVER_RUN_DIR/pids/proxy.pid'); \
        tr '\0' '\n' < /proc/\$pid/environ | \
        grep -E '^(MILVUS_CONF_PROXYQUERYVIEW|METRICS_PORT=)' | sort" >"$output"
}

run_diagnostic() {
    local mode=$1
    local local_dir="$RUN_DIR/diagnostic/$mode"
    local remote_dir="$CLIENT_RUN_DIR/diagnostic/$mode"
    mkdir -p "$local_dir"
    prepare_cell "$mode" true "diagnostic/$mode"
    run_client_action "diagnostic-$mode" \
        "verify --host 10.15.9.42 --port '$MILVUS_PORT' \
        --collection '$COLLECTION_NAME' --mode '$mode' --query-count 1 \
        --topk '$TOP_K' --ef '$SEARCH_EF' --output '$remote_dir/verify.json'"
    copy_from_client "$remote_dir/verify.json" "$local_dir/verify.json"
    cp "$local_dir/verify.json" "$RUN_DIR/diagnostic/$mode-verify.json"
    copy_cell_logs "$local_dir"
    record_process_environment "$local_dir/proxy-environment.txt"
}

reconcile_diagnostics() {
    compare_correctness "$RUN_DIR/diagnostic"
    python3 - "$RUN_DIR/diagnostic" "$RUN_DIR/reconciliation.json" <<'PY'
import json
import pathlib
import re
import sys

root = pathlib.Path(sys.argv[1])
field_pattern = re.compile(r'\[([A-Za-z][A-Za-z0-9]*)=("[^"]*"|[^\]]*)\]')

def parse(path):
    lines = [line for line in path.read_text(errors="replace").splitlines()
             if 'query view search benchmark metrics' in line]
    if len(lines) != 1:
        raise SystemExit(f"{path} has {len(lines)} metric records, expected 1")
    fields = dict(field_pattern.findall(lines[0]))
    return {key: value[1:-1] if value.startswith('"') else value
            for key, value in fields.items()}

def integer(fields, key):
    return int(fields[key])

def integers(fields, key):
    return [int(value) for value in re.findall(r'-?\d+', fields[key])]

result = {}
for mode, expected_messages in (("batch", 1), ("streaming", 8)):
    proxy = parse(root / mode / "proxy.log")
    querynode = parse(root / mode / "querynode.log")
    if integer(proxy, "requestID") != integer(querynode, "requestID"):
        raise SystemExit(f"{mode}: Proxy and QueryNode request IDs differ")
    if integer(querynode, "generatedUnits") != 8192:
        raise SystemExit(f"{mode}: QueryNode generatedUnits != 8192")
    for key in ("sendCompletedMessages", "grpcOutMessages"):
        if integer(querynode, key) != expected_messages:
            raise SystemExit(f"{mode}: QueryNode {key} != {expected_messages}")
    if integer(querynode, "sendCompletedBytes") != integer(querynode, "grpcOutPayloadBytes"):
        raise SystemExit(f"{mode}: completed bytes do not reconcile with gRPC output")
    for key in ("childApplicationReceivedMessages", "childGRPCInMessages"):
        if integers(proxy, key) != [expected_messages]:
            raise SystemExit(f"{mode}: Proxy {key} does not match message count")
    if integers(proxy, "childApplicationReceivedBytes") != integers(proxy, "childGRPCInPayloadBytes"):
        raise SystemExit(f"{mode}: Proxy application and gRPC payload bytes differ")
    if integers(proxy, "childApplicationReceivedUnits") != [8192]:
        raise SystemExit(f"{mode}: Proxy did not receive 8192 Units")
    expected_consumed = [8192] if mode == "streaming" else [0]
    if integers(proxy, "childApplicationConsumedUnits") != expected_consumed:
        raise SystemExit(f"{mode}: unexpected ReduceStream consumed Units")
    if integer(proxy, "finalResultCount") != 8192:
        raise SystemExit(f"{mode}: final result count != 8192")
    for key in ("grpcInMessages", "grpcOutMessages", "connectionReadBytes", "connectionWriteBytes"):
        if integer(proxy, key) != 0:
            raise SystemExit(f"{mode}: root metric {key} was polluted")
    if proxy["childTCPInfoAvailable"] != "[true]" or querynode["tcpInfoAvailable"] != "true":
        raise SystemExit(f"{mode}: TCP_INFO is unavailable")
    result[mode] = {"proxy": proxy, "querynode": querynode}

if result["batch"]["proxy"]["finalResultHash"] != result["streaming"]["proxy"]["finalResultHash"]:
    raise SystemExit("Batch and Streaming final-result hashes differ")
json.dump(result, open(sys.argv[2], "w", encoding="utf-8"), indent=2, sort_keys=True)
PY
}

run_overhead_interval() {
    local repetition=$1 mode=$2 metrics_enabled=$3
    local state=off
    [[ "$metrics_enabled" == true ]] && state=on
    local label="overhead/rep$repetition/$mode-metrics-$state"
    local local_dir="$RUN_DIR/$label"
    local remote_client_dir="$CLIENT_RUN_DIR/$label"
    local remote_runtime_dir="$SERVER_RUN_DIR/runtime/$label"
    local remote_sample_dir="$SERVER_RUN_DIR/samples/$label"

    mkdir -p "$local_dir"
    prepare_cell "$mode" "$metrics_enabled" "$label"
    record_process_environment "$local_dir/proxy-environment.txt"
    capture_runtime "$remote_runtime_dir" before
    capture_host_snapshot "$local_dir/server-before.txt"
    start_sampler "$remote_sample_dir"
    run_client_action "rep$repetition-$mode-metrics-$state" \
        "benchmark --host 10.15.9.42 --port '$MILVUS_PORT' \
        --collection '$COLLECTION_NAME' --case 'N1-$state' --mode '$mode' \
        --repetition '$repetition' --topk '$TOP_K' --ef '$SEARCH_EF' \
        --chunk-size '$CHUNK_SIZE' --query-count 100 \
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
    copy_cell_logs "$local_dir"

    if [[ "$metrics_enabled" == false ]]; then
        if grep -q 'query view search benchmark metrics' \
            "$local_dir/proxy.log" "$local_dir/querynode.log"; then
            fail "$label emitted benchmark metric records while disabled"
        fi
    elif ! grep -q 'query view search benchmark metrics' "$local_dir/proxy.log" \
        || ! grep -q 'query view search benchmark metrics' "$local_dir/querynode.log"; then
        fail "$label is missing enabled benchmark metric records"
    fi
}

run_overhead_matrix() {
    local repetition cell mode metrics
    local -a cells
    for ((repetition = 1; repetition <= REPETITIONS; repetition++)); do
        case $repetition in
            1) cells=(batch:false batch:true streaming:false streaming:true) ;;
            2) cells=(streaming:true streaming:false batch:true batch:false) ;;
            3) cells=(batch:true batch:false streaming:true streaming:false) ;;
            4) cells=(streaming:false streaming:true batch:false batch:true) ;;
        esac
        for cell in "${cells[@]}"; do
            mode=${cell%%:*}
            metrics=${cell##*:}
            log "Overhead repetition $repetition: $mode, metrics=$metrics"
            run_overhead_interval "$repetition" "$mode" "$metrics"
        done
    done
}

run_cpu_profile() {
    local mode=$1
    local label="profiles/$mode-metrics-on"
    local local_dir="$RUN_DIR/$label"
    local remote_client_dir="$CLIENT_RUN_DIR/$label"
    local remote_profile="$SERVER_RUN_DIR/$label/cpu.pb.gz"
    mkdir -p "$local_dir"
    prepare_cell "$mode" true "$label"
    server "mkdir -p '$(dirname "$remote_profile")'"
    server "curl --fail --silent \
        'http://127.0.0.1:$PROXY_METRICS_PORT/debug/pprof/profile?seconds=30' \
        >'$remote_profile'" &
    local profile_pid=$!
    sleep 1
    run_client_action "profile-$mode" \
        "benchmark --host 10.15.9.42 --port '$MILVUS_PORT' \
        --collection '$COLLECTION_NAME' --case 'N1-profile' --mode '$mode' \
        --repetition 0 --topk '$TOP_K' --ef '$SEARCH_EF' \
        --chunk-size '$CHUNK_SIZE' --query-count 100 --warmup-operations 10 \
        --min-operations 50 --min-duration-seconds 35 \
        --output '$remote_client_dir/benchmark.json'"
    wait "$profile_pid"
    copy_from_server "$remote_profile" "$local_dir/cpu.pb.gz"
    copy_from_client "$remote_client_dir/benchmark.json" "$local_dir/benchmark.json"
    server "export PATH=/home/ubuntu/.local/go1.26.5/bin:\$PATH; \
        cd '$SERVER_REPO' && go tool pprof -top '$SERVER_REPO/bin/milvus' '$remote_profile'" \
        >"$local_dir/cpu-top.txt"
    copy_cell_logs "$local_dir"
}

build_summary() {
    python3 - "$RUN_DIR" <<'PY'
import csv
import json
import pathlib
import statistics
import sys

run = pathlib.Path(sys.argv[1])
rows = []
hashes = {}
for path in sorted(run.glob("overhead/rep*/*/benchmark.json")):
    data = json.load(open(path, encoding="utf-8"))
    state = "on" if "metrics-on" in str(path) else "off"
    if data["errors"] or data["successfulOperations"] < 100 or data["elapsedSeconds"] < 60:
        raise SystemExit(f"invalid timed interval: {path}")
    for operation in data["operations"]:
        if operation["count"] != 8192:
            raise SystemExit(f"incomplete result: {path}")
        key = operation["queryIndex"]
        previous = hashes.setdefault(key, operation["sha256"])
        if previous != operation["sha256"]:
            raise SystemExit(f"result hash changed: {path}, query {key}")
    samples = list(csv.DictReader(open(path.parent / "processes.csv", encoding="utf-8")))
    by_time = {}
    for sample in samples:
        if sample["role"] == "proxy":
            entry = by_time.setdefault(sample["timestamp"], [0.0, 0])
            entry[0] += float(sample["pcpu"])
            entry[1] += int(sample["rss_kib"])
    rows.append({
        "repetition": data["repetition"], "mode": data["mode"], "metrics": state,
        "qps": data["qps"], "p50_ms": data["latencyNs"]["p50"] / 1e6,
        "p95_ms": data["latencyNs"]["p95"] / 1e6,
        "p99_ms": data["latencyNs"]["p99"] / 1e6,
        "max_ms": data["latencyNs"]["max"] / 1e6,
        "peak_proxy_rss_mib": max(value[1] for value in by_time.values()) / 1024,
        "median_proxy_cpu_percent": statistics.median(value[0] for value in by_time.values()),
        "path": str(path.relative_to(run)),
    })
if len(rows) != 16:
    raise SystemExit(f"found {len(rows)} timed intervals, expected 16")

with open(run / "manifest.tsv", "w", encoding="utf-8", newline="") as output:
    writer = csv.DictWriter(output, fieldnames=rows[0].keys(), delimiter="\t")
    writer.writeheader()
    writer.writerows(rows)

summary = []
for mode in ("batch", "streaming"):
    values = {state: [row for row in rows if row["mode"] == mode and row["metrics"] == state]
              for state in ("off", "on")}
    off_qps = statistics.median(row["qps"] for row in values["off"])
    on_qps = statistics.median(row["qps"] for row in values["on"])
    off_p95 = statistics.median(row["p95_ms"] for row in values["off"])
    on_p95 = statistics.median(row["p95_ms"] for row in values["on"])
    summary.append({
        "mode": mode, "metricsOffMedianQps": off_qps, "metricsOnMedianQps": on_qps,
        "metricsOnToOffQps": on_qps / off_qps,
        "metricsOffMedianP95Ms": off_p95, "metricsOnMedianP95Ms": on_p95,
        "metricsOnToOffP95": on_p95 / off_p95,
        "metricsOffMedianPeakProxyRssMiB": statistics.median(row["peak_proxy_rss_mib"] for row in values["off"]),
        "metricsOnMedianPeakProxyRssMiB": statistics.median(row["peak_proxy_rss_mib"] for row in values["on"]),
        "metricsOffMedianProxyCpuPercent": statistics.median(row["median_proxy_cpu_percent"] for row in values["off"]),
        "metricsOnMedianProxyCpuPercent": statistics.median(row["median_proxy_cpu_percent"] for row in values["on"]),
    })
json.dump(summary, open(run / "summary.json", "w", encoding="utf-8"), indent=2)
json.dump({"reconciliation": json.load(open(run / "reconciliation.json", encoding="utf-8")),
           "overhead": summary}, open(run / "report-input.json", "w", encoding="utf-8"), indent=2)
PY
}

main() {
    initialize_runner
    log "Run ID: $RUN_ID"
    preflight
    write_server_configuration
    install_client_driver
    start_infrastructure
    SEARCH_METRICS_ENABLED=false
    start_base_roles

    run_diagnostic batch
    mkdir -p "$RUN_DIR/topology"
    qualify_placement N1 1 "$RUN_DIR/topology"
    run_diagnostic streaming
    reconcile_diagnostics
    run_overhead_matrix
    run_cpu_profile batch
    run_cpu_profile streaming
    build_summary
    log "Issue 2 reconciliation and overhead experiment passed"
}

main "$@"
