#!/usr/bin/env bash

set -Eeuo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TEST_REPO="$(cd "$SCRIPT_DIR/../.." && pwd)"
WORKSPACE_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
MILVUS_LOCAL_REPO="${MILVUS_LOCAL_REPO:-$WORKSPACE_ROOT/milvus-qv}"

SSH_KEY="${SSH_KEY:-$HOME/Downloads/ec2_qtp.pem}"
SERVER_TARGET="${SERVER_TARGET:-ubuntu@10.15.9.42}"
CLIENT_TARGET="${CLIENT_TARGET:-ubuntu@10.15.2.233}"
SERVER_REPO="${SERVER_REPO:-/home/ubuntu/milvus-qv}"
CLIENT_REPO="${CLIENT_REPO:-/home/ubuntu/reducestream_perf}"

SOURCE_RUN_ID="${SOURCE_RUN_ID:-20260915T090613Z-20989}"
SOURCE_LOCAL_RUN_DIR="$SCRIPT_DIR/../test_0915_01/runs/$SOURCE_RUN_ID"
SOURCE_SERVER_RUN_DIR="/home/ubuntu/reducestream-e2e/$SOURCE_RUN_ID"

RUN_ID="${RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)-$$}"
RUN_DIR="$SCRIPT_DIR/runs/$RUN_ID"
SERVER_RUN_DIR="/home/ubuntu/reducestream-e2e/$RUN_ID"
CLIENT_RUN_DIR="/home/ubuntu/reducestream-e2e/$RUN_ID"
COLLECTION_NAME="${COLLECTION_NAME:-cohere_1m_qn_fanin}"

MILVUS_PORT=19532
MINIO_PORT=19000
MINIO_CONSOLE_PORT=19001
METRICS_BASE=19090
QUERYNODE_RPC_BASE=21123
QUERYNODE_METRICS_BASE=19200
STREAMINGNODE_PORT=22222
TOP_K=8192
SEARCH_EF=8192
CHUNK_SIZE=1024
WARMUP_OPERATIONS=20
MIN_OPERATIONS=100
MIN_DURATION_SECONDS=60
REPETITIONS=4
TARGET_ROWS_PER_SHARD_NODE=31250
TOPOLOGIES=(1 2 4 8 16 32)

COMPOSE_PROJECT="qv-perf-$(printf '%s' "$RUN_ID" | tr '[:upper:]' '[:lower:]' | tr -cd '[:alnum:]-')"
COMPOSE_FILE="$SERVER_REPO/deployments/docker/dev/docker-compose-apple-silicon.yml"
COMPOSE_OVERRIDE="$SERVER_RUN_DIR/docker-compose.override.yml"
SERVER_ENV="$SERVER_RUN_DIR/milvus.env"
CLIENT_DRIVER="$CLIENT_RUN_DIR/client_driver.py"

SSH_OPTIONS=(-i "$SSH_KEY" -o BatchMode=yes -o ConnectTimeout=15 -o ServerAliveInterval=30)
INFRA_STARTED=false
ACTIVE_SAMPLER=""

mkdir -p "$RUN_DIR"
exec > >(tee -a "$RUN_DIR/run.log") 2>&1

log() {
    printf '[test_0915_02] %s\n' "$*"
}

fail() {
    log "ERROR: $*"
    return 1
}

server() {
    ssh "${SSH_OPTIONS[@]}" "$SERVER_TARGET" "$@"
}

client() {
    ssh "${SSH_OPTIONS[@]}" "$CLIENT_TARGET" "$@"
}

server_bash() {
    ssh "${SSH_OPTIONS[@]}" "$SERVER_TARGET" bash -s -- "$@"
}

copy_from_server() {
    scp "${SSH_OPTIONS[@]}" "$SERVER_TARGET:$1" "$2"
}

copy_from_client() {
    scp "${SSH_OPTIONS[@]}" "$CLIENT_TARGET:$1" "$2"
}

compose_remote() {
    server "COMPOSE_PROJECT_NAME='$COMPOSE_PROJECT' \
        DOCKER_VOLUME_DIRECTORY='$SOURCE_SERVER_RUN_DIR/infra' \
        docker compose -f '$COMPOSE_FILE' -f '$COMPOSE_OVERRIDE' $*"
}

wait_for_health() {
    local name=$1
    local port=$2
    local timeout_seconds=${3:-300}

    server_bash "$name" "$port" "$timeout_seconds" <<'REMOTE'
set -Eeuo pipefail
name=$1
port=$2
timeout_seconds=$3
deadline=$((SECONDS + timeout_seconds))
until curl --fail --silent "http://127.0.0.1:${port}/healthz" >/dev/null; do
    if ((SECONDS >= deadline)); then
        echo "$name did not become healthy on metrics port $port" >&2
        exit 1
    fi
    sleep 2
done
REMOTE
}

wait_for_tcp() {
    local host=$1
    local port=$2
    local timeout_seconds=${3:-300}

    ssh "${SSH_OPTIONS[@]}" "$CLIENT_TARGET" \
        python3 - "$host" "$port" "$timeout_seconds" <<'PY'
import socket
import sys
import time

host = sys.argv[1]
port = int(sys.argv[2])
deadline = time.monotonic() + int(sys.argv[3])
while True:
    try:
        with socket.create_connection((host, port), timeout=3):
            break
    except OSError:
        if time.monotonic() >= deadline:
            raise
        time.sleep(2)
PY
}

start_role() {
    local name=$1
    local role=$2
    local metrics_port=$3
    local rpc_port=${4:-0}
    local log_level=${5:-info}
    # SSH flattens its command arguments, so an empty positional argument would
    # disappear before the remote shell and shift the remaining role settings.
    local search_streaming=${6:-unused}

    log "Starting $name"
    server_bash \
        "$name" "$role" "$metrics_port" "$rpc_port" "$log_level" \
        "$search_streaming" "$SERVER_RUN_DIR" "$SERVER_REPO" "$SERVER_ENV" <<'REMOTE'
set -Eeuo pipefail
name=$1
role=$2
metrics_port=$3
rpc_port=$4
log_level=$5
search_streaming=$6
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

stop_pid_file() {
    local pid_file=$1
    server_bash "$pid_file" <<'REMOTE'
set -Eeuo pipefail
pid_file=$1
[[ -f "$pid_file" ]] || exit 0
pid=$(cat "$pid_file")
if kill -0 "$pid" 2>/dev/null; then
    kill -TERM -- "-$pid" 2>/dev/null || kill -TERM "$pid" 2>/dev/null || true
    deadline=$((SECONDS + 120))
    while kill -0 "$pid" 2>/dev/null; do
        if ((SECONDS >= deadline)); then
            kill -KILL -- "-$pid" 2>/dev/null || kill -KILL "$pid" 2>/dev/null || true
            break
        fi
        sleep 2
    done
fi
rm -f "$pid_file"
REMOTE
}

start_proxy() {
    local mode=$1
    local log_level=${2:-info}
    local enabled=false
    [[ "$mode" == "streaming" ]] && enabled=true

    stop_pid_file "$SERVER_RUN_DIR/pids/proxy.pid"
    start_role proxy proxy "$((METRICS_BASE + 4))" 0 "$log_level" "$enabled"
    wait_for_tcp 10.15.9.42 "$MILVUS_PORT" 300
}

stop_querynodes() {
    local pid_file
    log "Stopping QueryNodes"
    while IFS= read -r pid_file; do
        [[ -n "$pid_file" ]] || continue
        stop_pid_file "$pid_file"
    done < <(server "find '$SERVER_RUN_DIR/pids' -maxdepth 1 -name 'querynode_*.pid' -print 2>/dev/null | sort")
}

start_querynodes() {
    local count=$1
    local i
    server "rm -f '$SERVER_RUN_DIR'/logs/querynode_*.log"
    for ((i = 1; i <= count; i++)); do
        start_role \
            "querynode_$i" querynode \
            "$((QUERYNODE_METRICS_BASE + i - 1))" \
            "$((QUERYNODE_RPC_BASE + i - 1))"
    done

    local running
    running=$(server "find '$SERVER_RUN_DIR/pids' -maxdepth 1 -name 'querynode_*.pid' | wc -l")
    [[ "$running" -eq "$count" ]] \
        || fail "started $running QueryNodes, expected $count"
}

stop_base_roles() {
    local role
    for role in proxy streamingnode datanode mixcoord; do
        stop_pid_file "$SERVER_RUN_DIR/pids/$role.pid"
    done
}

stop_sampler() {
    [[ -n "$ACTIVE_SAMPLER" ]] || return 0
    server_bash "$ACTIVE_SAMPLER" <<'REMOTE'
set +e
sample_dir=$1
touch "$sample_dir/stop"
if [[ -s "$sample_dir/sampler.pid" ]]; then
    pid=$(cat "$sample_dir/sampler.pid")
    deadline=$((SECONDS + 30))
    while kill -0 "$pid" 2>/dev/null && ((SECONDS < deadline)); do
        sleep 1
    done
    kill "$pid" 2>/dev/null || true
fi
REMOTE
    ACTIVE_SAMPLER=""
}

collect_server_evidence() {
    server "find '$SERVER_RUN_DIR' -maxdepth 2 -type f -printf '%p %s bytes\n' | sort" \
        >"$RUN_DIR/server-files.txt" 2>&1 || true
    mkdir -p "$RUN_DIR/final-server-logs"
    rsync -az -e "ssh -i '$SSH_KEY' -o BatchMode=yes" \
        "$SERVER_TARGET:$SERVER_RUN_DIR/logs/" "$RUN_DIR/final-server-logs/" \
        >/dev/null 2>&1 || true
}

cleanup() {
    local status=$?
    trap - EXIT INT TERM
    set +e

    stop_sampler
    collect_server_evidence
    stop_querynodes
    stop_base_roles
    if [[ "$INFRA_STARTED" == true ]]; then
        compose_remote down --remove-orphans >/dev/null 2>&1
    fi

    if ((status == 0)); then
        printf 'PASS\n' >"$RUN_DIR/COMPLETED"
        log "PASS. Evidence: $RUN_DIR"
    else
        printf 'status=%s\n' "$status" >"$RUN_DIR/FAILED"
        log "FAILED with status $status. Evidence: $RUN_DIR"
    fi
    exit "$status"
}

trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

preflight() {
    local command
    for command in git ssh scp rsync python3; do
        command -v "$command" >/dev/null || fail "missing local command: $command"
    done
    [[ -r "$SSH_KEY" ]] || fail "SSH identity file is not readable: $SSH_KEY"
    [[ -d "$MILVUS_LOCAL_REPO/.git" ]] || fail "missing Milvus checkout: $MILVUS_LOCAL_REPO"
    [[ -s "$SOURCE_LOCAL_RUN_DIR/N1/placement-3.json" ]] \
        || fail "missing accepted topology evidence: $SOURCE_LOCAL_RUN_DIR"

    git -C "$MILVUS_LOCAL_REPO" status --short --branch >"$RUN_DIR/local-milvus-status.txt"
    git -C "$TEST_REPO" rev-parse HEAD >"$RUN_DIR/test-plan-commit.txt"
    git -C "$TEST_REPO" status --short --branch >"$RUN_DIR/test-plan-status.txt"
    git -C "$TEST_REPO" diff >"$RUN_DIR/test-plan.diff"
    cp "$SCRIPT_DIR/TEST_PLAN.md" "$RUN_DIR/TEST_PLAN.md"
    cp "$SCRIPT_DIR/run_test.sh" "$RUN_DIR/run_test.sh"
    cp "$SOURCE_LOCAL_RUN_DIR/dataset.json" "$RUN_DIR/source-dataset.json"
    cp "$SOURCE_LOCAL_RUN_DIR/prepare.json" "$RUN_DIR/source-prepare.json"
    cp "$SOURCE_LOCAL_RUN_DIR/segment-size.json" "$RUN_DIR/source-segment-size.json"

    local local_head remote_head
    local_head=$(git -C "$MILVUS_LOCAL_REPO" rev-parse HEAD)
    remote_head=$(server "git -C '$SERVER_REPO' rev-parse HEAD")
    printf '%s\n' "$local_head" >"$RUN_DIR/local-milvus-commit.txt"
    printf '%s\n' "$remote_head" >"$RUN_DIR/remote-milvus-commit.txt"
    [[ "$local_head" == "$remote_head" ]] \
        || fail "local Milvus HEAD $local_head differs from .42 HEAD $remote_head"
    [[ -z "$(git -C "$MILVUS_LOCAL_REPO" status --porcelain)" ]] \
        || fail "local Milvus checkout is not clean"
    [[ -z "$(server "git -C '$SERVER_REPO' status --porcelain")" ]] \
        || fail "remote Milvus checkout is not clean"

    server "test -d '$SOURCE_SERVER_RUN_DIR/infra/volumes' && \
        test -s '$SOURCE_SERVER_RUN_DIR/milvus.env'"
    client "test -x '$CLIENT_REPO/.venv/bin/python'"

    server "uname -a; nproc; free -b; df -PB1 /home/ubuntu; \
        ps -eo pid,pcpu,pmem,rss,comm,args --sort=-rss | head -n 30; docker ps" \
        >"$RUN_DIR/server-preflight.txt"
    client "uname -a; nproc; free -b; df -PB1 /home/ubuntu; \
        ps -eo pid,pcpu,pmem,rss,comm,args --sort=-rss | head -n 30; sudo -n docker ps" \
        >"$RUN_DIR/client-preflight.txt"
    client "git -C '$CLIENT_REPO' rev-parse HEAD; \
        git -C '$CLIENT_REPO' status --short --branch" \
        >"$RUN_DIR/client-source-status.txt"
    client "cd '$CLIENT_REPO' && .venv/bin/python -c \
        'import pymilvus; print(pymilvus.__version__)'" \
        >"$RUN_DIR/pymilvus-version.txt"

    if server "pgrep -af '[m]ilvus run' >/dev/null || [[ -n \"\$(docker ps -q)\" ]]"; then
        fail ".42 has a running Milvus process or container"
    fi
    if client "pgrep -af '[l]ocust|[v]ectordb_bench' >/dev/null"; then
        fail ".233 has a running benchmark workload"
    fi

    python3 - "$SOURCE_LOCAL_RUN_DIR/N1/placement-3.json" \
        "$RUN_DIR/fixed-segments.json" <<'PY'
import json
import sys

records = json.load(open(sys.argv[1], encoding="utf-8"))
segments = sorted({item["segmentID"]: item["numRows"] for item in records}.items())
if len(segments) != 63:
    raise SystemExit(f"accepted topology has {len(segments)} segments, expected 63")
json.dump(segments, open(sys.argv[2], "w", encoding="utf-8"), indent=2)
PY

    log "Building Milvus once on .42"
    server "export PATH=/home/ubuntu/.local/go1.26.5/bin:\$PATH GOPATH=/home/ubuntu/go; \
        cd '$SERVER_REPO' && make milvus" | tee "$RUN_DIR/remote-build.log"
    server "test -x '$SERVER_REPO/bin/milvus'"
}

write_server_configuration() {
    server "mkdir -p '$SERVER_RUN_DIR/logs' '$SERVER_RUN_DIR/pids' \
        '$SERVER_RUN_DIR/infra' '$SERVER_RUN_DIR/local'"
    server "cp '$SOURCE_SERVER_RUN_DIR/milvus.env' '$SERVER_ENV'"
    server "sed -i \
        -e '/^MILVUS_CONF_PROXYQUERYVIEWENABLESEARCHSTREAMING=/d' \
        -e '/^MILVUS_CONF_PROXYQUERYVIEWSEARCHSTREAMCHUNKSIZE=/d' \
        '$SERVER_ENV'; \
        printf '%s\n' \
        'MILVUS_CONF_PROXYQUERYVIEWSEARCHSTREAMCHUNKSIZE=$CHUNK_SIZE' \
        'MILVUS_CONF_QUERYCOORDQUERYVIEWTARGETROWSPERSHARDNODE=$TARGET_ROWS_PER_SHARD_NODE' \
        >> '$SERVER_ENV'"

    ssh "${SSH_OPTIONS[@]}" "$SERVER_TARGET" "cat > '$COMPOSE_OVERRIDE'" <<EOF
services:
  minio:
    ports: !override
      - "127.0.0.1:${MINIO_PORT}:9000"
      - "127.0.0.1:${MINIO_CONSOLE_PORT}:9001"
EOF
    copy_from_server "$SERVER_ENV" "$RUN_DIR/milvus.env"
}

start_infrastructure() {
    log "Starting etcd, Pulsar, and MinIO from the accepted run volumes"
    INFRA_STARTED=true
    compose_remote up -d etcd pulsar minio
    server_bash "$MINIO_PORT" <<'REMOTE'
set -Eeuo pipefail
minio_port=$1
deadline=$((SECONDS + 300))
until curl --fail --silent http://127.0.0.1:2379/health >/dev/null \
    && curl --fail --silent "http://127.0.0.1:${minio_port}/minio/health/live" >/dev/null \
    && (exec 3<>/dev/tcp/127.0.0.1/6650) 2>/dev/null; do
    if ((SECONDS >= deadline)); then
        echo "Milvus dependencies did not become healthy" >&2
        exit 1
    fi
    sleep 2
done
REMOTE
}

start_base_roles() {
    start_role mixcoord mixcoord "$((METRICS_BASE + 1))"
    start_role datanode datanode "$((METRICS_BASE + 2))"
    start_role streamingnode streamingnode "$((METRICS_BASE + 3))" "$STREAMINGNODE_PORT"
}

install_client_driver() {
    client "mkdir -p '$CLIENT_RUN_DIR'"
    ssh "${SSH_OPTIONS[@]}" "$CLIENT_TARGET" "cat > '$CLIENT_DRIVER'" <<'PY'
#!/usr/bin/env python3
import argparse
import hashlib
import json
import math
import resource
import struct
import time

from pymilvus import Collection, connections, utility
from vectordb_bench.backend.dataset import Dataset


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


def write_json(path, value):
    with open(path, "w", encoding="utf-8") as output:
        json.dump(value, output, indent=2, sort_keys=True)


def manager():
    value = Dataset.COHERE.manager(1_000_000)
    if not value.prepare(with_train_files=False):
        raise RuntimeError("Cohere 1M test-data preparation returned false")
    return value


def query_vectors(count):
    data = manager().test_data
    if len(data) < count:
        raise RuntimeError(f"dataset has {len(data)} query vectors, expected at least {count}")
    return [[float(value) for value in data[index]] for index in range(count)]


def query_vector_sha256(vectors):
    digest = hashlib.sha256()
    for vector in vectors:
        for value in vector:
            digest.update(struct.pack("<f", value))
    return digest.hexdigest()


def search_once(collection, vector, args, include_values=False):
    iterator = collection.search_iterator(
        data=[vector],
        anns_field="vector",
        param={"metric_type": "COSINE", "params": {"ef": args.ef}},
        batch_size=args.topk,
        limit=args.topk,
        ignore_growing=True,
    )
    ids = []
    scores = []
    try:
        while True:
            page = iterator.next()
            if not page:
                break
            ids.extend(int(hit.id) for hit in page)
            scores.extend(float(hit.distance) for hit in page)
    finally:
        iterator.close()
    if len(ids) != args.topk:
        raise AssertionError(f"iterator returned {len(ids)} results, expected {args.topk}")

    digest = hashlib.sha256()
    for identifier in ids:
        digest.update(struct.pack("<q", identifier))
    result = {"count": len(ids), "sha256": digest.hexdigest()}
    if include_values:
        result.update({"ids": ids, "scores": scores})
    return result


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


def release(args):
    connect(args)
    Collection(args.collection).release(timeout=1800)
    deadline = time.monotonic() + 1800
    while utility.get_query_segment_info(args.collection, timeout=30):
        if time.monotonic() >= deadline:
            raise TimeoutError("segment placement remained after release")
        time.sleep(5)


def load(args):
    connect(args)
    collection = Collection(args.collection)
    collection.load(replica_number=1, timeout=7200)
    utility.wait_for_loading_complete(args.collection, timeout=7200)
    write_json(args.output, utility.loading_progress(args.collection))


def verify(args):
    connect(args)
    collection = Collection(args.collection)
    vectors = query_vectors(args.query_count)
    started = time.monotonic()
    queries = [
        {"queryIndex": index, **search_once(collection, vector, args, True)}
        for index, vector in enumerate(vectors)
    ]
    write_json(args.output, {
        "mode": args.mode,
        "topk": args.topk,
        "ef": args.ef,
        "queryVectorSha256": query_vector_sha256(vectors),
        "queries": queries,
        "elapsedSeconds": time.monotonic() - started,
    })


def percentile(values, percentage):
    ordered = sorted(values)
    rank = max(0, math.ceil(percentage * len(ordered)) - 1)
    return ordered[rank]


def benchmark(args):
    connect(args)
    collection = Collection(args.collection)
    vectors = query_vectors(args.query_count)
    for index in range(args.warmup_operations):
        search_once(collection, vectors[index % len(vectors)], args)

    operations = []
    started_ns = time.monotonic_ns()
    while True:
        query_index = len(operations) % len(vectors)
        operation_started_ns = time.monotonic_ns()
        result = search_once(collection, vectors[query_index], args)
        operations.append({
            "queryIndex": query_index,
            "latencyNs": time.monotonic_ns() - operation_started_ns,
            **result,
        })
        elapsed_seconds = (time.monotonic_ns() - started_ns) / 1_000_000_000
        if len(operations) >= args.min_operations and elapsed_seconds >= args.min_duration_seconds:
            break

    elapsed_seconds = (time.monotonic_ns() - started_ns) / 1_000_000_000
    latencies = [item["latencyNs"] for item in operations]
    usage = resource.getrusage(resource.RUSAGE_SELF)
    write_json(args.output, {
        "case": args.case,
        "mode": args.mode,
        "repetition": args.repetition,
        "topk": args.topk,
        "ef": args.ef,
        "chunkSize": args.chunk_size,
        "concurrency": 1,
        "queryVectorSha256": query_vector_sha256(vectors),
        "warmupOperations": args.warmup_operations,
        "successfulOperations": len(operations),
        "errors": 0,
        "elapsedSeconds": elapsed_seconds,
        "qps": len(operations) / elapsed_seconds,
        "latencyNs": {
            "p50": percentile(latencies, 0.50),
            "p95": percentile(latencies, 0.95),
            "p99": percentile(latencies, 0.99),
            "max": max(latencies),
        },
        "clientUserCpuSeconds": usage.ru_utime,
        "clientSystemCpuSeconds": usage.ru_stime,
        "clientMaxRssKiB": usage.ru_maxrss,
        "operations": operations,
    })


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=["inspect", "release", "load", "verify", "benchmark"])
    parser.add_argument("--host", default="10.15.9.42")
    parser.add_argument("--port", default="19532")
    parser.add_argument("--collection", default="cohere_1m_qn_fanin")
    parser.add_argument("--output", required=True)
    parser.add_argument("--mode", choices=["batch", "streaming"], default="batch")
    parser.add_argument("--case", default="qualification")
    parser.add_argument("--repetition", type=int, default=0)
    parser.add_argument("--topk", type=int, default=8192)
    parser.add_argument("--ef", type=int, default=8192)
    parser.add_argument("--chunk-size", type=int, default=1024)
    parser.add_argument("--query-count", type=int, default=100)
    parser.add_argument("--warmup-operations", type=int, default=20)
    parser.add_argument("--min-operations", type=int, default=100)
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

run_client_action() {
    local log_name=$1
    shift
    client "cd '$CLIENT_REPO' && PYTHONPATH='$CLIENT_REPO' \
        .venv/bin/python '$CLIENT_DRIVER' $*" \
        2>&1 | tee "$RUN_DIR/$log_name.log"
}

capture_placement() {
    local output=$1
    ssh "${SSH_OPTIONS[@]}" "$SERVER_TARGET" \
        python3 - "$SERVER_RUN_DIR/logs" <<'PY' >"$output"
import glob
import json
import re
import sys

records = []
for path in sorted(glob.glob(f"{sys.argv[1]}/querynode_*.log")):
    node_id = None
    segments = {}
    with open(path, encoding="utf-8", errors="replace") as source:
        for line in source:
            node_match = re.search(r'\["QueryNode init session"\].*\[nodeID=(\d+)\]', line)
            if node_match:
                node_id = int(node_match.group(1))
            segment_match = re.search(r"Successfully loaded segment (\d+) with (\d+) rows", line)
            if segment_match:
                segments[int(segment_match.group(1))] = int(segment_match.group(2))
    if node_id is None:
        raise SystemExit(f"QueryNode ID not found in {path}")
    for segment_id, num_rows in segments.items():
        records.append({
            "segmentID": segment_id,
            "numRows": num_rows,
            "nodeID": node_id,
            "nodeIds": [node_id],
            "state": "Sealed",
        })
records.sort(key=lambda item: (item["segmentID"], item["nodeID"]))
json.dump(records, sys.stdout, indent=2)
PY
}

validate_placement() {
    local case_name=$1
    local expected_nodes=$2
    local case_dir=$3

    python3 - "$case_name" "$expected_nodes" "$case_dir" \
        "$RUN_DIR/fixed-segments.json" <<'PY'
import json
import pathlib
import sys

case_name = sys.argv[1]
expected_nodes = int(sys.argv[2])
case_dir = pathlib.Path(sys.argv[3])
baseline = [tuple(item) for item in json.load(open(sys.argv[4], encoding="utf-8"))]
snapshots = [
    json.load(open(case_dir / f"placement-{index}.json", encoding="utf-8"))
    for index in range(1, 4)
]
if snapshots[0] != snapshots[1] or snapshots[1] != snapshots[2]:
    raise SystemExit("placement was not identical across three observations")
records = snapshots[0]
fixed = sorted({item["segmentID"]: item["numRows"] for item in records}.items())
if fixed != baseline:
    raise SystemExit("segment IDs or row counts differ from test_0915_01")

rows_by_node = {}
for item in records:
    if item["state"] != "Sealed" or len(item["nodeIds"]) != 1:
        raise SystemExit(f"invalid placement record: {item}")
    node_id = str(item["nodeIds"][0])
    rows_by_node[node_id] = rows_by_node.get(node_id, 0) + item["numRows"]
if len(rows_by_node) != expected_nodes:
    raise SystemExit(f"observed {len(rows_by_node)} QueryNodes, expected {expected_nodes}")

summary = {
    "case": case_name,
    "expectedQueryNodes": expected_nodes,
    "observedQueryNodeIDs": sorted(int(value) for value in rows_by_node),
    "rowsByQueryNode": rows_by_node,
    "sealedSegments": len(fixed),
    "minimumRowsPerQueryNode": min(rows_by_node.values()),
    "maximumRowsPerQueryNode": max(rows_by_node.values()),
}
json.dump(summary, open(case_dir / "placement-summary.json", "w", encoding="utf-8"), indent=2, sort_keys=True)
print(json.dumps(summary, sort_keys=True))
PY
}

validate_worknodes() {
    local case_dir=$1
    local expected_nodes=$2
    python3 - "$case_dir/worknodes.log" "$case_dir/placement-summary.json" \
        "$expected_nodes" <<'PY'
import json
import re
import sys

line = open(sys.argv[1], encoding="utf-8").read().strip()
summary = json.load(open(sys.argv[2], encoding="utf-8"))
expected = int(sys.argv[3])
ids_match = re.search(r'queryNodeIDs="?(\[[^]]*\])"?', line)
count_match = re.search(r'workNodeCount=(\d+)', line)
streaming_match = re.search(r'streamingNodePresent=(true|false)', line)
if not ids_match or not count_match or not streaming_match:
    raise SystemExit(f"cannot parse WorkNode log: {line}")
ids = [int(value) for value in re.findall(r'\d+', ids_match.group(1))]
if sorted(ids) != summary["observedQueryNodeIDs"]:
    raise SystemExit("WorkNode IDs do not match placement")
if int(count_match.group(1)) != expected:
    raise SystemExit("WorkNode count does not match requested QueryNode count")
if streaming_match.group(1) != "false":
    raise SystemExit("sealed-only Search selected a StreamingNode")
PY
}

compare_correctness() {
    local case_dir=$1
    python3 - "$case_dir/batch-verify.json" "$case_dir/streaming-verify.json" \
        "$case_dir/correctness.json" <<'PY'
import json
import sys

batch = json.load(open(sys.argv[1], encoding="utf-8"))
streaming = json.load(open(sys.argv[2], encoding="utf-8"))
if len(batch["queries"]) != len(streaming["queries"]):
    raise SystemExit("verification query counts differ")
for batch_query, streaming_query in zip(batch["queries"], streaming["queries"]):
    if batch_query["count"] != 8192 or streaming_query["count"] != 8192:
        raise SystemExit("verification result count differs from 8192")
    if batch_query["ids"] != streaming_query["ids"]:
        raise SystemExit(f"ordered IDs differ for query {batch_query['queryIndex']}")
    for batch_score, streaming_score in zip(batch_query["scores"], streaming_query["scores"]):
        if abs(batch_score - streaming_score) > 1e-6:
            raise SystemExit(f"scores differ for query {batch_query['queryIndex']}")
json.dump({"queries": len(batch["queries"]), "topk": 8192, "status": "PASS"},
          open(sys.argv[3], "w", encoding="utf-8"), indent=2)
PY
}

start_sampler() {
    local sample_dir=$1
    server_bash "$sample_dir" "$SERVER_RUN_DIR" <<'REMOTE'
set -Eeuo pipefail
sample_dir=$1
run_dir=$2
mkdir -p "$sample_dir"
rm -f "$sample_dir/stop"
(
    printf 'timestamp,pid,role,pcpu,rss_kib,vsz_kib,elapsed_seconds\n' >"$sample_dir/processes.csv"
    while [[ ! -e "$sample_dir/stop" ]]; do
        timestamp=$(date -u +%Y-%m-%dT%H:%M:%SZ)
        for pid_file in "$run_dir"/pids/*.pid; do
            [[ -s "$pid_file" ]] || continue
            leader_pid=$(cat "$pid_file")
            role=$(basename "$pid_file" .pid)
            ps --sid "$leader_pid" -o pid=,pcpu=,rss=,vsz=,etimes= 2>/dev/null \
                | awk -v timestamp="$timestamp" -v role="$role" \
                    '{printf "%s,%s,%s,%s,%s,%s,%s\n", timestamp,$1,role,$2,$3,$4,$5}' \
                >>"$sample_dir/processes.csv" || true
        done
        sleep 1
    done
) >/dev/null 2>&1 &
echo $! >"$sample_dir/sampler.pid"
REMOTE
    ACTIVE_SAMPLER=$sample_dir
}

capture_host_snapshot() {
    local output=$1
    server "date -u; cat /proc/meminfo; cat /proc/net/dev; \
        ps -eo pid,pcpu,pmem,rss,vsz,etimes,comm,args --sort=-rss | head -n 80" \
        >"$output"
}

qualify_placement() {
    local case_name=$1
    local count=$2
    local case_dir=$3
    local observation

    capture_placement "$case_dir/placement-1.json"
    sleep 10
    capture_placement "$case_dir/placement-2.json"
    for ((observation = 3; observation <= 30; observation++)); do
        sleep 10
        capture_placement "$case_dir/placement-3.json"
        if cmp -s "$case_dir/placement-1.json" "$case_dir/placement-2.json" \
            && cmp -s "$case_dir/placement-2.json" "$case_dir/placement-3.json"; then
            break
        fi
        mv "$case_dir/placement-2.json" "$case_dir/placement-1.json"
        mv "$case_dir/placement-3.json" "$case_dir/placement-2.json"
    done
    validate_placement "$case_name" "$count" "$case_dir" \
        | tee "$case_dir/placement-validation.log"
}

qualify_worknodes() {
    local case_dir=$1
    local remote_case_dir=$2
    local count=$3

    start_proxy streaming debug
    run_client_action "$(basename "$case_dir")-worknodes" \
        "verify --host 10.15.9.42 --port '$MILVUS_PORT' \
        --collection '$COLLECTION_NAME' --mode streaming --query-count 1 \
        --topk '$TOP_K' --ef '$SEARCH_EF' --output '$remote_case_dir/worknodes-probe.json'"
    copy_from_client "$remote_case_dir/worknodes-probe.json" "$case_dir/worknodes-probe.json"
    server "grep 'query view work nodes selected' '$SERVER_RUN_DIR/logs/proxy.log' | tail -n 1" \
        >"$case_dir/worknodes.log"
    [[ -s "$case_dir/worknodes.log" ]] || fail "WorkNode evidence is empty"
    validate_worknodes "$case_dir" "$count"
}

run_correctness_gate() {
    local case_name=$1
    local case_dir=$2
    local remote_case_dir=$3
    local mode

    for mode in batch streaming; do
        start_proxy "$mode" info
        run_client_action "$case_name-$mode-verify" \
            "verify --host 10.15.9.42 --port '$MILVUS_PORT' \
            --collection '$COLLECTION_NAME' --mode '$mode' --query-count 10 \
            --topk '$TOP_K' --ef '$SEARCH_EF' --output '$remote_case_dir/$mode-verify.json'"
        copy_from_client "$remote_case_dir/$mode-verify.json" "$case_dir/$mode-verify.json"
    done
    compare_correctness "$case_dir"
}

run_interval() {
    local case_name=$1
    local repetition=$2
    local mode=$3
    local interval_dir="$RUN_DIR/$case_name/rep$repetition/$mode"
    local remote_interval_dir="$CLIENT_RUN_DIR/$case_name/rep$repetition/$mode"
    local remote_sample_dir="$SERVER_RUN_DIR/samples/$case_name/rep$repetition/$mode"

    mkdir -p "$interval_dir"
    client "mkdir -p '$remote_interval_dir'"
    start_proxy "$mode" info
    server "pid=\$(cat '$SERVER_RUN_DIR/pids/proxy.pid'); \
        tr '\0' '\n' < /proc/\$pid/environ | \
        grep -E '^(MILVUS_CONF_PROXYQUERYVIEW|PROXY_PORT=)' | sort" \
        >"$interval_dir/proxy-environment.txt"
    capture_host_snapshot "$interval_dir/server-before.txt"
    client "date -u; cat /proc/meminfo; cat /proc/net/dev" \
        >"$interval_dir/client-before.txt"
    start_sampler "$remote_sample_dir"

    run_client_action "$case_name-rep$repetition-$mode" \
        "benchmark --host 10.15.9.42 --port '$MILVUS_PORT' \
        --collection '$COLLECTION_NAME' --case '$case_name' --mode '$mode' \
        --repetition '$repetition' --topk '$TOP_K' --ef '$SEARCH_EF' \
        --chunk-size '$CHUNK_SIZE' --query-count 100 \
        --warmup-operations '$WARMUP_OPERATIONS' \
        --min-operations '$MIN_OPERATIONS' \
        --min-duration-seconds '$MIN_DURATION_SECONDS' \
        --output '$remote_interval_dir/benchmark.json'"

    stop_sampler
    copy_from_client "$remote_interval_dir/benchmark.json" "$interval_dir/benchmark.json"
    copy_from_server "$remote_sample_dir/processes.csv" "$interval_dir/processes.csv"
    copy_from_server "$SERVER_RUN_DIR/logs/proxy.log" "$interval_dir/proxy.log"
    capture_host_snapshot "$interval_dir/server-after.txt"
    client "date -u; cat /proc/meminfo; cat /proc/net/dev" \
        >"$interval_dir/client-after.txt"
}

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

    mkdir -p "$case_dir/server-logs"
    rsync -az -e "ssh -i '$SSH_KEY' -o BatchMode=yes" \
        "$SERVER_TARGET:$SERVER_RUN_DIR/logs/" "$case_dir/server-logs/"
    printf 'PASS\n' >"$case_dir/status.txt"
}

build_manifest() {
    python3 - "$RUN_DIR" <<'PY'
import csv
import json
import pathlib
import statistics
import sys

run_dir = pathlib.Path(sys.argv[1])
rows = []
for path in sorted(run_dir.glob("FANIN-N*/rep*/*/benchmark.json")):
    data = json.load(open(path, encoding="utf-8"))
    if data["errors"] != 0:
        raise SystemExit(f"{path} recorded request errors")
    if data["successfulOperations"] < 100 or data["elapsedSeconds"] < 60:
        raise SystemExit(f"{path} did not satisfy the measurement minimums")
    if any(item["count"] != 8192 for item in data["operations"]):
        raise SystemExit(f"{path} contains an incomplete result")
    rows.append({
        "case": data["case"],
        "repetition": data["repetition"],
        "mode": data["mode"],
        "operations": data["successfulOperations"],
        "elapsed_seconds": data["elapsedSeconds"],
        "qps": data["qps"],
        "p50_ms": data["latencyNs"]["p50"] / 1_000_000,
        "p95_ms": data["latencyNs"]["p95"] / 1_000_000,
        "p99_ms": data["latencyNs"]["p99"] / 1_000_000,
        "max_ms": data["latencyNs"]["max"] / 1_000_000,
        "path": str(path.relative_to(run_dir)),
    })
if len(rows) != 48:
    raise SystemExit(f"manifest found {len(rows)} intervals, expected 48")

query_vector_hashes = {
    json.load(open(path, encoding="utf-8"))["queryVectorSha256"]
    for path in run_dir.glob("FANIN-N*/rep*/*/benchmark.json")
}
if len(query_vector_hashes) != 1:
    raise SystemExit("measured intervals did not use one fixed query-vector set")

for case_dir in sorted(path for path in run_dir.glob("FANIN-N*") if path.is_dir()):
    digests = {"batch": {}, "streaming": {}}
    for path in sorted(case_dir.glob("rep*/*/benchmark.json")):
        data = json.load(open(path, encoding="utf-8"))
        mode_digests = digests[data["mode"]]
        for operation in data["operations"]:
            query_index = operation["queryIndex"]
            digest = operation["sha256"]
            previous = mode_digests.setdefault(query_index, digest)
            if previous != digest:
                raise SystemExit(f"{path} changed digest for query {query_index}")
    if set(digests["batch"]) != set(range(100)) or set(digests["streaming"]) != set(range(100)):
        raise SystemExit(f"{case_dir} did not measure all 100 query vectors in both modes")
    if digests["batch"] != digests["streaming"]:
        raise SystemExit(f"{case_dir} Batch and Streaming hashes differ")

with open(run_dir / "manifest.tsv", "w", encoding="utf-8", newline="") as output:
    writer = csv.DictWriter(output, fieldnames=rows[0].keys(), delimiter="\t")
    writer.writeheader()
    writer.writerows(rows)

summary = []
for case in sorted({row["case"] for row in rows}, key=lambda value: int(value.rsplit("N", 1)[1])):
    values = {mode: [row for row in rows if row["case"] == case and row["mode"] == mode]
              for mode in ("batch", "streaming")}
    batch_qps = statistics.median(row["qps"] for row in values["batch"])
    streaming_qps = statistics.median(row["qps"] for row in values["streaming"])
    summary.append({
        "case": case,
        "batchMedianQps": batch_qps,
        "streamingMedianQps": streaming_qps,
        "streamingToBatchQps": streaming_qps / batch_qps,
        "batchMedianP95Ms": statistics.median(row["p95_ms"] for row in values["batch"]),
        "streamingMedianP95Ms": statistics.median(row["p95_ms"] for row in values["streaming"]),
    })
json.dump(summary, open(run_dir / "summary.json", "w", encoding="utf-8"), indent=2)
PY
}

main() {
    log "Run ID: $RUN_ID"
    preflight
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

    build_manifest
    log "All Batch-versus-Streaming fan-in intervals passed"
}

main "$@"
