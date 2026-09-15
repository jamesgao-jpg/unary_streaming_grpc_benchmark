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

RUN_ID="${RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)-$$}"
RUN_DIR="$SCRIPT_DIR/runs/$RUN_ID"
SERVER_RUN_DIR="/home/ubuntu/reducestream-e2e/$RUN_ID"
CLIENT_RUN_DIR="/home/ubuntu/reducestream-e2e/$RUN_ID"
COLLECTION_NAME="${COLLECTION_NAME:-cohere_10m_qn_fanin}"

MILVUS_PORT=19532
MINIO_PORT=19000
MINIO_CONSOLE_PORT=19001
METRICS_BASE=19090
QUERYNODE_RPC_BASE=21123
QUERYNODE_METRICS_BASE=19200
STREAMINGNODE_PORT=22222

DATASET_ROWS=10000000
VECTOR_DIMENSION=768
TARGET_SEGMENTS=256
SEAL_PROPORTION=0.12
SEGMENT_MAX_SIZE_MIB=959
INSERT_BATCH_SIZE=10000
TOPOLOGIES=(1 2 4 8 16 32)

COMPOSE_PROJECT="qv-topology-$(printf '%s' "$RUN_ID" | tr '[:upper:]' '[:lower:]' | tr -cd '[:alnum:]-')"
CLUSTER_ID="qvtopology$(printf '%s' "$RUN_ID" | tr -cd '[:alnum:]')"
COMPOSE_FILE="$SERVER_REPO/deployments/docker/dev/docker-compose-apple-silicon.yml"
COMPOSE_OVERRIDE="$SERVER_RUN_DIR/docker-compose.override.yml"
CLIENT_DRIVER="$CLIENT_RUN_DIR/client_driver.py"
SERVER_ENV="$SERVER_RUN_DIR/milvus.env"

SSH_OPTIONS=(-i "$SSH_KEY" -o BatchMode=yes -o ConnectTimeout=15 -o ServerAliveInterval=30)
INFRA_STARTED=false

mkdir -p "$RUN_DIR"
exec > >(tee -a "$RUN_DIR/run.log") 2>&1

log() {
    printf '[test_0915_01] %s\n' "$*"
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

copy_from_client() {
    scp "${SSH_OPTIONS[@]}" "$CLIENT_TARGET:$1" "$2"
}

compose_remote() {
    server "COMPOSE_PROJECT_NAME='$COMPOSE_PROJECT' DOCKER_VOLUME_DIRECTORY='$SERVER_RUN_DIR/infra' \
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
    local log_level=info
    [[ "$role" == "proxy" ]] && log_level=debug

    log "Starting $name"
    server_bash \
        "$name" "$role" "$metrics_port" "$rpc_port" "$log_level" \
        "$SERVER_RUN_DIR" "$SERVER_REPO" "$SERVER_ENV" <<'REMOTE'
set -Eeuo pipefail
name=$1
role=$2
metrics_port=$3
rpc_port=$4
log_level=$5
run_dir=$6
repo=$7
env_file=$8

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

collect_server_evidence() {
    server "find '$SERVER_RUN_DIR' -maxdepth 2 -type f -printf '%p %s bytes\n' | sort" \
        >"$RUN_DIR/server-files.txt" 2>&1 || true
    server "grep -h 'query view work nodes selected' '$SERVER_RUN_DIR/logs/proxy.log' 2>/dev/null" \
        >"$RUN_DIR/all-worknodes.log" 2>&1 || true
    server_bash "$SERVER_RUN_DIR" <<'REMOTE' \
        >"$RUN_DIR/server-log-tails.txt" 2>&1 || true
set +e
run_dir=$1
for file in "$run_dir"/logs/*.log; do
    [[ -e "$file" ]] || continue
    echo "===== $file ====="
    tail -n 200 "$file"
done
REMOTE
}

cleanup() {
    local status=$?
    trap - EXIT INT TERM
    set +e

    collect_server_evidence
    stop_querynodes
    stop_base_roles
    if [[ "$INFRA_STARTED" == true ]]; then
        compose_remote down --remove-orphans >/dev/null 2>&1
    fi

    if ((status == 0)); then
        log "PASS. Evidence: $RUN_DIR"
    else
        log "FAILED with status $status. Evidence: $RUN_DIR"
        tail -n 120 "$RUN_DIR/server-log-tails.txt" 2>/dev/null || true
    fi
    exit "$status"
}

trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

calculate_segment_size() {
    python3 - \
        "$DATASET_ROWS" "$VECTOR_DIMENSION" "$TARGET_SEGMENTS" \
        "$SEAL_PROPORTION" "$RUN_DIR/segment-size.json" <<'PY'
import json
import math
import sys

rows = int(sys.argv[1])
dimension = int(sys.argv[2])
target_segments = int(sys.argv[3])
seal_proportion = float(sys.argv[4])
output = sys.argv[5]

bytes_per_row = 8 + 8 + dimension * 4
target_rows = math.ceil(rows / target_segments)
max_size_mib = math.ceil(
    target_rows * bytes_per_row / seal_proportion / 1_048_576
)
nominal_max_rows = math.floor(max_size_mib * 1_048_576 / bytes_per_row)
nominal_seal_rows = math.ceil(nominal_max_rows * seal_proportion)

result = {
    "datasetRows": rows,
    "vectorDimension": dimension,
    "targetSegments": target_segments,
    "bytesPerRow": bytes_per_row,
    "targetRowsPerSegment": target_rows,
    "sealProportion": seal_proportion,
    "segmentMaxSizeMiB": max_size_mib,
    "nominalMaximumRows": nominal_max_rows,
    "nominalSealRows": nominal_seal_rows,
    "nominalSegmentCount": math.ceil(rows / nominal_seal_rows),
}
with open(output, "w", encoding="utf-8") as f:
    json.dump(result, f, indent=2, sort_keys=True)
print(max_size_mib)
PY
}

preflight() {
    local command
    for command in git ssh scp python3; do
        command -v "$command" >/dev/null || fail "missing local command: $command"
    done
    [[ -r "$SSH_KEY" ]] || fail "SSH identity file is not readable: $SSH_KEY"
    [[ -d "$MILVUS_LOCAL_REPO/.git" ]] || fail "missing Milvus checkout: $MILVUS_LOCAL_REPO"

    local calculated_max_size
    calculated_max_size=$(calculate_segment_size)
    [[ "$calculated_max_size" -eq "$SEGMENT_MAX_SIZE_MIB" ]] \
        || fail "segment.maxSize calculation returned $calculated_max_size, expected $SEGMENT_MAX_SIZE_MIB"

    git -C "$MILVUS_LOCAL_REPO" status --short --branch >"$RUN_DIR/local-milvus-status.txt"
    git -C "$TEST_REPO" rev-parse HEAD >"$RUN_DIR/test-plan-commit.txt"
    git -C "$TEST_REPO" status --short --branch >"$RUN_DIR/test-plan-status.txt"
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

    server "uname -a; nproc; free -b; df -PB1 /home/ubuntu; \
        ps -eo pid,pcpu,pmem,rss,comm,args --sort=-rss | head -n 30; docker ps" \
        >"$RUN_DIR/server-preflight.txt"
    client "uname -a; nproc; free -b; df -PB1 /home/ubuntu; \
        ps -eo pid,pcpu,pmem,rss,comm,args --sort=-rss | head -n 30; docker ps" \
        >"$RUN_DIR/client-preflight.txt"

    local server_free client_free
    server_free=$(server "df -PB1 /home/ubuntu | awk 'NR == 2 {print \$4}'")
    client_free=$(client "df -PB1 /home/ubuntu | awk 'NR == 2 {print \$4}'")
    ((server_free >= 100 * 1024 * 1024 * 1024)) \
        || fail ".42 has less than 100 GiB free"
    ((client_free >= 40 * 1024 * 1024 * 1024)) \
        || fail ".233 has less than 40 GiB free"

    if server "pgrep -af '[m]ilvus run' >/dev/null || [[ -n \"\$(docker ps -q)\" ]]"; then
        fail ".42 has a running Milvus process or container"
    fi
    if client "pgrep -af '[l]ocust|[v]ectordb_bench' >/dev/null"; then
        fail ".233 has a running benchmark workload"
    fi

    log "Building Milvus on .42"
    server "export PATH=/home/ubuntu/.local/go1.26.5/bin:\$PATH GOPATH=/home/ubuntu/go; \
        cd '$SERVER_REPO' && make milvus" | tee "$RUN_DIR/remote-build.log"
    server "test -x '$SERVER_REPO/bin/milvus'"
    client "test -x '$CLIENT_REPO/.venv/bin/python'"
}

install_client_driver() {
    client "mkdir -p '$CLIENT_RUN_DIR'"
    ssh "${SSH_OPTIONS[@]}" "$CLIENT_TARGET" "cat > '$CLIENT_DRIVER'" <<'PY'
#!/usr/bin/env python3
import argparse
import hashlib
import json
import time

import pandas as pd
from pymilvus import (
    Collection,
    CollectionSchema,
    DataType,
    FieldSchema,
    connections,
    utility,
)
from pymilvus.grpc_gen import common_pb2
from vectordb_bench.backend.dataset import Dataset


def connect(args):
    deadline = time.monotonic() + 300
    while True:
        try:
            connections.connect(
                alias="default", host=args.host, port=args.port, timeout=10
            )
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


def dataset_manager(with_train_files):
    manager = Dataset.COHERE.manager(10_000_000)
    if not manager.prepare(with_train_files=with_train_files):
        raise RuntimeError("Cohere 10M preparation returned false")
    return manager


def download(args):
    manager = dataset_manager(True)
    files = []
    for path in sorted(manager.data_dir.rglob("*")):
        if not path.is_file():
            continue
        digest = hashlib.sha256()
        with path.open("rb") as source:
            for block in iter(lambda: source.read(8 * 1024 * 1024), b""):
                digest.update(block)
        files.append(
            {
                "path": str(path),
                "bytes": path.stat().st_size,
                "sha256": digest.hexdigest(),
            }
        )
    write_json(args.output, {"datasetDir": str(manager.data_dir), "files": files})


def prepare(args):
    manager = dataset_manager(True)
    connect(args)
    if utility.has_collection(args.collection):
        utility.drop_collection(args.collection)

    schema = CollectionSchema(
        fields=[
            FieldSchema("pk", DataType.INT64, is_primary=True, auto_id=False),
            FieldSchema("id", DataType.INT64),
            FieldSchema("vector", DataType.FLOAT_VECTOR, dim=768),
        ],
        description="Cohere 10M QueryNode fan-in qualification",
    )
    collection = Collection(
        args.collection,
        schema=schema,
        shards_num=1,
        consistency_level="Strong",
    )

    inserted = 0
    started = time.monotonic()
    for batch in manager.iter_batches(args.insert_batch_size):
        ids = batch["id"].astype("int64", copy=False)
        insert_batch = pd.DataFrame(
            {"pk": ids, "id": ids, "vector": batch["emb"]}
        )
        collection.insert(insert_batch, timeout=300)
        inserted += len(insert_batch)
        if inserted % 100_000 == 0 or inserted == args.expected_rows:
            print(f"inserted_rows={inserted}", flush=True)

    if inserted != args.expected_rows:
        raise AssertionError(f"inserted {inserted} rows, expected {args.expected_rows}")
    insert_seconds = time.monotonic() - started

    collection.flush(timeout=7200)
    collection.create_index(
        "vector",
        {
            "index_type": "HNSW",
            "metric_type": "COSINE",
            "params": {"M": 16, "efConstruction": 200},
        },
        timeout=43200,
    )
    utility.wait_for_index_building_complete(
        args.collection, timeout=43200
    )
    write_json(
        args.output,
        {
            "collection": args.collection,
            "insertedRows": inserted,
            "insertSeconds": insert_seconds,
            "shards": 1,
            "index": {
                "index_type": "HNSW",
                "metric_type": "COSINE",
                "M": 16,
                "efConstruction": 200,
            },
        },
    )


def release(args):
    connect(args)
    collection = Collection(args.collection)
    collection.release(timeout=1800)
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


def snapshot(args):
    connect(args)
    infos = utility.get_query_segment_info(args.collection, timeout=60)
    records = []
    for info in infos:
        records.append(
            {
                "segmentID": info.segmentID,
                "numRows": info.num_rows,
                "memSize": info.mem_size,
                "nodeID": info.nodeID,
                "nodeIds": list(info.nodeIds),
                "state": common_pb2.SegmentState.Name(info.state),
            }
        )
    records.sort(key=lambda item: (item["segmentID"], item["nodeID"]))
    write_json(args.output, records)


def iterator(args):
    manager = dataset_manager(False)
    connect(args)
    collection = Collection(args.collection)
    search_iterator = collection.search_iterator(
        data=[[float(value) for value in manager.test_data[0]]],
        anns_field="vector",
        param={"metric_type": "COSINE", "params": {"ef": 128}},
        batch_size=10,
        limit=10,
        ignore_growing=True,
    )
    ids = []
    try:
        while True:
            page = search_iterator.next()
            if not page:
                break
            ids.extend(hit.id for hit in page)
    finally:
        search_iterator.close()
    if len(ids) != 10:
        raise AssertionError(f"iterator returned {len(ids)} results, expected 10")
    write_json(args.output, {"count": len(ids), "ids": ids})


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=["download", "prepare", "release", "load", "snapshot", "iterator"])
    parser.add_argument("--host", default="10.15.9.42")
    parser.add_argument("--port", default="19532")
    parser.add_argument("--collection", default="cohere_10m_qn_fanin")
    parser.add_argument("--output")
    parser.add_argument("--expected-rows", type=int, default=10_000_000)
    parser.add_argument("--insert-batch-size", type=int, default=10_000)
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

write_server_configuration() {
    server "mkdir -p '$SERVER_RUN_DIR/logs' '$SERVER_RUN_DIR/pids' \
        '$SERVER_RUN_DIR/infra' '$SERVER_RUN_DIR/local'"

    ssh "${SSH_OPTIONS[@]}" "$SERVER_TARGET" "cat > '$SERVER_ENV'" <<EOF
ETCD_ENDPOINTS=127.0.0.1:2379
MINIO_ADDRESS=127.0.0.1:$MINIO_PORT
PULSAR_ADDRESS=pulsar://127.0.0.1:6650
MQ_TYPE=pulsar
ETCD_ROOTPATH=$CLUSTER_ID
MINIO_ROOTPATH=$CLUSTER_ID
MSGCHANNEL_CHANNAMEPREFIX_CLUSTER=$CLUSTER_ID
PROXY_PORT=$MILVUS_PORT
MILVUS_CONF_PROXYQUERYVIEWENABLESEARCHSTREAMING=true
MILVUS_CONF_PROXYQUERYVIEWSEARCHSTREAMCHUNKSIZE=1024
MILVUS_CONF_DATACOORDSEGMENTMAXSIZE=$SEGMENT_MAX_SIZE_MIB
MILVUS_CONF_DATACOORDSEGMENTSEALPROPORTION=$SEAL_PROPORTION
MILVUS_CONF_DATACOORDSEGMENTSEALPROPORTIONJITTER=0
MILVUS_CONF_DATACOORDENABLECOMPACTION=false
MILVUS_CONF_DATACOORDCOMPACTIONENABLEAUTOCOMPACTION=false
EOF

    ssh "${SSH_OPTIONS[@]}" "$SERVER_TARGET" "cat > '$COMPOSE_OVERRIDE'" <<EOF
services:
  minio:
    ports: !override
      - "127.0.0.1:${MINIO_PORT}:9000"
      - "127.0.0.1:${MINIO_CONSOLE_PORT}:9001"
EOF
}

start_infrastructure() {
    log "Starting etcd, Pulsar, and MinIO"
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

    start_role proxy proxy "$((METRICS_BASE + 4))"
    wait_for_tcp 10.15.9.42 "$MILVUS_PORT" 300

    local mixcoord_pid
    mixcoord_pid=$(server "cat '$SERVER_RUN_DIR/pids/mixcoord.pid'")
    server "tr '\\0' '\\n' < '/proc/$mixcoord_pid/environ' | \
        grep -E '^(MILVUS_CONF_DATACOORD|ETCD_ROOTPATH|MINIO_ROOTPATH)' | sort" \
        >"$RUN_DIR/running-configuration.txt"
}

run_client_action() {
    local log_name=$1
    shift
    client "cd '$CLIENT_REPO' && PYTHONPATH='$CLIENT_REPO' \
        .venv/bin/python '$CLIENT_DRIVER' $*" \
        2>&1 | tee "$RUN_DIR/$log_name.log"
}

validate_placement() {
    local case_name=$1
    local expected_nodes=$2
    local case_dir=$3

    python3 - \
        "$case_name" "$expected_nodes" "$case_dir" \
        "$RUN_DIR/fixed-segments.json" <<'PY'
import json
import pathlib
import sys

case_name = sys.argv[1]
expected_nodes = int(sys.argv[2])
case_dir = pathlib.Path(sys.argv[3])
baseline_path = pathlib.Path(sys.argv[4])
snapshots = []
for path in sorted(case_dir.glob("placement-*.json")):
    with path.open(encoding="utf-8") as source:
        snapshots.append(json.load(source))
if len(snapshots) != 3 or snapshots[0] != snapshots[1] or snapshots[1] != snapshots[2]:
    raise SystemExit("placement was not identical across three observations")

records = snapshots[0]
if not records:
    raise SystemExit("placement snapshot is empty")
if any(record["state"] != "Sealed" for record in records):
    raise SystemExit("placement contains a non-sealed segment")

fixed_segments = sorted(
    {record["segmentID"]: record["numRows"] for record in records}.items()
)
if case_name == "N1":
    if not 240 <= len(fixed_segments) <= 272:
        raise SystemExit(
            f"observed {len(fixed_segments)} segments, expected 240..272"
        )
    with baseline_path.open("w", encoding="utf-8") as output:
        json.dump(fixed_segments, output, indent=2)
else:
    with baseline_path.open(encoding="utf-8") as source:
        baseline = json.load(source)
    if fixed_segments != [tuple(item) for item in baseline]:
        raise SystemExit("fixed segment IDs or row counts changed")

rows_by_node = {}
for record in records:
    node_ids = record["nodeIds"] or [record["nodeID"]]
    node_ids = sorted(set(node_ids))
    if len(node_ids) != 1:
        raise SystemExit(
            f"segment {record['segmentID']} has unexpected node IDs {node_ids}"
        )
    node_id = str(node_ids[0])
    rows_by_node[node_id] = rows_by_node.get(node_id, 0) + record["numRows"]

if len(rows_by_node) != expected_nodes:
    raise SystemExit(
        f"observed {len(rows_by_node)} QueryNodes, expected {expected_nodes}"
    )
row_counts = list(rows_by_node.values())
largest_segment = max(value for _, value in fixed_segments)
if max(row_counts) - min(row_counts) > largest_segment:
    raise SystemExit("row imbalance exceeds one largest sealed segment")

summary = {
    "case": case_name,
    "expectedQueryNodes": expected_nodes,
    "observedQueryNodeIDs": sorted(int(value) for value in rows_by_node),
    "rowsByQueryNode": rows_by_node,
    "sealedSegments": len(fixed_segments),
    "largestSegmentRows": largest_segment,
    "rowImbalance": max(row_counts) - min(row_counts),
}
with (case_dir / "placement-summary.json").open("w", encoding="utf-8") as output:
    json.dump(summary, output, indent=2, sort_keys=True)
print(json.dumps(summary, sort_keys=True))
PY
}

validate_worknodes() {
    local case_dir=$1
    local expected_nodes=$2

    python3 - \
        "$case_dir/worknodes.log" "$case_dir/placement-summary.json" \
        "$expected_nodes" <<'PY'
import json
import re
import sys

line = open(sys.argv[1], encoding="utf-8").read().strip()
summary = json.load(open(sys.argv[2], encoding="utf-8"))
expected_count = int(sys.argv[3])

ids_match = re.search(r'queryNodeIDs="?(\[[^]]*\])"?', line)
count_match = re.search(r'workNodeCount=(\d+)', line)
streaming_match = re.search(r'streamingNodePresent=(true|false)', line)
if not ids_match or not count_match or not streaming_match:
    raise SystemExit(f"cannot parse WorkNode log: {line}")

query_node_ids = [int(value) for value in re.findall(r'\d+', ids_match.group(1))]
if sorted(query_node_ids) != summary["observedQueryNodeIDs"]:
    raise SystemExit(
        f"WorkNode IDs {query_node_ids} do not match placement "
        f"{summary['observedQueryNodeIDs']}"
    )
if int(count_match.group(1)) != expected_count:
    raise SystemExit("WorkNode count does not match requested QueryNode count")
if streaming_match.group(1) != "false":
    raise SystemExit("sealed-only Search unexpectedly selected a StreamingNode")
PY
}

qualify_topology() {
    local count=$1
    local case_name="N$count"
    local case_dir="$RUN_DIR/$case_name"
    local remote_case_dir="$CLIENT_RUN_DIR/$case_name"
    local observation

    mkdir -p "$case_dir"
    client "mkdir -p '$remote_case_dir'"

    run_client_action "$case_name-load" \
        "load --host 10.15.9.42 --port '$MILVUS_PORT' \
        --collection '$COLLECTION_NAME' --output '$remote_case_dir/load.json'"
    copy_from_client "$remote_case_dir/load.json" "$case_dir/load.json"

    for observation in 1 2 3; do
        run_client_action "$case_name-placement-$observation" \
            "snapshot --host 10.15.9.42 --port '$MILVUS_PORT' \
            --collection '$COLLECTION_NAME' \
            --output '$remote_case_dir/placement-$observation.json'"
        copy_from_client \
            "$remote_case_dir/placement-$observation.json" \
            "$case_dir/placement-$observation.json"
        if ((observation < 3)); then
            sleep 10
        fi
    done
    validate_placement "$case_name" "$count" "$case_dir" \
        | tee "$case_dir/placement-validation.log"

    local before_line
    before_line=$(server "wc -l < '$SERVER_RUN_DIR/logs/proxy.log'")
    run_client_action "$case_name-iterator" \
        "iterator --host 10.15.9.42 --port '$MILVUS_PORT' \
        --collection '$COLLECTION_NAME' --output '$remote_case_dir/iterator.json'"
    copy_from_client "$remote_case_dir/iterator.json" "$case_dir/iterator.json"
    server "tail -n +$((before_line + 1)) '$SERVER_RUN_DIR/logs/proxy.log' | \
        grep 'query view work nodes selected' | tail -n 1" >"$case_dir/worknodes.log"
    [[ -s "$case_dir/worknodes.log" ]] \
        || fail "$case_name did not emit WorkNode evidence"
    validate_worknodes "$case_dir" "$count"
    printf 'PASS\n' >"$case_dir/status.txt"
}

main() {
    log "Run ID: $RUN_ID"
    preflight
    install_client_driver
    write_server_configuration
    start_infrastructure
    start_base_roles
    start_querynodes 1

    log "Downloading and checksumming Cohere 10M on .233"
    run_client_action dataset \
        "download --output '$CLIENT_RUN_DIR/dataset.json'"
    copy_from_client "$CLIENT_RUN_DIR/dataset.json" "$RUN_DIR/dataset.json"

    log "Creating the fixed one-vchannel collection and HNSW index"
    run_client_action prepare \
        "prepare --host 10.15.9.42 --port '$MILVUS_PORT' \
        --collection '$COLLECTION_NAME' --expected-rows '$DATASET_ROWS' \
        --insert-batch-size '$INSERT_BATCH_SIZE' \
        --output '$CLIENT_RUN_DIR/prepare.json'"
    copy_from_client "$CLIENT_RUN_DIR/prepare.json" "$RUN_DIR/prepare.json"

    local count
    for count in "${TOPOLOGIES[@]}"; do
        log "Qualifying N$count"
        if ((count != 1)); then
            run_client_action "N$count-release" \
                "release --host 10.15.9.42 --port '$MILVUS_PORT' \
                --collection '$COLLECTION_NAME'"
            stop_querynodes
            start_querynodes "$count"
        fi
        qualify_topology "$count"
    done

    log "All requested QueryNode fan-in topologies passed"
}

main "$@"
