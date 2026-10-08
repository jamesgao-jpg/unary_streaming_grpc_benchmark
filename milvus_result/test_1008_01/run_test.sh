#!/usr/bin/env bash
# Runs Stage 2 ReduceStream failure qualification against real Milvus processes.

set -Eeuo pipefail

FAILURE_TEST_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$FAILURE_TEST_DIR/../test_0915_02/run_test.sh"

SCRIPT_DIR="$FAILURE_TEST_DIR"
TEST_REPO="$(cd "$SCRIPT_DIR/../.." && pwd)"
WORKSPACE_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
MILVUS_LOCAL_REPO="${MILVUS_LOCAL_REPO:-$WORKSPACE_ROOT/milvus-qv}"

RUN_ID="${RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)-$$}"
RUN_DIR="$SCRIPT_DIR/runs/$RUN_ID"
SERVER_RUN_DIR="/home/ubuntu/reducestream-e2e/$RUN_ID"
CLIENT_RUN_DIR="/home/ubuntu/reducestream-e2e/$RUN_ID"
COMPOSE_PROJECT="qv-failure-$(printf '%s' "$RUN_ID" | tr '[:upper:]' '[:lower:]' | tr -cd '[:alnum:]-')"
COMPOSE_OVERRIDE="$SERVER_RUN_DIR/docker-compose.override.yml"
SERVER_ENV="$SERVER_RUN_DIR/milvus.env"
CLIENT_DRIVER="$CLIENT_RUN_DIR/failure_driver.py"

CHUNK_BYTES=16384
TOP_K=8192
QUERY_LIMIT=8192
SEARCH_EF=8192
REQUEST_TIMEOUT_SECONDS=60
DEADLINE_SECONDS=5
QUERYNODE_FAULT=""
CLIENT_REQUEST_PID=""
CLIENT_REQUEST_OUTPUT=""

log() {
    printf '[test_1008_01] %s\n' "$*"
}

cleanup() {
    local status=$?
    trap - EXIT INT TERM
    set +e

    if [[ -n "$CLIENT_REQUEST_PID" ]]; then
        kill "$CLIENT_REQUEST_PID" 2>/dev/null || true
        wait "$CLIENT_REQUEST_PID" 2>/dev/null || true
    fi
    client "pkill -f '[f]ailure_driver.py' 2>/dev/null || true"
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

# start_role starts a native Milvus role and applies a QueryNode fault only when requested.
start_role() {
    local name=$1 role=$2 metrics_port=$3 rpc_port=${4:-0}
    local log_level=${5:-info}
    local fault=""
    [[ "$role" == querynode ]] && fault=$QUERYNODE_FAULT
    local fault_arg=${fault:-unused}

    log "Starting $name, fault=${fault:-none}"
    server_bash \
        "$name" "$role" "$metrics_port" "$rpc_port" "$log_level" \
        "$fault_arg" "$SERVER_RUN_DIR" "$SERVER_REPO" "$SERVER_ENV" <<'REMOTE'
set -Eeuo pipefail
name=$1
role=$2
metrics_port=$3
rpc_port=$4
log_level=$5
fault=$6
run_dir=$7
repo=$8
env_file=$9
[[ "$fault" == unused ]] && fault=""

mkdir -p "$run_dir/logs" "$run_dir/pids" "$run_dir/local/$name"
(
    set -a
    source "$env_file"
    set +a
    export METRICS_PORT="$metrics_port"
    export LOCALSTORAGE_PATH="$run_dir/local/$name"
    export MILVUS_CONF_LOGLEVEL="$log_level"
    export MILVUS_CONF_LOGFORMAT=text
    if [[ "$role" == querynode ]]; then
        export MILVUS_CONF_QUERYNODE_PORT="$rpc_port"
        if [[ -n "$fault" ]]; then
            export MILVUS_REDUCE_STREAM_FAULT="$fault"
        fi
    elif [[ "$role" == streamingnode ]]; then
        export MILVUS_CONF_STREAMINGNODE_PORT="$rpc_port"
    elif [[ "$role" == proxy ]]; then
        export MILVUS_CONF_PROXYQUERYVIEWENABLESEARCHSTREAMING=true
        export MILVUS_CONF_PROXYQUERYVIEWENABLEQUERYSTREAMING=true
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
    stop_pid_file "$SERVER_RUN_DIR/pids/proxy.pid"
    start_role proxy proxy "$((METRICS_BASE + 4))" 0 debug
    wait_for_tcp 10.15.9.42 "$MILVUS_PORT" 300
}

preflight() {
    local command
    for command in git ssh scp rsync python3; do
        command -v "$command" >/dev/null || fail "missing local command: $command"
    done
    [[ -r "$SSH_KEY" ]] || fail "SSH identity file is not readable: $SSH_KEY"
    [[ -d "$MILVUS_LOCAL_REPO/.git" ]] || fail "missing Milvus checkout: $MILVUS_LOCAL_REPO"
    [[ -s "$SOURCE_LOCAL_RUN_DIR/N1/placement-3.json" ]] \
        || fail "missing accepted N1 topology evidence: $SOURCE_LOCAL_RUN_DIR"

    cp "$SCRIPT_DIR/TEST_PLAN.md" "$RUN_DIR/TEST_PLAN.md"
    cp "$SCRIPT_DIR/run_test.sh" "$RUN_DIR/run_test.sh"
    git -C "$MILVUS_LOCAL_REPO" status --short --branch >"$RUN_DIR/local-milvus-status.txt"
    git -C "$TEST_REPO" status --short --branch >"$RUN_DIR/test-repo-status.txt"
    git -C "$TEST_REPO" rev-parse HEAD >"$RUN_DIR/test-repo-commit.txt"
    git -C "$TEST_REPO" diff >"$RUN_DIR/test-repo.diff"

    local local_head remote_head
    local_head=$(git -C "$MILVUS_LOCAL_REPO" rev-parse HEAD)
    remote_head=$(server "git -C '$SERVER_REPO' rev-parse HEAD")
    printf '%s\n' "$local_head" >"$RUN_DIR/local-milvus-commit.txt"
    printf '%s\n' "$remote_head" >"$RUN_DIR/remote-milvus-commit.txt"
    server "git -C '$SERVER_REPO' status --short --branch" >"$RUN_DIR/remote-milvus-status.txt"
    [[ "$local_head" == "$remote_head" ]] \
        || fail "local Milvus HEAD differs from .42 HEAD"
    local dirty_source
    dirty_source=$(git -C "$MILVUS_LOCAL_REPO" status --porcelain --untracked-files=all \
        | grep -vE '^.. docs/' || true)
    [[ -z "$dirty_source" ]] \
        || fail "local Milvus source checkout is not clean: $dirty_source"
    [[ -z "$(server "git -C '$SERVER_REPO' status --porcelain")" ]] \
        || fail "remote Milvus checkout is not clean"

    server "test -d '$SOURCE_SERVER_RUN_DIR/infra/volumes' && \
        test -s '$SOURCE_SERVER_RUN_DIR/milvus.env' && \
        grep -q 'MILVUS_REDUCE_STREAM_FAULT' \
          '$SERVER_REPO/internal/views/viewquery/server.go'"
    client "test -x '$CLIENT_REPO/.venv/bin/python'"
    client "cd '$CLIENT_REPO' && .venv/bin/python -c \
        'import pymilvus; print(pymilvus.__version__)'" \
        >"$RUN_DIR/pymilvus-version.txt"
    server "sudo -n ss -K 'sport = :1' >/dev/null"

    server "uname -a; nproc; free -b; df -PB1 /home/ubuntu; \
        ps -eo pid,pcpu,pmem,rss,comm,args --sort=-rss | head -n 30; docker ps" \
        >"$RUN_DIR/server-preflight.txt"
    client "uname -a; nproc; free -b; df -PB1 /home/ubuntu; \
        ps -eo pid,pcpu,pmem,rss,comm,args --sort=-rss | head -n 30" \
        >"$RUN_DIR/client-preflight.txt"
    if server "pgrep -af '[m]ilvus run' >/dev/null || [[ -n \"\$(docker ps -q)\" ]]"; then
        fail ".42 has a running Milvus process or container"
    fi
    if client "pgrep -af '[f]ailure_driver.py|[l]ocust|[v]ectordb_bench' >/dev/null"; then
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
    server "cp '$SOURCE_SERVER_RUN_DIR/milvus.env' '$SERVER_ENV'; \
        sed -i \
          -e '/^MILVUS_CONF_PROXYQUERYVIEWENABLESEARCHSTREAMING=/d' \
          -e '/^MILVUS_CONF_PROXYQUERYVIEWENABLEQUERYSTREAMING=/d' \
          -e '/^MILVUS_CONF_PROXYQUERYVIEWSEARCHSTREAMCHUNKBYTES=/d' \
          -e '/^MILVUS_CONF_PROXYQUERYVIEWQUERYSTREAMCHUNKBYTES=/d' \
          -e '/^MILVUS_CONF_QUERYCOORDQUERYVIEWTARGETROWSPERSHARDNODE=/d' \
          '$SERVER_ENV'; \
        printf '%s\n' \
          'MILVUS_CONF_PROXYQUERYVIEWSEARCHSTREAMCHUNKBYTES=$CHUNK_BYTES' \
          'MILVUS_CONF_PROXYQUERYVIEWQUERYSTREAMCHUNKBYTES=$CHUNK_BYTES' \
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

install_client_driver() {
    client "mkdir -p '$CLIENT_RUN_DIR'"
    ssh "${SSH_OPTIONS[@]}" "$CLIENT_TARGET" "cat > '$CLIENT_DRIVER'" <<'PY'
#!/usr/bin/env python3
import argparse
import hashlib
import json
import struct
import time

from pymilvus import Collection, connections, utility
from vectordb_bench.backend.dataset import Dataset


def connect(args):
    connections.connect(alias="default", host=args.host, port=args.port, timeout=10)


def query_vector():
    manager = Dataset.COHERE.manager(1_000_000)
    if not manager.prepare(with_train_files=False):
        raise RuntimeError("Cohere 1M test-data preparation returned false")
    return [float(value) for value in manager.test_data[0]]


def digest_ids(ids):
    digest = hashlib.sha256()
    for identifier in ids:
        digest.update(struct.pack("<q", int(identifier)))
    return digest.hexdigest()


def run_search(collection, args):
    iterator = collection.search_iterator(
        data=[query_vector()],
        anns_field="vector",
        param={"metric_type": "COSINE", "params": {"ef": args.ef}},
        batch_size=args.count,
        limit=args.count,
        ignore_growing=True,
        timeout=args.timeout,
    )
    ids = []
    try:
        while True:
            page = iterator.next()
            if not page:
                break
            ids.extend(int(hit.id) for hit in page)
    finally:
        try:
            iterator.close()
        except Exception:
            pass
    return ids, digest_ids(ids)


def run_query(collection, args):
    rows = collection.query(
        expr="pk >= 0",
        output_fields=["pk", "vector"],
        limit=args.count,
        consistency_level="Strong",
        timeout=args.timeout,
    )
    digest = hashlib.sha256()
    ids = []
    for row in rows:
        identifier = int(row["pk"])
        ids.append(identifier)
        digest.update(struct.pack("<q", identifier))
        for value in row["vector"]:
            digest.update(struct.pack("<f", float(value)))
    return ids, digest.hexdigest()


def load(collection, args):
    collection.load(replica_number=1, timeout=7200)
    utility.wait_for_loading_complete(args.collection, timeout=7200)
    return {"status": "success", "progress": utility.loading_progress(args.collection)}


def request(collection, args):
    started = time.monotonic_ns()
    try:
        if args.operation == "Search":
            ids, digest = run_search(collection, args)
        else:
            ids, digest = run_query(collection, args)
        return {
            "status": "success",
            "operation": args.operation,
            "count": len(ids),
            "sha256": digest,
            "elapsedNs": time.monotonic_ns() - started,
        }
    except Exception as error:
        return {
            "status": "error",
            "operation": args.operation,
            "count": 0,
            "errorType": type(error).__name__,
            "error": str(error),
            "errorCode": getattr(error, "code", None),
            "elapsedNs": time.monotonic_ns() - started,
        }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=["load", "request"])
    parser.add_argument("--host", default="10.15.9.42")
    parser.add_argument("--port", default="19532")
    parser.add_argument("--collection", default="cohere_1m_qn_fanin")
    parser.add_argument("--operation", choices=["Search", "Query"], default="Search")
    parser.add_argument("--count", type=int, default=8192)
    parser.add_argument("--ef", type=int, default=8192)
    parser.add_argument("--timeout", type=float, default=60)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    connect(args)
    try:
        collection = Collection(args.collection)
        result = load(collection, args) if args.action == "load" else request(collection, args)
        with open(args.output, "w", encoding="utf-8") as output:
            json.dump(result, output, indent=2, sort_keys=True)
        print(json.dumps(result, sort_keys=True))
    finally:
        connections.disconnect("default")


if __name__ == "__main__":
    main()
PY
    client "chmod +x '$CLIENT_DRIVER'"
}

run_load() {
    local label=$1
    local remote_output="$CLIENT_RUN_DIR/$label-load.json"
    client "cd '$CLIENT_REPO' && PYTHONPATH='$CLIENT_REPO' \
        .venv/bin/python '$CLIENT_DRIVER' load \
        --collection '$COLLECTION_NAME' --output '$remote_output'" \
        >"$RUN_DIR/$label-load.log" 2>&1
    copy_from_client "$remote_output" "$RUN_DIR/$label-load.json"
}

restart_querynode() {
    local fault=$1 label=$2
    stop_querynodes
    QUERYNODE_FAULT=$fault
    start_querynodes 1
    run_load "$label"
}

start_request() {
    local operation=$1 case_name=$2 timeout_seconds=$3 case_dir=$4
    local remote_dir="$CLIENT_RUN_DIR/$operation/$case_name"
    CLIENT_REQUEST_OUTPUT="$remote_dir/client.json"
    client "mkdir -p '$remote_dir'; cd '$CLIENT_REPO'; PYTHONPATH='$CLIENT_REPO' \
        .venv/bin/python '$CLIENT_DRIVER' request \
        --collection '$COLLECTION_NAME' --operation '$operation' \
        --count '$TOP_K' --ef '$SEARCH_EF' --timeout '$timeout_seconds' \
        --output '$CLIENT_REQUEST_OUTPUT'" >"$case_dir/client.log" 2>&1 &
    CLIENT_REQUEST_PID=$!
}

finish_request() {
    local case_dir=$1
    wait "$CLIENT_REQUEST_PID"
    copy_from_client "$CLIENT_REQUEST_OUTPUT" "$case_dir/client.json"
    CLIENT_REQUEST_PID=""
}

wait_for_checkpoint() {
    local operation=$1 fault=$2
    server_bash "$operation" "$fault" "$SERVER_RUN_DIR/logs/querynode_1.log" <<'REMOTE'
set -Eeuo pipefail
operation=$1
fault=$2
log_file=$3
deadline=$((SECONDS + 120))
until grep -F 'ReduceStream fault checkpoint reached' "$log_file" 2>/dev/null \
    | grep -F "operation=$operation" | grep -F "fault=$fault" >/dev/null; do
    if ((SECONDS >= deadline)); then
        echo "fault checkpoint was not observed: operation=$operation fault=$fault" >&2
        exit 1
    fi
    sleep 1
done
REMOTE
}

apply_external_action() {
    local action=$1 case_dir=$2
    server "ss -tinp '( sport = :$QUERYNODE_RPC_BASE )' || true" \
        >"$case_dir/sockets-before.txt"
    case "$action" in
        deadline)
            ;;
        sigkill)
            server "pid=\$(cat '$SERVER_RUN_DIR/pids/querynode_1.pid'); \
                kill -KILL -- -\$pid 2>/dev/null || kill -KILL \$pid"
            ;;
        sigterm)
            server "pid=\$(cat '$SERVER_RUN_DIR/pids/querynode_1.pid'); \
                kill -TERM -- -\$pid 2>/dev/null || kill -TERM \$pid"
            ;;
        tcpreset)
            server "sudo -n ss -K 'sport = :$QUERYNODE_RPC_BASE'"
            ;;
        *)
            fail "unknown external action: $action"
            ;;
    esac
    server "ss -tinp '( sport = :$QUERYNODE_RPC_BASE )' || true" \
        >"$case_dir/sockets-after.txt"
}

collect_case_evidence() {
    local operation=$1 case_name=$2 fault=$3 case_dir=$4
    copy_from_server "$SERVER_RUN_DIR/logs/querynode_1.log" "$case_dir/querynode.log"
    copy_from_server "$SERVER_RUN_DIR/logs/proxy.log" "$case_dir/proxy.log"
    grep -F 'ReduceStream fault request accepted' "$case_dir/querynode.log" \
        | grep -F "operation=$operation" | grep -F "fault=$fault" \
        >"$case_dir/accepted-attempts.txt" || true
    grep -F 'ReduceStream fault checkpoint reached' "$case_dir/querynode.log" \
        | grep -F "operation=$operation" | grep -F "fault=$fault" \
        >"$case_dir/checkpoints.txt" || true
    server "if [[ -s '$SERVER_RUN_DIR/pids/querynode_1.pid' ]]; then \
        pid=\$(cat '$SERVER_RUN_DIR/pids/querynode_1.pid'); \
        if kill -0 \$pid 2>/dev/null; then echo alive; else echo exited; fi; \
        ps -o pid,ppid,sid,stat,etimes,args -p \$pid || true; else echo no-pid-file; fi" \
        >"$case_dir/process-state.txt"
    printf '%s\n' "$case_name" >"$case_dir/case.txt"
}

qualify_current_topology() {
    local topology_dir="$RUN_DIR/topology"
    mkdir -p "$topology_dir"
    qualify_placement N1 1 "$topology_dir"
    server "grep 'query view work nodes selected' '$SERVER_RUN_DIR/logs/proxy.log' | tail -n 1" \
        >"$topology_dir/worknodes.log"
    [[ -s "$topology_dir/worknodes.log" ]] || fail "WorkNode evidence is empty"
    validate_worknodes "$topology_dir" 1
}

run_case() {
    local operation=$1 case_name=$2 fault=$3 action=$4
    local case_dir="$RUN_DIR/$operation/$case_name"
    local timeout=$REQUEST_TIMEOUT_SECONDS
    mkdir -p "$case_dir"

    [[ "$action" == deadline ]] && timeout=$DEADLINE_SECONDS
    restart_querynode "$fault" "$operation-$case_name"
    start_request "$operation" "$case_name" "$timeout" "$case_dir"

    if [[ -n "$action" ]]; then
        wait_for_checkpoint "$operation" "$fault"
        apply_external_action "$action" "$case_dir"
    fi
    finish_request "$case_dir"
    collect_case_evidence "$operation" "$case_name" "$fault" "$case_dir"
}

run_operation() {
    local operation=$1
    log "Running $operation failure cases"
    run_case "$operation" BASELINE "" ""
    if [[ "$operation" == Search ]]; then
        qualify_current_topology
    fi
    run_case "$operation" ERROR-BEFORE error_before_first_chunk ""
    run_case "$operation" ERROR-AFTER error_after_first_chunk ""
    run_case "$operation" EOF-AFTER eof_after_first_chunk ""
    run_case "$operation" DEADLINE-BEFORE block_before_first_chunk deadline
    run_case "$operation" DEADLINE-AFTER block_after_first_chunk deadline
    run_case "$operation" SIGKILL-BEFORE block_before_first_chunk sigkill
    run_case "$operation" SIGKILL-AFTER block_after_first_chunk sigkill
    run_case "$operation" SIGTERM-BEFORE block_before_first_chunk sigterm
    run_case "$operation" SIGTERM-AFTER block_after_first_chunk sigterm
    run_case "$operation" TCPRESET-BEFORE block_before_first_chunk tcpreset
    run_case "$operation" TCPRESET-AFTER block_after_first_chunk tcpreset
}

validate_results() {
    python3 - "$RUN_DIR" <<'PY'
import csv
import json
import pathlib
import sys

run_dir = pathlib.Path(sys.argv[1])
rows = []
for operation in ("Search", "Query"):
    operation_dir = run_dir / operation
    baseline = json.load(open(operation_dir / "BASELINE" / "client.json", encoding="utf-8"))
    if baseline["status"] != "success" or baseline["count"] != 8192:
        raise SystemExit(f"{operation} baseline failed: {baseline}")

    for case_dir in sorted(path for path in operation_dir.iterdir() if path.is_dir()):
        case = case_dir.name
        result = json.load(open(case_dir / "client.json", encoding="utf-8"))
        attempts_path = case_dir / "accepted-attempts.txt"
        attempts = len(attempts_path.read_text(encoding="utf-8").splitlines()) if attempts_path.exists() else 0
        checkpoints_path = case_dir / "checkpoints.txt"
        checkpoints = len(checkpoints_path.read_text(encoding="utf-8").splitlines()) if checkpoints_path.exists() else 0

        if case in {"BASELINE", "ERROR-BEFORE", "TCPRESET-BEFORE"}:
            if result["status"] != "success" or result["count"] != 8192:
                raise SystemExit(f"{operation}/{case} was not a complete success: {result}")
            if result["sha256"] != baseline["sha256"]:
                raise SystemExit(f"{operation}/{case} differs from baseline")
        elif case != "EOF-AFTER" and result["status"] != "error":
            raise SystemExit(f"{operation}/{case} returned partial success: {result}")

        if case in {"ERROR-BEFORE", "TCPRESET-BEFORE"} and attempts < 2:
            raise SystemExit(f"{operation}/{case} did not expose a retry: attempts={attempts}")
        if case not in {"BASELINE", "ERROR-BEFORE", "TCPRESET-BEFORE"} and attempts != 1:
            raise SystemExit(f"{operation}/{case} accepted attempts={attempts}, expected 1")
        if case not in {"BASELINE"} and checkpoints != 1:
            raise SystemExit(f"{operation}/{case} checkpoints={checkpoints}, expected 1")

        rows.append({
            "operation": operation,
            "case": case,
            "status": result["status"],
            "count": result.get("count", 0),
            "sha256": result.get("sha256", ""),
            "errorType": result.get("errorType", ""),
            "errorCode": result.get("errorCode", ""),
            "acceptedAttempts": attempts,
            "checkpoints": checkpoints,
            "elapsedNs": result["elapsedNs"],
        })

with open(run_dir / "manifest.tsv", "w", encoding="utf-8", newline="") as output:
    writer = csv.DictWriter(output, fieldnames=rows[0].keys(), delimiter="\t")
    writer.writeheader()
    writer.writerows(rows)
json.dump(rows, open(run_dir / "summary.json", "w", encoding="utf-8"), indent=2)
PY
}

run_recovery() {
    local operation
    restart_querynode "" final-recovery
    for operation in Search Query; do
        local case_dir="$RUN_DIR/final-recovery/$operation"
        mkdir -p "$case_dir"
        start_request "$operation" RECOVERY "$REQUEST_TIMEOUT_SECONDS" "$case_dir"
        finish_request "$case_dir"
        python3 - "$RUN_DIR/$operation/BASELINE/client.json" "$case_dir/client.json" <<'PY'
import json
import sys

baseline = json.load(open(sys.argv[1], encoding="utf-8"))
recovery = json.load(open(sys.argv[2], encoding="utf-8"))
if recovery.get("status") != "success" or recovery.get("count") != 8192:
    raise SystemExit(f"recovery request failed: {recovery}")
if recovery.get("sha256") != baseline.get("sha256"):
    raise SystemExit("recovery result differs from baseline")
PY
    done
}

main() {
    initialize_runner
    log "Run ID: $RUN_ID"
    preflight
    write_server_configuration
    install_client_driver
    start_infrastructure
    start_base_roles
    start_proxy

    run_operation Search
    run_operation Query
    run_recovery
    validate_results
    log "Stage 2 QueryNode failure qualification passed"
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
    main "$@"
fi
