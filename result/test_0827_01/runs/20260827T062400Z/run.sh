#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRIPT_PATH="$SCRIPT_DIR/$(basename "${BASH_SOURCE[0]}")"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
RUNS_DIR="$SCRIPT_DIR/runs"

if ! command -v go >/dev/null 2>&1 && [[ -x "$HOME/.local/bin/go" ]]; then
    export PATH="$HOME/.local/bin:$PATH"
fi
if ! command -v go >/dev/null 2>&1; then
    printf 'go executable not found\n' >&2
    exit 1
fi

if [[ "${MATRIX_WORKER:-0}" != "1" ]]; then
    run_id="$(date -u +%Y%m%dT%H%M%SZ)"
    run_dir="$RUNS_DIR/$run_id"
    mkdir -p "$run_dir"
    nohup env MATRIX_WORKER=1 MATRIX_RUN_DIR="$run_dir" "$SCRIPT_PATH" \
        >"$run_dir/runner.log" 2>&1 </dev/null &
    pid=$!
    printf '%s\n' "$pid" >"$run_dir/pid"
    printf 'started run %s\npid: %s\nartifacts: %s\n' "$run_id" "$pid" "$run_dir"
    exit 0
fi

RUN_DIR="${MATRIX_RUN_DIR:?MATRIX_RUN_DIR is required in worker mode}"
CONFIG_DIR="$RUN_DIR/configs"
LOG_DIR="$RUN_DIR/logs"
BINARY="$RUN_DIR/benchmark"
MANIFEST="$RUN_DIR/manifest.tsv"
mkdir -p "$CONFIG_DIR" "$LOG_DIR"

finish() {
    status=$?
    if [[ $status -eq 0 ]]; then
        date -u +%Y-%m-%dT%H:%M:%SZ >"$RUN_DIR/COMPLETED"
    else
        printf 'exit_code=%d\nfinished_at=%s\n' \
            "$status" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" >"$RUN_DIR/FAILED"
    fi
}
trap finish EXIT

cd "$REPO_ROOT"
git rev-parse HEAD >"$RUN_DIR/source_commit.txt"
git status --short --branch >"$RUN_DIR/source_status.txt"
git diff --binary >"$RUN_DIR/source_diff.patch"
{
    go version
    uname -a
} >"$RUN_DIR/environment.txt"
cp "$SCRIPT_DIR/TEST_PLAN.md" "$RUN_DIR/TEST_PLAN.md"
cp "$SCRIPT_PATH" "$RUN_DIR/run.sh"
go test -race -count=1 ./... >"$RUN_DIR/go_test.log" 2>&1
go build -o "$BINARY" .

printf 'case_id\trepetition\tchildren\tpayload_per_child\tchunk_bytes\tunit_bytes\ttopk\tstarted_at\tfinished_at\texit_code\toutcome\tconfig\tlog\n' >"$MANIFEST"

write_config() {
    local destination=$1 children=$2 payload=$3
    sed -E \
        -e "s/^  child_processes:.*/  child_processes: $children/" \
        -e "s/^  workflow:.*/  workflow: ordered_topk/" \
        -e "s/^  total_payload_bytes_per_child:.*/  total_payload_bytes_per_child: $payload/" \
        -e "s/^  stream_chunk_bytes:.*/  stream_chunk_bytes: 262144/" \
        -e "s/^  per_unit_bytes:.*/  per_unit_bytes: 256/" \
        -e "s/^  global_topk:.*/  global_topk: 4096/" \
        -e "s/^  result_distribution:.*/  result_distribution: interleaved/" \
        -e "s/^  concurrency:.*/  concurrency: 1/" \
        -e "s/^  warmup_requests:.*/  warmup_requests: 3/" \
        -e "s/^  measured_requests:.*/  measured_requests: 10/" \
        -e "s/^  minimum_measurement_duration_ms:.*/  minimum_measurement_duration_ms: 1000/" \
        -e "s/^  mode_order:.*/  mode_order: streaming_first/" \
        -e "s/^  request_timeout_ms:.*/  request_timeout_ms: 120000/" \
        "$REPO_ROOT/config.yaml" >"$destination"
}

validate_log() {
    local log_path=$1 children=$2
    for (( child = 0; child < children; child++ )); do
        [[ $(grep -c "^child_transfer mode=streaming child=$child " "$log_path") -eq 1 ]]
        [[ $(grep -c "^grpc_receive mode=streaming child=$child " "$log_path") -eq 1 ]]
        [[ $(grep -c "^transport mode=streaming child=$child .*child_tcp_info_available=true parent_tcp_info_available=true" "$log_path") -eq 1 ]]
    done
    [[ $(grep -c '^application_retention mode=streaming ' "$log_path") -eq 1 ]]
}

run_case() {
    local case_id=$1 children=$2 payload=$3
    local repetition stem config_path log_path started_at finished_at exit_code outcome

    for repetition in 1 2 3; do
        stem="${case_id}_rep${repetition}_streaming_first"
        config_path="$CONFIG_DIR/${stem}.yaml"
        log_path="$LOG_DIR/${stem}.log"
        write_config "$config_path" "$children" "$payload"

        started_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
        printf '[%s] starting %s repetition %d\n' "$started_at" "$case_id" "$repetition"
        set +e
        "$BINARY" --config "$config_path" >"$log_path" 2>&1
        exit_code=$?
        if [[ $exit_code -eq 0 ]] && ! validate_log "$log_path" "$children"; then
            printf 'required Streaming transport metrics are missing\n' >>"$log_path"
            exit_code=1
        fi
        set -e

        finished_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
        outcome=passed
        if [[ $exit_code -ne 0 ]]; then
            outcome=failed
        fi
        printf '%s\t%d\t%d\t%d\t262144\t256\t4096\t%s\t%s\t%d\t%s\t%s\t%s\n' \
            "$case_id" "$repetition" "$children" "$payload" "$started_at" \
            "$finished_at" "$exit_code" "$outcome" "$config_path" "$log_path" >>"$MANIFEST"
        if [[ $exit_code -ne 0 ]]; then
            cat "$log_path"
            return "$exit_code"
        fi
    done
}

CASES=(
    'RUNAHEAD-N2-P4|2|4194304'
    'RUNAHEAD-N2-P16|2|16777216'
    'RUNAHEAD-N2-P32|2|33554432'
    'RUNAHEAD-N2-P60|2|62914560'
    'RUNAHEAD-N8-P4|8|4194304'
    'RUNAHEAD-N8-P16|8|16777216'
    'RUNAHEAD-N8-P32|8|33554432'
    'RUNAHEAD-N8-P60|8|62914560'
)

for definition in "${CASES[@]}"; do
    IFS='|' read -r case_id children payload <<<"$definition"
    run_case "$case_id" "$children" "$payload"
done

printf '[%s] matrix completed\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
