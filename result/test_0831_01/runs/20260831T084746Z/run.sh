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
git diff --binary HEAD >"$RUN_DIR/source_diff.patch"
git ls-files --others --exclude-standard >"$RUN_DIR/source_untracked.txt"
{
    go version
    uname -a
    ulimit -a
    [[ ! -r /proc/meminfo ]] || cat /proc/meminfo
} >"$RUN_DIR/environment.txt"
cp "$SCRIPT_DIR/TEST_PLAN.md" "$RUN_DIR/TEST_PLAN.md"
cp "$SCRIPT_PATH" "$RUN_DIR/run.sh"
go test -race -count=1 ./... >"$RUN_DIR/go_test.log" 2>&1
go build -o "$BINARY" .

printf 'case_id\trepetition\tmode_order\tchildren\tpayload_per_child\tchunk_bytes\tstatic_window_bytes\tunit_bytes\ttopk\tconcurrency\tstarted_at\tfinished_at\texit_code\toutcome\tconfig\tlog\n' >"$MANIFEST"

write_config() {
    local destination=$1 concurrency=$2 mode_order=$3
    sed -E \
        -e 's/^    static_window_bytes:.*/    static_window_bytes: 0/' \
        -e 's/^  child_processes:.*/  child_processes: 8/' \
        -e 's/^  workflow:.*/  workflow: ordered_topk/' \
        -e 's/^  total_payload_bytes_per_child:.*/  total_payload_bytes_per_child: 16777216/' \
        -e 's/^  stream_chunk_bytes:.*/  stream_chunk_bytes: 262144/' \
        -e 's/^  per_unit_bytes:.*/  per_unit_bytes: 256/' \
        -e 's/^  global_topk:.*/  global_topk: 65536/' \
        -e 's/^  result_distribution:.*/  result_distribution: interleaved/' \
        -e "s/^  concurrency:.*/  concurrency: $concurrency/" \
        -e 's/^  warmup_requests:.*/  warmup_requests: 20/' \
        -e 's/^  measured_requests:.*/  measured_requests: 200/' \
        -e 's/^  minimum_measurement_duration_ms:.*/  minimum_measurement_duration_ms: 20000/' \
        -e "s/^  mode_order:.*/  mode_order: $mode_order/" \
        -e 's/^  request_timeout_ms:.*/  request_timeout_ms: 120000/' \
        "$REPO_ROOT/config.yaml" >"$destination"
}

validate_log() {
    local log_path=$1 concurrency=$2
    grep -q "^workflow=ordered_topk children=8 .* concurrency=$concurrency .* static_window_bytes=0 .* global_topk=65536 result_distribution=interleaved$" "$log_path"
    for mode in unary streaming; do
        awk -v mode="$mode" '$1 == mode && $2 >= 200 && $3 == 0 { found = 1 } END { exit !found }' "$log_path"
        grep -q "^transfer mode=$mode .*emitted_units_per_operation=65536.00 " "$log_path"
        grep -q "^application_retention mode=$mode " "$log_path"
        for (( child = 0; child < 8; child++ )); do
            [[ $(grep -c "^child_transfer mode=$mode child=$child " "$log_path") -eq 1 ]]
            [[ $(grep -c "^grpc_receive mode=$mode child=$child " "$log_path") -eq 1 ]]
            [[ $(grep -c "^transport mode=$mode child=$child .*child_tcp_info_available=true parent_tcp_info_available=true" "$log_path") -eq 1 ]]
        done
    done
    grep -q '^transfer mode=unary potential_bytes_per_operation=134217728 received_bytes_per_operation=134217728 ' "$log_path"
}

run_case() {
    local concurrency=$1 case_id="CONCURRENCY-K${1}"
    local repetition mode_order stem config_path log_path
    local started_at finished_at exit_code outcome

    for repetition in 1 2 3 4; do
        if (( repetition % 2 == 1 )); then
            mode_order=unary_first
        else
            mode_order=streaming_first
        fi
        stem="${case_id}_rep${repetition}_${mode_order}"
        config_path="$CONFIG_DIR/${stem}.yaml"
        log_path="$LOG_DIR/${stem}.log"
        write_config "$config_path" "$concurrency" "$mode_order"

        started_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
        printf '[%s] starting %s repetition %d (%s)\n' \
            "$started_at" "$case_id" "$repetition" "$mode_order"
        set +e
        "$BINARY" --config "$config_path" >"$log_path" 2>&1
        exit_code=$?
        if [[ $exit_code -eq 0 ]] && ! validate_log "$log_path" "$concurrency"; then
            printf 'required correctness or transport metrics are missing\n' >>"$log_path"
            exit_code=1
        fi
        set -e

        finished_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
        outcome=passed
        [[ $exit_code -eq 0 ]] || outcome=failed
        printf '%s\t%d\t%s\t8\t16777216\t262144\t0\t256\t65536\t%d\t%s\t%s\t%d\t%s\t%s\t%s\n' \
            "$case_id" "$repetition" "$mode_order" "$concurrency" \
            "$started_at" "$finished_at" "$exit_code" "$outcome" \
            "$config_path" "$log_path" >>"$MANIFEST"
        if [[ $exit_code -ne 0 ]]; then
            cat "$log_path"
            return "$exit_code"
        fi
    done
}

gate_k64() {
    local gate_file="$RUN_DIR/resource_gate_k64.txt"
    local expected_passes=24 required_bytes=$(( 2 * 8 * 16777216 * 64 ))
    local passed available_bytes
    passed=$(grep -c $'\tpassed\t' "$MANIFEST")
    if [[ ! -r /proc/meminfo ]]; then
        printf 'result=failed\nreason=/proc/meminfo unavailable\n' >"$gate_file"
        return 1
    fi
    available_bytes=$(awk '/^MemAvailable:/ { printf "%.0f", $2 * 1024 }' /proc/meminfo)
    printf 'completed_prior_executions=%d\nrequired_prior_executions=%d\nmem_available_bytes=%s\nrequired_mem_available_bytes=%d\n' \
        "$passed" "$expected_passes" "$available_bytes" "$required_bytes" >"$gate_file"
    if [[ $passed -ne $expected_passes || $available_bytes -lt $required_bytes ]]; then
        printf 'result=failed\n' >>"$gate_file"
        return 1
    fi
    printf 'result=passed\n' >>"$gate_file"
}

for concurrency in 1 2 4 8 16 32; do
    run_case "$concurrency"
done
gate_k64
run_case 64

printf '[%s] concurrency matrix completed\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
