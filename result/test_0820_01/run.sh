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
go build -o "$BINARY" .

printf 'case_id\tgroup\trepetition\tmode_order\tchildren\tpayload_per_child\tchunk_bytes\tconcurrency\tstarted_at\tfinished_at\tprocess_exit\toutcome\tconfig\tlog\n' >"$MANIFEST"

write_config() {
    local destination=$1 children=$2 payload=$3 chunk=$4 concurrency=$5 mode_order=$6
    sed -E \
        -e "s/^  child_processes:.*/  child_processes: $children/" \
        -e "s/^  total_payload_bytes_per_child:.*/  total_payload_bytes_per_child: $payload/" \
        -e "s/^  stream_chunk_bytes:.*/  stream_chunk_bytes: $chunk/" \
        -e "s/^  concurrency:.*/  concurrency: $concurrency/" \
        -e "s/^  warmup_requests:.*/  warmup_requests: 20/" \
        -e "s/^  measured_requests:.*/  measured_requests: 200/" \
        -e "s/^  minimum_measurement_duration_ms:.*/  minimum_measurement_duration_ms: 20000/" \
        -e "s/^  mode_order:.*/  mode_order: $mode_order/" \
        "$REPO_ROOT/config.yaml" >"$destination"
}

run_case() {
    local case_id=$1 group=$2 children=$3 payload=$4 chunk=$5 concurrency=$6
    local repetition mode_order stem config_path log_path started_at finished_at exit_code outcome
    for repetition in 1 2 3 4 5; do
        if (( repetition % 2 == 1 )); then
            mode_order=unary_first
        else
            mode_order=streaming_first
        fi
        stem="${case_id}_rep${repetition}_${mode_order}"
        config_path="$CONFIG_DIR/${stem}.yaml"
        log_path="$LOG_DIR/${stem}.log"
        write_config "$config_path" "$children" "$payload" "$chunk" "$concurrency" "$mode_order"
        started_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
        printf '[%s] starting %s repetition %d (%s)\n' "$started_at" "$case_id" "$repetition" "$mode_order"
        set +e
        "$BINARY" --config "$config_path" >"$log_path" 2>&1
        exit_code=$?
        set -e
        finished_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
        outcome=passed
        if [[ $exit_code -ne 0 ]]; then
            outcome=failed
        fi
        printf '%s\t%s\t%d\t%s\t%d\t%d\t%d\t%d\t%s\t%s\t%d\t%s\t%s\t%s\n' \
            "$case_id" "$group" "$repetition" "$mode_order" "$children" "$payload" \
            "$chunk" "$concurrency" "$started_at" "$finished_at" "$exit_code" "$outcome" \
            "$config_path" "$log_path" >>"$MANIFEST"
        if [[ $exit_code -ne 0 ]]; then
            cat "$log_path"
            return "$exit_code"
        fi
    done
}

CASES=(
    'CHUNK-001|chunk|8|16777216|16384|1'
    'CHUNK-002|chunk|8|16777216|65536|1'
    'CHUNK-003|chunk|8|16777216|262144|1'
    'CHUNK-004|chunk|8|16777216|1048576|1'
    'CHUNK-005|chunk|8|16777216|4194304|1'
    'CHUNK-006|chunk|8|16777216|16777216|1'
    'FANIN-001|fanin|1|33554432|262144|1'
    'FANIN-002|fanin|2|16777216|262144|1'
    'FANIN-003|fanin|4|8388608|262144|1'
    'FANIN-004|fanin|8|4194304|262144|1'
    'FANIN-005|fanin|16|2097152|262144|1'
    'FANIN-006|fanin|32|1048576|262144|1'
    'PAYLOAD-001|payload|8|262144|262144|1'
    'PAYLOAD-002|payload|8|1048576|262144|1'
    'PAYLOAD-003|payload|8|4194304|262144|1'
    'PAYLOAD-004|payload|8|16777216|262144|1'
    'PAYLOAD-005|payload|8|33554432|262144|1'
    'CONCURRENCY-001|concurrency|8|1048576|262144|1'
    'CONCURRENCY-002|concurrency|8|1048576|262144|4'
    'CONCURRENCY-003|concurrency|8|1048576|262144|16'
    'CONCURRENCY-004|concurrency|8|1048576|262144|32'
    'CONCURRENCY-005|concurrency|8|1048576|262144|64'
    'BOUNDARY-VALID|boundary|1|62914560|262144|1'
)

for definition in "${CASES[@]}"; do
    IFS='|' read -r case_id group children payload chunk concurrency <<<"$definition"
    run_case "$case_id" "$group" "$children" "$payload" "$chunk" "$concurrency"
done

invalid_config="$CONFIG_DIR/BOUNDARY-INVALID.yaml"
invalid_log="$LOG_DIR/BOUNDARY-INVALID.log"
write_config "$invalid_config" 1 67108864 262144 1 unary_first
started_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
set +e
"$BINARY" --config "$invalid_config" >"$invalid_log" 2>&1
invalid_exit=$?
set -e
finished_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
if [[ $invalid_exit -eq 0 ]] || ! grep -q 'exceeding the effective' "$invalid_log"; then
    printf 'BOUNDARY-INVALID did not produce the expected message-limit rejection\n' >&2
    exit 1
fi
printf 'BOUNDARY-INVALID\tboundary\t1\tunary_first\t1\t67108864\t262144\t1\t%s\t%s\t%d\texpected_rejection\t%s\t%s\n' \
    "$started_at" "$finished_at" "$invalid_exit" "$invalid_config" "$invalid_log" >>"$MANIFEST"

printf '[%s] matrix completed\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
