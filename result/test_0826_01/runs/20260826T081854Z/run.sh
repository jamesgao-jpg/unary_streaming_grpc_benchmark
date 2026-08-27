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

printf 'case_id\tgroup\trepetition\tmode_order\tworkflow\tdistribution\tchildren\tpayload_per_child\tchunk_bytes\tunit_bytes\ttopk\tconcurrency\tstarted_at\tfinished_at\texit_code\toutcome\tconfig\tlog\n' >"$MANIFEST"

write_config() {
    local destination=$1 workflow=$2 children=$3 payload=$4 chunk=$5
    local unit_bytes=$6 topk=$7 distribution=$8 concurrency=$9 mode_order=${10}
    sed -E \
        -e "s/^  child_processes:.*/  child_processes: $children/" \
        -e "s/^  workflow:.*/  workflow: $workflow/" \
        -e "s/^  total_payload_bytes_per_child:.*/  total_payload_bytes_per_child: $payload/" \
        -e "s/^  stream_chunk_bytes:.*/  stream_chunk_bytes: $chunk/" \
        -e "s/^  per_unit_bytes:.*/  per_unit_bytes: $unit_bytes/" \
        -e "s/^  global_topk:.*/  global_topk: $topk/" \
        -e "s/^  result_distribution:.*/  result_distribution: $distribution/" \
        -e "s/^  concurrency:.*/  concurrency: $concurrency/" \
        -e "s/^  warmup_requests:.*/  warmup_requests: 20/" \
        -e "s/^  measured_requests:.*/  measured_requests: 200/" \
        -e "s/^  minimum_measurement_duration_ms:.*/  minimum_measurement_duration_ms: 20000/" \
        -e "s/^  mode_order:.*/  mode_order: $mode_order/" \
        "$REPO_ROOT/config.yaml" >"$destination"
}

run_case() {
    local case_id=$1 group=$2 workflow=$3 children=$4 payload=$5 chunk=$6
    local unit_bytes=$7 topk=$8 distribution=$9 concurrency=${10}
    local repetition mode_order stem config_path log_path started_at finished_at
    local exit_code outcome

    for repetition in 1 2 3 4 5; do
        if (( repetition % 2 == 1 )); then
            mode_order=unary_first
        else
            mode_order=streaming_first
        fi
        stem="${case_id}_rep${repetition}_${mode_order}"
        config_path="$CONFIG_DIR/${stem}.yaml"
        log_path="$LOG_DIR/${stem}.log"
        write_config "$config_path" "$workflow" "$children" "$payload" "$chunk" \
            "$unit_bytes" "$topk" "$distribution" "$concurrency" "$mode_order"

        started_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
        printf '[%s] starting %s repetition %d (%s)\n' \
            "$started_at" "$case_id" "$repetition" "$mode_order"
        set +e
        "$BINARY" --config "$config_path" >"$log_path" 2>&1
        exit_code=$?
        set -e
        if [[ $exit_code -eq 0 ]]; then
            for mode in unary streaming; do
                for (( child_index = 0; child_index < children; child_index++ )); do
                    if [[ $(grep -c "^child_transfer mode=$mode child=$child_index " "$log_path") -ne 1 ]]; then
                        printf 'missing per-child counters for mode=%s child=%d\n' \
                            "$mode" "$child_index" >>"$log_path"
                        exit_code=1
                    fi
                done
            done
        fi
        finished_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
        outcome=passed
        if [[ $exit_code -ne 0 ]]; then
            outcome=failed
        fi

        printf '%s\t%s\t%d\t%s\t%s\t%s\t%d\t%d\t%d\t%d\t%d\t%d\t%s\t%s\t%d\t%s\t%s\t%s\n' \
            "$case_id" "$group" "$repetition" "$mode_order" "$workflow" \
            "$distribution" "$children" "$payload" "$chunk" "$unit_bytes" \
            "$topk" "$concurrency" "$started_at" "$finished_at" "$exit_code" \
            "$outcome" "$config_path" "$log_path" >>"$MANIFEST"
        if [[ $exit_code -ne 0 ]]; then
            cat "$log_path"
            return "$exit_code"
        fi
    done
}

CASES=(
    'TOPK-INT-001|topk_scale|ordered_topk|8|131072|262144|256|512|interleaved|1'
    'TOPK-DOM-001|topk_scale|ordered_topk|8|131072|262144|256|512|dominant_child|1'
    'TOPK-INT-002|topk_scale|ordered_topk|8|262144|262144|256|1024|interleaved|1'
    'TOPK-DOM-002|topk_scale|ordered_topk|8|262144|262144|256|1024|dominant_child|1'
    'TOPK-INT-003|topk_scale|ordered_topk|8|1048576|262144|256|4096|interleaved|1'
    'TOPK-DOM-003|topk_scale|ordered_topk|8|1048576|262144|256|4096|dominant_child|1'
    'TOPK-INT-004|topk_scale|ordered_topk|8|4194304|262144|256|16384|interleaved|1'
    'TOPK-DOM-004|topk_scale|ordered_topk|8|4194304|262144|256|16384|dominant_child|1'
    'TOPK-INT-005|topk_scale|ordered_topk|8|16777216|262144|256|65536|interleaved|1'
    'TOPK-DOM-005|topk_scale|ordered_topk|8|16777216|262144|256|65536|dominant_child|1'
    'CHUNK-INT-001|chunk_size|ordered_topk|8|4194304|16384|256|16384|interleaved|1'
    'CHUNK-DOM-001|chunk_size|ordered_topk|8|4194304|16384|256|16384|dominant_child|1'
    'CHUNK-INT-002|chunk_size|ordered_topk|8|4194304|65536|256|16384|interleaved|1'
    'CHUNK-DOM-002|chunk_size|ordered_topk|8|4194304|65536|256|16384|dominant_child|1'
    'CHUNK-INT-003|chunk_size|ordered_topk|8|4194304|262144|256|16384|interleaved|1'
    'CHUNK-DOM-003|chunk_size|ordered_topk|8|4194304|262144|256|16384|dominant_child|1'
    'CHUNK-INT-004|chunk_size|ordered_topk|8|4194304|1048576|256|16384|interleaved|1'
    'CHUNK-DOM-004|chunk_size|ordered_topk|8|4194304|1048576|256|16384|dominant_child|1'
    'CHUNK-INT-005|chunk_size|ordered_topk|8|4194304|4194304|256|16384|interleaved|1'
    'CHUNK-DOM-005|chunk_size|ordered_topk|8|4194304|4194304|256|16384|dominant_child|1'
    'CONTROL-001|full_transfer|full_transfer|8|1048576|262144|256|4096|interleaved|1'
    'CONTROL-002|full_transfer|full_transfer|8|4194304|262144|256|16384|interleaved|1'
    'CONTROL-003|full_transfer|full_transfer|8|16777216|262144|256|65536|interleaved|1'
)

for definition in "${CASES[@]}"; do
    IFS='|' read -r case_id group workflow children payload chunk unit_bytes \
        topk distribution concurrency <<<"$definition"
    run_case "$case_id" "$group" "$workflow" "$children" "$payload" "$chunk" \
        "$unit_bytes" "$topk" "$distribution" "$concurrency"
done

printf '[%s] matrix completed\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
