#!/usr/bin/env bash
# Runs the detached small-payload stream-creation premise matrix and preserves artifacts.
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

# finish records one unambiguous terminal marker for the detached run.
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
git ls-files --others --exclude-standard >"$RUN_DIR/source_untracked.txt"
{
    go version
    uname -a
    if command -v nproc >/dev/null 2>&1; then
        nproc
    fi
    if [[ -r /proc/meminfo ]]; then
        cat /proc/meminfo
    fi
} >"$RUN_DIR/environment.txt"
cp "$SCRIPT_DIR/TEST_PLAN.md" "$RUN_DIR/TEST_PLAN.md"
cp "$SCRIPT_PATH" "$RUN_DIR/run.sh"
go test -race -count=1 ./... >"$RUN_DIR/go_test.log" 2>&1
go build -o "$BINARY" .

printf 'case_id\tgroup\trepetition\tmode_order\tchildren\tpayload_per_child\tchunk_bytes\tconcurrency\tstarted_at\tfinished_at\texit_code\toutcome\tconfig\tlog\n' >"$MANIFEST"

# write_config derives one full-transfer YAML file from the repository default.
write_config() {
    local destination=$1 children=$2 payload=$3 chunk=$4 concurrency=$5 mode_order=$6
    sed -E \
        -e "s/^    static_window_bytes:.*/    static_window_bytes: 0/" \
        -e "s/^  child_processes:.*/  child_processes: $children/" \
        -e "s/^  workflow:.*/  workflow: full_transfer/" \
        -e "s/^  total_payload_bytes_per_child:.*/  total_payload_bytes_per_child: $payload/" \
        -e "s/^  stream_chunk_bytes:.*/  stream_chunk_bytes: $chunk/" \
        -e "s/^  concurrency:.*/  concurrency: $concurrency/" \
        -e "s/^  warmup_requests:.*/  warmup_requests: 3/" \
        -e "s/^  measured_requests:.*/  measured_requests: 500/" \
        -e "s/^  minimum_measurement_duration_ms:.*/  minimum_measurement_duration_ms: 20000/" \
        -e "s/^  mode_order:.*/  mode_order: $mode_order/" \
        -e "s/^  request_timeout_ms:.*/  request_timeout_ms: 120000/" \
        "$REPO_ROOT/config.yaml" >"$destination"
}

# validate_mode verifies complete transfer and all current per-child metric layers.
validate_mode() {
    local log_path=$1 mode=$2 children=$3 expected_messages=$4 total_bytes=$5
    awk -v mode="$mode" '$1 == mode { found = 1; if ($2 < 500 || $3 != 0) exit 1 } END { if (!found) exit 1 }' "$log_path"
    grep -Eq "^transfer mode=$mode potential_bytes_per_operation=$total_bytes received_bytes_per_operation=$total_bytes .*response_messages_per_operation=${expected_messages}\\.00 .*saved_percent=0\\.00$" "$log_path"
    grep -Eq "^application_retention mode=$mode .*unconsumed_payload_bytes_at_topk_total=0 unconsumed_payload_bytes_per_operation=0$" "$log_path"
    for (( child = 0; child < children; child++ )); do
        [[ $(grep -c "^child_transfer mode=$mode child=$child " "$log_path") -eq 1 ]]
        [[ $(grep -c "^grpc_receive mode=$mode child=$child " "$log_path") -eq 1 ]]
        [[ $(grep -c "^transport mode=$mode child=$child .*child_tcp_info_available=true parent_tcp_info_available=true" "$log_path") -eq 1 ]]
    done
}

# validate_log checks workload identity and both transport modes.
validate_log() {
    local log_path=$1 children=$2 payload=$3 chunk=$4 concurrency=$5
    local total_bytes=$(( children * payload ))
    local unary_messages=$children
    local streaming_messages=$(( children * ((payload + chunk - 1) / chunk) ))

    grep -Eq "^workflow=full_transfer children=$children .*total_payload_bytes_per_child=$payload stream_chunk_bytes=$chunk concurrency=$concurrency .*static_window_bytes=0$" "$log_path"
    validate_mode "$log_path" unary "$children" "$unary_messages" "$total_bytes"
    validate_mode "$log_path" streaming "$children" "$streaming_messages" "$total_bytes"
}

# run_case executes four fresh-process repetitions with balanced mode order.
run_case() {
    local case_id=$1 group=$2 children=$3 payload=$4 chunk=$5 concurrency=$6
    local repetition mode_order stem config_path log_path started_at finished_at
    local exit_code outcome

    for repetition in 1 2 3 4; do
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
        printf '[%s] starting %s repetition %d (%s)\n' \
            "$started_at" "$case_id" "$repetition" "$mode_order"
        set +e
        "$BINARY" --config "$config_path" >"$log_path" 2>&1
        exit_code=$?
        if [[ $exit_code -eq 0 ]] && ! validate_log "$log_path" "$children" "$payload" "$chunk" "$concurrency"; then
            printf 'required full-transfer or transport metrics are missing\n' >>"$log_path"
            exit_code=1
        fi
        set -e

        finished_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
        outcome=passed
        if [[ $exit_code -ne 0 ]]; then
            outcome=failed
        fi
        printf '%s\t%s\t%d\t%s\t%d\t%d\t%d\t%d\t%s\t%s\t%d\t%s\t%s\t%s\n' \
            "$case_id" "$group" "$repetition" "$mode_order" "$children" \
            "$payload" "$chunk" "$concurrency" "$started_at" "$finished_at" \
            "$exit_code" "$outcome" "$config_path" "$log_path" >>"$MANIFEST"
        if [[ $exit_code -ne 0 ]]; then
            cat "$log_path"
            return "$exit_code"
        fi
    done
}

CASES=(
    'SP-P1K|payload|1|1024|1024|1'
    'SP-P4K|payload|1|4096|4096|1'
    'SP-P16K|payload|1|16384|16384|1'
    'SP-P64K|payload|1|65536|65536|1'
    'SP-N8-P1K|fanin|8|1024|1024|1'
    'SP-N8-P4K|fanin|8|4096|4096|1'
    'SP-N8-P16K|fanin|8|16384|16384|1'
    'SP-N8-P64K|fanin|8|65536|65536|1'
    'SP-N8-K8|concurrency|8|4096|4096|8'
    'SP-N8-K32|concurrency|8|4096|4096|32'
)

if [[ ${#CASES[@]} -ne 10 ]]; then
    printf 'generated %d cases, expected 10\n' "${#CASES[@]}" >&2
    exit 1
fi

for definition in "${CASES[@]}"; do
    IFS='|' read -r case_id group children payload chunk concurrency <<<"$definition"
    run_case "$case_id" "$group" "$children" "$payload" "$chunk" "$concurrency"
done
