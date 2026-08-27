#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRIPT_PATH="$SCRIPT_DIR/$(basename "${BASH_SOURCE[0]}")"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
RUNS_DIR="$SCRIPT_DIR/runs"
PHASE="${1:-phase_a}"

case "$PHASE" in
    phase_a|phase_b|phase_c) ;;
    *) printf 'usage: %s {phase_a|phase_b|phase_c}\n' "$0" >&2; exit 2 ;;
esac

if ! command -v go >/dev/null 2>&1 && [[ -x "$HOME/.local/bin/go" ]]; then
    export PATH="$HOME/.local/bin:$PATH"
fi
if ! command -v go >/dev/null 2>&1; then
    printf 'go executable not found\n' >&2
    exit 1
fi

if [[ "${MATRIX_WORKER:-0}" != "1" ]]; then
    run_id="${PHASE}_$(date -u +%Y%m%dT%H%M%SZ)"
    run_dir="$RUNS_DIR/$run_id"
    mkdir -p "$run_dir"
    nohup env MATRIX_WORKER=1 MATRIX_RUN_DIR="$run_dir" "$SCRIPT_PATH" "$PHASE" \
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

printf 'phase\tcase_id\trepetition\tmode_order\tchildren\tpayload_per_child\tchunk_bytes\tstatic_window_bytes\tunit_bytes\ttopk\tstarted_at\tfinished_at\texit_code\toutcome\tconfig\tlog\n' >"$MANIFEST"

write_config() {
    local destination=$1 children=$2 payload=$3 chunk=$4 window=$5 topk=$6 mode_order=$7
    sed -E \
        -e "s/^    static_window_bytes:.*/    static_window_bytes: $window/" \
        -e "s/^  child_processes:.*/  child_processes: $children/" \
        -e "s/^  workflow:.*/  workflow: ordered_topk/" \
        -e "s/^  total_payload_bytes_per_child:.*/  total_payload_bytes_per_child: $payload/" \
        -e "s/^  stream_chunk_bytes:.*/  stream_chunk_bytes: $chunk/" \
        -e "s/^  per_unit_bytes:.*/  per_unit_bytes: 256/" \
        -e "s/^  global_topk:.*/  global_topk: $topk/" \
        -e "s/^  result_distribution:.*/  result_distribution: interleaved/" \
        -e "s/^  concurrency:.*/  concurrency: 1/" \
        -e "s/^  warmup_requests:.*/  warmup_requests: 3/" \
        -e "s/^  measured_requests:.*/  measured_requests: 10/" \
        -e "s/^  minimum_measurement_duration_ms:.*/  minimum_measurement_duration_ms: 1000/" \
        -e "s/^  mode_order:.*/  mode_order: $mode_order/" \
        -e "s/^  request_timeout_ms:.*/  request_timeout_ms: 120000/" \
        "$REPO_ROOT/config.yaml" >"$destination"
}

validate_log() {
    local log_path=$1 children=$2 topk=$3 window=$4
    grep -q "^workflow=.* static_window_bytes=$window " "$log_path"
    for mode in unary streaming; do
        grep -q "^transfer mode=$mode .*emitted_units_per_operation=${topk}.00 " "$log_path"
        grep -q "^application_retention mode=$mode " "$log_path"
        for (( child = 0; child < children; child++ )); do
            [[ $(grep -c "^child_transfer mode=$mode child=$child " "$log_path") -eq 1 ]]
            [[ $(grep -c "^grpc_receive mode=$mode child=$child " "$log_path") -eq 1 ]]
            [[ $(grep -c "^transport mode=$mode child=$child .*child_tcp_info_available=true parent_tcp_info_available=true" "$log_path") -eq 1 ]]
        done
    done
}

run_case() {
    local case_id=$1 children=$2 payload=$3 chunk=$4 window=$5
    local topk=$(( payload / 256 )) repetition mode_order stem config_path log_path
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
        write_config "$config_path" "$children" "$payload" "$chunk" "$window" "$topk" "$mode_order"

        started_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
        printf '[%s] starting %s repetition %d (%s)\n' \
            "$started_at" "$case_id" "$repetition" "$mode_order"
        set +e
        "$BINARY" --config "$config_path" >"$log_path" 2>&1
        exit_code=$?
        if [[ $exit_code -eq 0 ]] && ! validate_log "$log_path" "$children" "$topk" "$window"; then
            printf 'required correctness or transport metrics are missing\n' >>"$log_path"
            exit_code=1
        fi
        set -e

        finished_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
        outcome=passed
        if [[ $exit_code -ne 0 ]]; then
            outcome=failed
        fi
        printf '%s\t%s\t%d\t%s\t%d\t%d\t%d\t%d\t256\t%d\t%s\t%s\t%d\t%s\t%s\t%s\n' \
            "$PHASE" "$case_id" "$repetition" "$mode_order" "$children" \
            "$payload" "$chunk" "$window" "$topk" "$started_at" "$finished_at" \
            "$exit_code" "$outcome" "$config_path" "$log_path" >>"$MANIFEST"
        if [[ $exit_code -ne 0 ]]; then
            cat "$log_path"
            return "$exit_code"
        fi
    done
}

CASES=()
add_case() {
    CASES+=("$1|$2|$3|$4|$5")
}

build_phase_a() {
    local children
    for children in 1 2 4 8 16 32; do
        add_case "A-FANIN-N${children}" "$children" 16777216 262144 0
    done
    add_case A-TOPK-P1 8 1048576 262144 0
    add_case A-TOPK-P4 8 4194304 262144 0
    add_case A-TOPK-P32 8 33554432 262144 0
    add_case A-TOPK-P60 8 62914560 262144 0
    add_case A-CHUNK-C16K 8 16777216 16384 0
    add_case A-CHUNK-C64K 8 16777216 65536 0
    add_case A-CHUNK-C1M 8 16777216 1048576 0
    add_case A-CHUNK-C4M 8 16777216 4194304 0
    add_case A-WINDOW-W64K 8 16777216 262144 65536
    add_case A-WINDOW-W256K 8 16777216 262144 262144
    add_case A-WINDOW-W1M 8 16777216 262144 1048576
    add_case A-WINDOW-W4M 8 16777216 262144 4194304
    add_case A-WINDOW-W16M 8 16777216 262144 16777216
}

build_phase_b() {
    local children payload_label payload chunk_label chunk
    for children in 2 8 32; do
        for payload_label in 4 16 60; do
            payload=$(( payload_label * 1048576 ))
            for chunk_label in 64K 256K 1M; do
                case "$chunk_label" in
                    64K) chunk=65536 ;;
                    256K) chunk=262144 ;;
                    1M) chunk=1048576 ;;
                esac
                add_case "B-N${children}-P${payload_label}-C${chunk_label}" \
                    "$children" "$payload" "$chunk" 0
            done
        done
    done
}

build_phase_c() {
    local children payload_label payload chunk_label chunk window_label window
    for window_label in 64K 1M 16M; do
        case "$window_label" in
            64K) window=65536 ;;
            1M) window=1048576 ;;
            16M) window=16777216 ;;
        esac
        for children in 2 8 32; do
            add_case "C-FANIN-N${children}-W${window_label}" "$children" 16777216 262144 "$window"
        done
        for payload_label in 4 60; do
            payload=$(( payload_label * 1048576 ))
            add_case "C-TOPK-P${payload_label}-W${window_label}" 8 "$payload" 262144 "$window"
        done
        for chunk_label in 64K 1M; do
            case "$chunk_label" in
                64K) chunk=65536 ;;
                1M) chunk=1048576 ;;
            esac
            add_case "C-CHUNK-C${chunk_label}-W${window_label}" 8 16777216 "$chunk" "$window"
        done
    done
}

case "$PHASE" in
    phase_a) build_phase_a; expected_cases=19 ;;
    phase_b) build_phase_b; expected_cases=27 ;;
    phase_c) build_phase_c; expected_cases=21 ;;
esac
if [[ ${#CASES[@]} -ne $expected_cases ]]; then
    printf '%s generated %d cases, expected %d\n' \
        "$PHASE" "${#CASES[@]}" "$expected_cases" >&2
    exit 1
fi

for definition in "${CASES[@]}"; do
    IFS='|' read -r case_id children payload chunk window <<<"$definition"
    run_case "$case_id" "$children" "$payload" "$chunk" "$window"
done

printf '[%s] %s completed\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$PHASE"
