#!/usr/bin/env bash
set -euo pipefail

# Sweep worker counts across test cases using hyperfine.
#
# Usage:
#   ./bench_sweep.sh                          # all cases
#   ./bench_sweep.sh --case flat_100k         # single case
#   ./bench_sweep.sh --case flat_100k,flat_1m # multiple cases
#   ./bench_sweep.sh --workers 1,2,4,8        # custom worker list
#
# Examples:
#   ./bench_sweep.sh --case flat_100k --runs 10
#   ./bench_sweep.sh --workers 1,2,4,8,16,24,32

# Find project root (this script lives in .pi/skills/fileproc-benchmark/scripts/)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
ROOT_DIR="$(cd "$SCRIPT_DIR/../../../.." && pwd -P)"
cd "$ROOT_DIR"

if ! command -v hyperfine >/dev/null 2>&1; then
  echo "hyperfine not found" >&2
  exit 1
fi

# Defaults
RUNS=10
WARMUP=3
WORKERS_LIST=""  # empty = use defaults per case
WORKERS_DEFAULT="1,2,4,8,16"
WORKERS_DEFAULT_LARGE="6,8,12,16,24"  # 100k/1m: skip low worker counts
CASES=""
PROCESS="frontmatter"
VERBOSE=false

# Small datasets are very fast, so run more iterations for stability.
# Multipliers are applied to RUNS.
MULT_1K=20
MULT_5K=10

BIN="./cmd/fileprocbench/fileprocbench"

# All available cases
ALL_CASES="flat_1k,nested1_1k,flat_5k,nested1_5k,flat_100k,nested1_100k,flat_1m,nested1_1m"

usage() {
  echo "Usage: ./bench_sweep.sh [options]"
  echo ""
  echo "Options:"
  echo "  --case CASES      Comma-separated cases (default: all)"
  echo "  --workers LIST    Comma-separated worker counts (overrides defaults)"
  echo "  --runs N          Base runs per combination (default: $RUNS)"
  echo "  --warmup N        Warmup runs (default: $WARMUP)"
  echo "  --process NAME    Process mode: frontmatter | noop (default: $PROCESS)"
  echo "  --verbose, -v     Show full hyperfine output (default: summary table only)"
  echo ""
  echo "Defaults per case size:"
  echo "  1k/5k workers:    $WORKERS_DEFAULT"
  echo "  100k/1m workers:  $WORKERS_DEFAULT_LARGE"
  echo "  1k runs:          base × $MULT_1K"
  echo "  5k runs:          base × $MULT_5K"
  echo "  100k/1m runs:     base × 1"
  echo ""
  echo "Available cases:"
  echo "  flat_1k, nested1_1k, flat_5k, nested1_5k,"
  echo "  flat_100k, nested1_100k, flat_1m, nested1_1m"
  echo ""
  echo "Examples:"
  echo "  ./bench_sweep.sh --case flat_100k"
  echo "  ./bench_sweep.sh --case flat_100k,flat_1m"
  echo "  ./bench_sweep.sh --case flat_100k -v       # verbose/interactive"
  echo "  ./bench_sweep.sh --workers 4,8,16,24,32    # override for all"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --case) CASES="$2"; shift 2 ;;
    --workers) WORKERS_LIST="$2"; shift 2 ;;
    --runs) RUNS="$2"; shift 2 ;;
    --warmup) WARMUP="$2"; shift 2 ;;
    --process) PROCESS="$2"; shift 2 ;;
    --verbose|-v) VERBOSE=true; shift 1 ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "error: unknown option: $1" >&2
      echo >&2
      usage >&2
      exit 2
      ;;
  esac
done

# Default to all cases if not specified
if [[ -z "$CASES" ]]; then
  CASES="$ALL_CASES"
fi

# Resolve case name to directory path
resolve_case() {
  local c="$1"
  case "$c" in
    flat_1k)      echo ".data/tickets_flat_1k" ;;
    nested1_1k)   echo ".data/tickets_nested1_1k" ;;
    flat_5k)      echo ".data/tickets_flat_5k" ;;
    nested1_5k)   echo ".data/tickets_nested1_5k" ;;
    flat_100k)    echo ".data/tickets_flat_100k" ;;
    nested1_100k) echo ".data/tickets_nested1_100k" ;;
    flat_1m)      echo ".data/tickets_flat_1m" ;;
    nested1_1m)   echo ".data/tickets_nested1_1m" ;;
    *) echo "" ;;
  esac
}

# Check if case needs -tree flag
needs_tree() {
  local c="$1"
  [[ "$c" == nested1_* ]]
}

# Validate all cases exist
IFS=',' read -ra CASE_ARR <<< "$CASES"
for c in "${CASE_ARR[@]}"; do
  dir=$(resolve_case "$c")
  if [[ -z "$dir" ]]; then
    echo "unknown case: $c" >&2
    exit 2
  fi
  if [[ ! -d "$dir" ]]; then
    echo "missing dataset: $dir" >&2
    echo "Generate with: make data" >&2
    exit 1
  fi
done

SEP="============================================================"

echo
echo "$SEP"
echo "fileproc worker sweep"
echo "$SEP"
echo
echo "cases:       $CASES"
if [[ -n "$WORKERS_LIST" ]]; then
  echo "workers:     $WORKERS_LIST (custom)"
else
  echo "workers:     (per-case defaults)"
fi
echo "runs_base:   $RUNS (warmup: $WARMUP)"
echo "runs_1k:     $((RUNS * MULT_1K)) (base × $MULT_1K)"
echo "runs_5k:     $((RUNS * MULT_5K)) (base × $MULT_5K)"
echo "process:     $PROCESS"
echo

echo "building..."
make build
echo

echo "$SEP"
echo "running sweep..."
echo "$SEP"
echo

# Get runs for a case (small cases need more runs for stability)
runs_for_case() {
  local c="$1"
  case "$c" in
    *_1k) echo $((RUNS * MULT_1K)) ;;
    *_5k) echo $((RUNS * MULT_5K)) ;;
    *)    echo "$RUNS" ;;
  esac
}

# Get workers list for a case (large cases skip low worker counts)
workers_for_case() {
  local c="$1"
  # If user specified --workers, use that
  if [[ -n "$WORKERS_LIST" ]]; then
    echo "$WORKERS_LIST"
    return
  fi
  # Otherwise use defaults based on case size
  case "$c" in
    *_100k|*_1m) echo "$WORKERS_DEFAULT_LARGE" ;;
    *)           echo "$WORKERS_DEFAULT" ;;
  esac
}

# Build hyperfine commands for each case
# We run each case separately to handle -tree flag, run multipliers, and worker lists
for c in "${CASE_ARR[@]}"; do
  dir=$(resolve_case "$c")
  case_runs=$(runs_for_case "$c")
  case_workers=$(workers_for_case "$c")
  
  tree_flag=""
  if needs_tree "$c"; then
    tree_flag="-tree"
  fi
  
  echo "--- $c ---"
  
  # Build command array with -n names for clean output
  hf_args=(
    --shell=none
    --warmup "$WARMUP"
    --runs "$case_runs"
    --sort command
    --output=pipe
  )
  
  tmp_json=""
  if [[ "$VERBOSE" == "true" ]]; then
    hf_args+=(--style full)
  else
    tmp_json=$(mktemp)
    hf_args+=(--style none --export-json "$tmp_json")
  fi
  
  # Add each worker count with a short name
  IFS=',' read -ra WORKER_ARR <<< "$case_workers"
  for w in "${WORKER_ARR[@]}"; do
    hf_args+=(-n "w=$w" "$BIN -dir $dir $tree_flag -process $PROCESS -workers $w")
  done
  
  hyperfine "${hf_args[@]}"
  
  if [[ -n "$tmp_json" ]]; then
    jq -r '
      (.results | min_by(.mean).mean) as $best |
      (
        ["Command", "Mean", "Min", "Max", "Relative", ""],
        ["-------", "----", "---", "---", "--------", ""],
        (.results[] | 
          (.mean / $best | . * 100 | round / 100) as $rel |
          [
            .command,
            "\(.mean * 1000 | . * 10 | round / 10) ms",
            "\(.min * 1000 | . * 10 | round / 10) ms",
            "\(.max * 1000 | . * 10 | round / 10) ms",
            "\($rel)",
            (if $rel == 1 then "<== best" else "" end)
          ]
        )
      ) | @tsv
    ' "$tmp_json" | column -t -s$'\t'
    rm -f "$tmp_json"
  fi
  echo
done

echo "$SEP"
echo "sweep complete"
echo "$SEP"
