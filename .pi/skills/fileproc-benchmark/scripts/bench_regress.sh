#!/usr/bin/env bash
set -euo pipefail

# Stable regression benchmark suite using hyperfine.
#
# Goals:
# - Provide a stable(ish) "did we regress?" signal.
# - Fail fast on any correctness issue via fileprocbench's -expect.
# - Produce self-contained artifacts under `.benchmarks/`.
#
# Optional stability helpers (Linux/macOS):
#   --cpu-set 0-7                    Pin benchmark process to specific CPUs (Linux: taskset)
#   --require-ac                     Fail if running on battery power
#   --require-performance-governor   Fail if Linux CPU governor != performance
#
# Outputs (under a run directory):
#   .benchmarks/regress-<timestamp>[-<tag>]/
#     meta.txt
#     hyperfine-1k.json   / .md
#     hyperfine-5k.json   / .md
#     hyperfine-100k.json / .md
#     hyperfine-1m.json   / .md

# Find project root (this script lives in .pi/skills/fileproc-benchmark/scripts/)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
ROOT_DIR="$(cd "$SCRIPT_DIR/../../../.." && pwd -P)"
cd "$ROOT_DIR"

if ! command -v hyperfine >/dev/null 2>&1; then
  echo "hyperfine not found" >&2
  exit 1
fi

ORIG_ARGS=("$@")

RUNS=10
WARMUP=5

# Small datasets are very fast, so run more iterations for stability.
# Multipliers are applied to RUNS.
SMALL_RUN_MULT_1K=20
SMALL_RUN_MULT_5K=10

TAG=""

# Build command/binary used for benchmarking.
# We build once up-front and then benchmark the default binary.
BIN="./cmd/fileprocbench/fileprocbench"
FLAT_1K=".data/tickets_flat_1k"
NESTED_1K=".data/tickets_nested1_1k"
FLAT_5K=".data/tickets_flat_5k"
NESTED_5K=".data/tickets_nested1_5k"
FLAT_100K=".data/tickets_flat_100k"
NESTED_100K=".data/tickets_nested1_100k"
FLAT_1M=".data/tickets_flat_1m"
NESTED_1M=".data/tickets_nested1_1m"

# Use fileprocbench defaults unless explicitly overridden.
# If these are left empty, the corresponding flag is not passed.
#
# You can also override via environment variables, e.g.
#   WORKERS=16 ./bench_regress.sh
WORKERS="${WORKERS-}" # -workers
READ="${READ-}"       # -read
GC="${GC-}"           # -gc

EXPECT_1K=1000
EXPECT_5K=5000
EXPECT_100K=100000
EXPECT_1M=1000000

# Optional: pin GOMAXPROCS for the benchmark process.
GOMAXPROCS=""

# Optional stability helpers.
CPU_SET=""
REQUIRE_AC=false
REQUIRE_PERF_GOV=false
VERBOSE=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --runs) RUNS="$2"; shift 2 ;;
    --warmup) WARMUP="$2"; shift 2 ;;

    --tag) TAG="$2"; shift 2 ;;

    --workers) WORKERS="$2"; shift 2 ;;

    --flat-1k) FLAT_1K="$2"; shift 2 ;;
    --nested-1k) NESTED_1K="$2"; shift 2 ;;
    --flat-5k) FLAT_5K="$2"; shift 2 ;;
    --nested-5k) NESTED_5K="$2"; shift 2 ;;

    --flat-100k) FLAT_100K="$2"; shift 2 ;;
    --nested-100k) NESTED_100K="$2"; shift 2 ;;
    --flat-1m) FLAT_1M="$2"; shift 2 ;;
    --nested-1m) NESTED_1M="$2"; shift 2 ;;

    --read) READ="$2"; shift 2 ;;

    --expect-1k) EXPECT_1K="$2"; shift 2 ;;
    --expect-5k) EXPECT_5K="$2"; shift 2 ;;
    --expect-100k) EXPECT_100K="$2"; shift 2 ;;
    --expect-1m) EXPECT_1M="$2"; shift 2 ;;

    --gc) GC="$2"; shift 2 ;;
    --gomaxprocs) GOMAXPROCS="$2"; shift 2 ;;

    --cpu-set) CPU_SET="$2"; shift 2 ;;
    --require-ac) REQUIRE_AC=true; shift 1 ;;
    --require-performance-governor) REQUIRE_PERF_GOV=true; shift 1 ;;
    --verbose|-v) VERBOSE=true; shift 1 ;;

    *)
      echo "unknown arg: $1" >&2
      exit 2
      ;;
  esac
done

for d in "$FLAT_1K" "$NESTED_1K" "$FLAT_5K" "$NESTED_5K" "$FLAT_100K" "$NESTED_100K" "$FLAT_1M" "$NESTED_1M"; do
  if [[ ! -d "$d" ]]; then
    echo "missing dataset directory: $d" >&2
    echo "Generate datasets with ./ticketgen." >&2
    exit 1
  fi
done

mkdir -p .benchmarks

# Use an ISO-8601-ish timestamp that is filename-safe (no ':' characters).
# Example: 2026-01-17T16-38-44+0100
STAMP=$(date +"%Y-%m-%dT%H-%M-%S%z")
TAG_SAFE="${TAG// /_}"
RUN_DIR=".benchmarks/regress-${STAMP}"
if [[ -n "$TAG_SAFE" ]]; then
  RUN_DIR+="-${TAG_SAFE}"
fi
mkdir -p "$RUN_DIR"

OUT_JSON_1K="$RUN_DIR/hyperfine-1k.json"
OUT_MD_1K="$RUN_DIR/hyperfine-1k.md"
OUT_JSON_5K="$RUN_DIR/hyperfine-5k.json"
OUT_MD_5K="$RUN_DIR/hyperfine-5k.md"
OUT_JSON_100K="$RUN_DIR/hyperfine-100k.json"
OUT_MD_100K="$RUN_DIR/hyperfine-100k.md"
OUT_JSON_1M="$RUN_DIR/hyperfine-1m.json"
OUT_MD_1M="$RUN_DIR/hyperfine-1m.md"

ENV_PREFIX=()
if [[ -n "$GOMAXPROCS" ]]; then
  ENV_PREFIX=(env "GOMAXPROCS=$GOMAXPROCS")
fi

AFFINITY_PREFIX=()
if [[ -n "$CPU_SET" ]]; then
  if command -v taskset >/dev/null 2>&1; then
    AFFINITY_PREFIX=(taskset -c "$CPU_SET")
  else
    echo "--cpu-set provided but taskset not found" >&2
    exit 1
  fi
fi

# -----------------------------------------------------------------------------
# Environment checks (noise reduction / reproducibility)
# -----------------------------------------------------------------------------

POWER_SOURCE="unknown" # ac|battery|unknown
POWER_DETAIL=""
CPU_GOVERNOR=""
if [[ "$(uname -s)" == "Linux" ]]; then
  if [[ -d /sys/class/power_supply ]]; then
    ac_online=""
    for ps in /sys/class/power_supply/*; do
      [[ -f "$ps/type" ]] || continue
      t=$(<"$ps/type")
      if [[ "$t" == "Mains" || "$t" == "AC" ]]; then
        if [[ -f "$ps/online" ]]; then
          v=$(<"$ps/online")
          if [[ "$v" == "1" ]]; then
            ac_online="1"
            break
          elif [[ "$v" == "0" ]]; then
            ac_online="0"
          fi
        fi
      fi
    done

    if [[ "$ac_online" == "1" ]]; then
      POWER_SOURCE="ac"
    elif [[ "$ac_online" == "0" ]]; then
      POWER_SOURCE="battery"
    fi

    for ps in /sys/class/power_supply/*; do
      [[ -f "$ps/type" ]] || continue
      t=$(<"$ps/type")
      if [[ "$t" == "Battery" ]]; then
        if [[ -f "$ps/status" ]]; then
          POWER_DETAIL="status=$(<"$ps/status")"
        fi
        if [[ -f "$ps/capacity" ]]; then
          if [[ -n "$POWER_DETAIL" ]]; then POWER_DETAIL+=" "; fi
          POWER_DETAIL+="capacity=$(<"$ps/capacity")%"
        fi
        break
      fi
    done
  fi

  if [[ -f /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor ]]; then
    CPU_GOVERNOR=$(</sys/devices/system/cpu/cpu0/cpufreq/scaling_governor)
  fi
elif [[ "$(uname -s)" == "Darwin" ]]; then
  if command -v pmset >/dev/null 2>&1; then
    if pmset -g batt 2>/dev/null | grep -q "AC Power"; then
      POWER_SOURCE="ac"
    elif pmset -g batt 2>/dev/null | grep -q "Battery Power"; then
      POWER_SOURCE="battery"
    fi
    POWER_DETAIL=$(pmset -g batt 2>/dev/null | tr '\n' ' ' | sed 's/  */ /g' || true)
  fi
fi

if [[ "$POWER_SOURCE" == "battery" ]]; then
  echo "WARNING: running on battery power; results may be noisier/slower. $POWER_DETAIL" >&2
  if [[ "$REQUIRE_AC" == "true" ]]; then
    echo "ERROR: --require-ac set and we appear to be on battery." >&2
    exit 2
  fi
fi

# On intel_pstate/amd_pstate, scaling_governor may stay 'powersave' even in
# performance mode. The real indicator is energy_performance_preference (EPP).
EPP=""
if [[ -f /sys/devices/system/cpu/cpu0/cpufreq/energy_performance_preference ]]; then
  EPP=$(</sys/devices/system/cpu/cpu0/cpufreq/energy_performance_preference)
fi

if [[ -n "$CPU_GOVERNOR" && "$CPU_GOVERNOR" != "performance" ]]; then
  # Accept EPP=performance as valid performance mode (intel_pstate/amd_pstate)
  if [[ "$EPP" != "performance" ]]; then
    echo "WARNING: Linux CPU governor is '$CPU_GOVERNOR' (expected 'performance' for stable benchmarks)." >&2
    if [[ "$REQUIRE_PERF_GOV" == "true" ]]; then
      echo "ERROR: --require-performance-governor set and governor != performance." >&2
      exit 2
    fi
  fi
fi

# -----------------------------------------------------------------------------
# Metadata
# -----------------------------------------------------------------------------
META="$RUN_DIR/meta.txt"
{
  echo "ts: $(date +"%Y-%m-%dT%H:%M:%S%z")"

  echo -n "args:"
  for a in "${ORIG_ARGS[@]}"; do
    printf ' %q' "$a"
  done
  echo

  echo "cwd: $ROOT_DIR"
  echo "bench_bin: $BIN"
  echo "go: $(go version 2>/dev/null || true)"
  echo "hyperfine: $(hyperfine --version 2>/dev/null | head -n1 || true)"
  echo "uname: $(uname -a 2>/dev/null || true)"

  if command -v git >/dev/null 2>&1 && git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
    echo "git_rev: $(git rev-parse HEAD 2>/dev/null || true)"
    if git diff --quiet --no-ext-diff 2>/dev/null; then
      echo "git_dirty: false"
    else
      echo "git_dirty: true"
    fi
  fi

  echo "workers: ${WORKERS:-<default>}"
  echo "gc: ${GC:-<default>}"
  echo "read: ${READ:-<default>}"
  echo "gomaxprocs: ${GOMAXPROCS:-<default>}"
  echo "cpu_set: ${CPU_SET:-<none>}"
  echo "power_source: $POWER_SOURCE"
  echo "power_detail: $POWER_DETAIL"
  echo "cpu_governor: ${CPU_GOVERNOR:-<unknown>}"
  echo "energy_perf_pref: ${EPP:-<unknown>}"
  echo "expect_1k: $EXPECT_1K"
  echo "expect_5k: $EXPECT_5K"
  echo "expect_100k: $EXPECT_100K"
  echo "expect_1m: $EXPECT_1M"

  if command -v lscpu >/dev/null 2>&1; then
    echo
    echo "--- lscpu ---"
    lscpu || true
  fi

  if [[ "$(uname -s)" == "Darwin" ]] && command -v sysctl >/dev/null 2>&1; then
    echo
    echo "--- sysctl (selected) ---"
    sysctl -n machdep.cpu.brand_string 2>/dev/null || true
    sysctl -n hw.ncpu 2>/dev/null || true
    sysctl -n hw.memsize 2>/dev/null || true
  fi
} >"$META"

# -----------------------------------------------------------------------------
# Command templates
# -----------------------------------------------------------------------------
prefix_cmd() {
  local parts=()
  if [[ ${#AFFINITY_PREFIX[@]} -gt 0 ]]; then
    parts+=("${AFFINITY_PREFIX[@]}")
  fi
  if [[ ${#ENV_PREFIX[@]} -gt 0 ]]; then
    parts+=("${ENV_PREFIX[@]}")
  fi
  printf "%s" "${parts[*]}"
}

PREFIX="$(prefix_cmd)"
if [[ -n "$PREFIX" ]]; then
  PREFIX="$PREFIX "
fi

cmd_for() {
  # Args: dir tree expect
  local dir="$1"; shift
  local tree="$1"; shift
  local expect="$1"; shift

  local tree_flag=""
  if [[ "$tree" == "true" ]]; then
    tree_flag="-tree"
  fi

  # Omit flags we want to keep at fileprocbench defaults.
  local cmd="${PREFIX}$BIN -dir $dir $tree_flag -process {process} -expect $expect"

  if [[ -n "$READ" ]]; then
    cmd+=" -read $READ"
  fi
  if [[ -n "$WORKERS" ]]; then
    cmd+=" -workers $WORKERS"
  fi
  if [[ -n "$GC" ]]; then
    cmd+=" -gc $GC"
  fi

  echo "$cmd"
}

CMD_FLAT_1K=$(cmd_for "$FLAT_1K" false "$EXPECT_1K")
CMD_NESTED_1K=$(cmd_for "$NESTED_1K" true "$EXPECT_1K")
CMD_FLAT_5K=$(cmd_for "$FLAT_5K" false "$EXPECT_5K")
CMD_NESTED_5K=$(cmd_for "$NESTED_5K" true "$EXPECT_5K")
CMD_FLAT_100K=$(cmd_for "$FLAT_100K" false "$EXPECT_100K")
CMD_NESTED_100K=$(cmd_for "$NESTED_100K" true "$EXPECT_100K")
CMD_FLAT_1M=$(cmd_for "$FLAT_1M" false "$EXPECT_1M")
CMD_NESTED_1M=$(cmd_for "$NESTED_1M" true "$EXPECT_1M")

# -----------------------------------------------------------------------------
# Run hyperfine
# -----------------------------------------------------------------------------

SEP="============================================================"

echo
echo "$SEP"
echo "fileproc regression benchmark"
echo "$SEP"
echo
echo "run_dir:    $RUN_DIR"
echo "runs_base:  $RUNS (warmup: $WARMUP)"
echo "runs_1k:    $((RUNS*SMALL_RUN_MULT_1K)) (base*$SMALL_RUN_MULT_1K)"
echo "runs_5k:    $((RUNS*SMALL_RUN_MULT_5K)) (base*$SMALL_RUN_MULT_5K)"
echo "workers:    ${WORKERS:-<default>}"
echo "read:       ${READ:-<default>}"
echo "gc:         ${GC:-<default>}"
echo "gomaxprocs: ${GOMAXPROCS:-<default>}"
echo "cpu_set:    ${CPU_SET:-<none>}"
echo

echo "building..."
# Build once up-front (instead of hyperfine --setup) so we don't rebuild per run.
make build
echo

# Display clean table from hyperfine JSON
display_results() {
  local json_file="$1"
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
  ' "$json_file" | column -t -s$'\t'
}

HF_COMMON_BASE=(--shell=none --sort command --output=pipe)
if [[ "$VERBOSE" == "true" ]]; then
  HF_COMMON_BASE+=(--style full)
else
  HF_COMMON_BASE+=(--style none)
fi
HF_COMMON=("${HF_COMMON_BASE[@]}" --warmup "$WARMUP" --runs "$RUNS")

# Small datasets are very fast, so increase sample size for more stable results.
HF_1K=("${HF_COMMON_BASE[@]}" --warmup "$WARMUP" --runs "$((RUNS*SMALL_RUN_MULT_1K))")
HF_5K=("${HF_COMMON_BASE[@]}" --warmup "$WARMUP" --runs "$((RUNS*SMALL_RUN_MULT_5K))")

echo "$SEP"
echo "dataset: 1k md files (flat + nested1)"
echo "runs: $((RUNS*SMALL_RUN_MULT_1K)) (warmup: $WARMUP)"
echo "$SEP"
echo

hyperfine "${HF_1K[@]}" \
  -L process frontmatter,noop \
  --export-json "$OUT_JSON_1K" \
  --export-markdown "$OUT_MD_1K" \
  --command-name "flat_1k_frontmatter" \
  --command-name "flat_1k_noop" \
  --command-name "nested1_1k_frontmatter" \
  --command-name "nested1_1k_noop" \
  "$CMD_FLAT_1K" \
  "$CMD_NESTED_1K"

if [[ "$VERBOSE" != "true" ]]; then
  display_results "$OUT_JSON_1K"
fi
echo

echo "$SEP"
echo "dataset: 5k md files (flat + nested1)"
echo "runs: $((RUNS*SMALL_RUN_MULT_5K)) (warmup: $WARMUP)"
echo "$SEP"
echo

hyperfine "${HF_5K[@]}" \
  -L process frontmatter,noop \
  --export-json "$OUT_JSON_5K" \
  --export-markdown "$OUT_MD_5K" \
  --command-name "flat_5k_frontmatter" \
  --command-name "flat_5k_noop" \
  --command-name "nested1_5k_frontmatter" \
  --command-name "nested1_5k_noop" \
  "$CMD_FLAT_5K" \
  "$CMD_NESTED_5K"

if [[ "$VERBOSE" != "true" ]]; then
  display_results "$OUT_JSON_5K"
fi
echo

echo "$SEP"
echo "dataset: 100k md files (flat + nested1)"
echo "runs: $RUNS (warmup: $WARMUP)"
echo "$SEP"
echo

hyperfine "${HF_COMMON[@]}" \
  -L process frontmatter,noop \
  --export-json "$OUT_JSON_100K" \
  --export-markdown "$OUT_MD_100K" \
  --command-name "flat_100k_frontmatter" \
  --command-name "flat_100k_noop" \
  --command-name "nested1_100k_frontmatter" \
  --command-name "nested1_100k_noop" \
  "$CMD_FLAT_100K" \
  "$CMD_NESTED_100K"

if [[ "$VERBOSE" != "true" ]]; then
  display_results "$OUT_JSON_100K"
fi
echo

echo "$SEP"
echo "dataset: 1m md files (flat + nested1)"
echo "runs: $RUNS (warmup: $WARMUP)"
echo "$SEP"
echo

hyperfine "${HF_COMMON[@]}" \
  -L process frontmatter,noop \
  --export-json "$OUT_JSON_1M" \
  --export-markdown "$OUT_MD_1M" \
  --command-name "flat_1m_frontmatter" \
  --command-name "flat_1m_noop" \
  --command-name "nested1_1m_frontmatter" \
  --command-name "nested1_1m_noop" \
  "$CMD_FLAT_1M" \
  "$CMD_NESTED_1M"

if [[ "$VERBOSE" != "true" ]]; then
  display_results "$OUT_JSON_1M"
fi
echo

# -----------------------------------------------------------------------------
# Generate summary and append to history
# -----------------------------------------------------------------------------
echo "$SEP"
echo "Generating summary..."
echo "$SEP"
echo

BENCHREPORT="$ROOT_DIR/cmd/benchreport/benchreport"
if [[ ! -x "$BENCHREPORT" ]]; then
  echo "benchreport not found at $BENCHREPORT" >&2
  echo "Run 'make build' first." >&2
  exit 1
fi

"$BENCHREPORT" summarize "$RUN_DIR"

echo
echo "$SEP"
echo "Benchmark complete!"
echo "$SEP"
echo
echo "Run artifacts: $RUN_DIR"
echo "Summary:       $RUN_DIR/summary.json"
echo "History:       .benchmarks/history.jsonl"
echo
echo "To compare with previous run:"
echo "  ./cmd/benchreport/benchreport compare"
echo
echo "To compare with rolling average 1% max threshold allowed:"
echo "  ./cmd/benchreport/benchreport compare --against avg --n 5 --fail-above 2"
echo
