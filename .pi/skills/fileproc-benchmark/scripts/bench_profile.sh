#!/usr/bin/env bash
set -euo pipefail

# Profile a benchmark case with pprof or strace.
#
# Usage:
#   ./bench_profile.sh --case flat_100k --cpu           # CPU profile
#   ./bench_profile.sh --case flat_100k --mem           # heap profile
#   ./bench_profile.sh --case flat_100k --strace        # syscall analysis
#   ./bench_profile.sh --case flat_100k --all           # everything
#
# Options:
#   --case CASE       Test case (flat_1k, nested1_100k, flat_1m, etc.)
#   --all             Enable all profiling (cpu + mem + strace)
#   --cpu             Generate CPU profile
#   --mem             Generate memory/heap profile
#   --strace          Syscall analysis (summary + full trace to file)
#   --repeat N        Iterations per run (default: 10, more = better profiles)
#   --workers N       Worker count (default: fileprocbench default)
#   --gc N            GC percent (default: fileprocbench default)
#   --process NAME    Process mode: bytes | read | stat (default: bytes)
#   --out DIR         Output directory (default: .benchmarks/profiles)

# Find project root (this script lives in .pi/skills/fileproc-benchmark/scripts/)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
ROOT_DIR="$(cd "$SCRIPT_DIR/../../../.." && pwd -P)"
cd "$ROOT_DIR"

# Defaults
CASE=""
ALL=false
CPU=false
MEM=false
STRACE=false
REPEAT=10
WORKERS=""
SCAN_WORKERS=""
CHUNK_SIZE=""
GC=""
PROCESS="bytes"
OUT_DIR=".benchmarks/profiles"

BIN="./cmd/fileprocbench/fileprocbench"

usage() {
  cat <<EOF
Usage: ./bench_profile.sh --case CASE [options]

Profile a benchmark case with pprof or strace.

Options:
  --case CASE       Test case (required)
  --all             Enable all profiling (cpu + mem + strace)
  --cpu             Generate CPU profile
  --mem             Generate memory/heap profile
  --strace          Syscall analysis (summary + full trace to file)
  --repeat N        Iterations in single process (default: $REPEAT)
  --workers N       Worker count override
  --scan-workers N  Scan worker count override
  --chunk-size N    Entries per chunk override
  --gc N            GC percent override
  --process NAME    Process mode: bytes | read | stat (default: $PROCESS)
  --out DIR         Output directory (default: $OUT_DIR)
  -h, --help        Show this help

Available cases:
  flat_1k, nested1_1k, flat_5k, nested1_5k,
  flat_100k, nested1_100k, flat_1m, nested1_1m

Examples:
  ./bench_profile.sh --case flat_100k --cpu
  ./bench_profile.sh --case flat_100k --all
  ./bench_profile.sh --case flat_100k --cpu --mem --repeat 20
  ./bench_profile.sh --case flat_1m --strace
  ./bench_profile.sh --case flat_100k --cpu --workers 8

EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --case) CASE="$2"; shift 2 ;;
    --all) ALL=true; shift ;;
    --cpu) CPU=true; shift ;;
    --mem) MEM=true; shift ;;
    --strace) STRACE=true; shift ;;
    --repeat) REPEAT="$2"; shift 2 ;;
    --workers) WORKERS="$2"; shift 2 ;;
    --scan-workers) SCAN_WORKERS="$2"; shift 2 ;;
    --chunk-size) CHUNK_SIZE="$2"; shift 2 ;;
    --gc) GC="$2"; shift 2 ;;
    --process) PROCESS="$2"; shift 2 ;;
    --out) OUT_DIR="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *)
      echo "error: unknown option: $1" >&2
      echo >&2
      usage >&2
      exit 2
      ;;
  esac
done

# --all enables everything
if [[ "$ALL" == "true" ]]; then
  CPU=true
  MEM=true
  STRACE=true
fi

if [[ -z "$CASE" ]]; then
  echo "error: --case is required" >&2
  echo >&2
  usage >&2
  exit 2
fi

# At least one profile mode required
if [[ "$CPU" == "false" && "$MEM" == "false" && "$STRACE" == "false" ]]; then
  echo "error: specify at least one of --all, --cpu, --mem, --strace" >&2
  exit 2
fi

# Resolve case to directory
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

needs_tree() {
  [[ "$1" == nested1_* ]]
}

DIR=$(resolve_case "$CASE")
if [[ -z "$DIR" ]]; then
  echo "error: unknown case: $CASE" >&2
  exit 2
fi

if [[ ! -d "$DIR" ]]; then
  echo "error: missing dataset: $DIR" >&2
  echo "Generate with: make data" >&2
  exit 1
fi

# Build
echo "Building..."
make build >/dev/null
echo

# Create output directory
mkdir -p "$OUT_DIR"

# Timestamp for unique filenames
STAMP=$(date +"%Y%m%d-%H%M%S")

# Build base command
CMD=("$BIN" -dir "$DIR" -repeat "$REPEAT" -process "$PROCESS")

if needs_tree "$CASE"; then
  CMD+=(-tree)
fi

if [[ -n "$WORKERS" ]]; then
  CMD+=(-workers "$WORKERS")
fi

if [[ -n "$SCAN_WORKERS" ]]; then
  CMD+=(-scan-workers "$SCAN_WORKERS")
fi

if [[ -n "$CHUNK_SIZE" ]]; then
  CMD+=(-chunk-size "$CHUNK_SIZE")
fi

if [[ -n "$GC" ]]; then
  CMD+=(-gc "$GC")
fi

# Profile paths
CPU_PROF="$OUT_DIR/${CASE}-${STAMP}-cpu.prof"
MEM_PROF="$OUT_DIR/${CASE}-${STAMP}-mem.prof"
STRACE_OUT="$OUT_DIR/${CASE}-${STAMP}-strace.log"

SEP="============================================================"

echo "$SEP"
echo "Profiling: $CASE"
echo "$SEP"
echo
echo "  dir:      $DIR"
echo "  repeat:   $REPEAT"
echo "  workers:  ${WORKERS:-<default>}"
echo "  scan_workers: ${SCAN_WORKERS:-<default>}"
echo "  chunk_size: ${CHUNK_SIZE:-<default>}"
echo "  gc:       ${GC:-<default>}"
echo "  process:  ${PROCESS}"
echo

# Run pprof profiles (can do both in one run)
if [[ "$CPU" == "true" || "$MEM" == "true" ]]; then
  PROF_CMD=("${CMD[@]}")
  
  if [[ "$CPU" == "true" ]]; then
    PROF_CMD+=(-cpuprofile "$CPU_PROF")
  fi
  
  if [[ "$MEM" == "true" ]]; then
    PROF_CMD+=(-memprofile "$MEM_PROF")
  fi
  
  echo "Running pprof profiling..."
  echo "  ${PROF_CMD[*]}"
  echo
  
  "${PROF_CMD[@]}"
  
  echo
  
  if [[ "$CPU" == "true" ]]; then
    echo "CPU profile: $CPU_PROF"
    echo
    echo "Top functions by CPU:"
    go tool pprof -top "$CPU_PROF" 2>/dev/null | head -25
    echo
  fi
  
  if [[ "$MEM" == "true" ]]; then
    echo "Memory profile: $MEM_PROF"
    echo
    echo "Top allocations:"
    go tool pprof -top "$MEM_PROF" 2>/dev/null | head -25
    echo
  fi
fi

# Run strace (separate run since it adds overhead)
if [[ "$STRACE" == "true" ]]; then
  if ! command -v strace >/dev/null 2>&1; then
    echo "error: strace not found (Linux only)" >&2
    exit 1
  fi
  
  echo "Running strace..."
  echo "  strace -f -C -o $STRACE_OUT ${CMD[*]}"
  echo
  
  # -f follows threads, -C writes full trace + summary at end of file
  strace -f -C -o "$STRACE_OUT" "${CMD[@]}"
  
  echo
  # Print summary (last 30 lines of trace file)
  echo "Syscall summary:"
  grep -A 100 "^% time" "$STRACE_OUT" || tail -30 "$STRACE_OUT"
  
  echo
  echo "Full trace: $STRACE_OUT"
  echo "  View: less $STRACE_OUT"
  echo
fi

echo "$SEP"
echo "Done"
echo "$SEP"
