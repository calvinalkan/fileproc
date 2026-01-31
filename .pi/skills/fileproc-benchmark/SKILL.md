---
name: fileproc-benchmark
description: |
  Benchmark and profile the fileproc library. Run regression tests, compare performance,
  detect regressions, sweep parameters, and profile CPU/memory/syscalls. Use when asked
  about performance, benchmarks, profiling, optimization, or regression testing.
---

# fileproc Benchmarking

## 1. Test Data

Test data is stored in `.data/`. Check available datasets:

```bash
ls .data   # ONE LEVEL ONLY - never run nested ls on .data
```

Available datasets follow the pattern `tickets_{layout}_{size}`:
- **Layouts**: `flat` (single dir), `nested1` (year-month dirs), `nested3` (deep hierarchy)
- **Sizes**: `1k`, `5k`, `100k`, `1m`

If data is missing, generate with `cmd/ticketgen`:

```bash
# Flat 100k files
go run ./cmd/ticketgen --out .data/tickets_flat_100k --files 100k --layout flat

# Nested 100k files (year-month directories)
go run ./cmd/ticketgen --out .data/tickets_nested1_100k --files 100k --layout shallow

# Deep hierarchy
go run ./cmd/ticketgen --out .data/tickets_nested3_1m --files 1m --layout deep
```

## 2. Quick Start

```bash
make bench   # Run regression + compare avg last 5 vs baseline avg, fail if >2% regression
```

## 3. Process Modes

Benchmarks support three process modes via `--process`:

| Mode | What it measures |
|------|------------------|
| `bytes` | `f.Bytes()` — read full file into arena (default) |
| `read` | `f.Read(buf)` — read into worker buffer |
| `stat` | `f.Stat()` — metadata only, no content read |

## 4. Commands Reference

| Task | Command |
|------|---------|
| Run regression benchmarks | `.pi/skills/fileproc-benchmark/scripts/bench_regress.sh` |
| Compare vs previous run | `cmd/benchreport/benchreport compare` |
| Compare read results | `cmd/benchreport/benchreport compare --process read` |
| Compare stat results | `cmd/benchreport/benchreport compare --process stat` |
| Compare avg last 5 vs prev 5 | `cmd/benchreport/benchreport compare --against avg --n 5` |
| Compare avg last 5 vs baseline avg | `cmd/benchreport/benchreport compare --against baseline --n 5` |
| Fail if regression >5% | `cmd/benchreport/benchreport compare --against baseline --n 5 --fail-above 5` |
| Sweep worker counts | `.pi/skills/fileproc-benchmark/scripts/bench_sweep.sh --case flat_100k` |
| Sweep with read mode | `.pi/skills/fileproc-benchmark/scripts/bench_sweep.sh --case flat_100k --process read` |
| Profile CPU/memory | `.pi/skills/fileproc-benchmark/scripts/bench_profile.sh --case flat_100k --cpu --mem` |
| Profile stat-only | `.pi/skills/fileproc-benchmark/scripts/bench_profile.sh --case flat_100k --process stat --cpu --mem` |
| Profile everything | `.pi/skills/fileproc-benchmark/scripts/bench_profile.sh --case flat_100k --all` |

All paths are from project root.

## 5. Regression Testing

```bash
# Run full regression suite (1k, 5k, 100k, 1m datasets)
.pi/skills/fileproc-benchmark/scripts/bench_regress.sh

# Compare latest vs previous run (default: bytes mode)
./cmd/benchreport/benchreport compare

# Compare read-mode results vs previous run
./cmd/benchreport/benchreport compare --process read

# Compare stat-mode results vs previous run
./cmd/benchreport/benchreport compare --process stat

# Compare avg of last 5 runs vs avg of previous 5 runs
./cmd/benchreport/benchreport compare --against avg --n 5

# Compare avg of last 5 runs vs baseline avg
./cmd/benchreport/benchreport compare --against baseline --n 5

# Fail if >5% regression vs baseline
./cmd/benchreport/benchreport compare --against baseline --n 5 --fail-above 5
```

## 6. Profiling

```bash
# CPU profile - where is time spent?
.pi/skills/fileproc-benchmark/scripts/bench_profile.sh --case flat_100k --cpu

# Memory profile - what allocates?
.pi/skills/fileproc-benchmark/scripts/bench_profile.sh --case flat_100k --mem

# Syscall analysis - what syscalls dominate?
.pi/skills/fileproc-benchmark/scripts/bench_profile.sh --case flat_100k --strace

# Profile stat-only (discovery + metadata)
.pi/skills/fileproc-benchmark/scripts/bench_profile.sh --case flat_100k --process stat --cpu --mem

# Everything at once
.pi/skills/fileproc-benchmark/scripts/bench_profile.sh --case flat_100k --all
```

## 7. Parameter Sweeps

```bash
# Sweep worker counts across all cases
.pi/skills/fileproc-benchmark/scripts/bench_sweep.sh

# Sweep specific case
.pi/skills/fileproc-benchmark/scripts/bench_sweep.sh --case flat_100k

# Sweep with read mode
.pi/skills/fileproc-benchmark/scripts/bench_sweep.sh --case flat_100k --process read

# Custom worker list
.pi/skills/fileproc-benchmark/scripts/bench_sweep.sh --case flat_100k --workers 4,8,16,24,32
```

## 8. Managing Baseline

Update baseline after verified improvements:

```bash
# Find latest run
ls -lt .benchmarks/regress-* | head -5

# Append a baseline entry
./cmd/benchreport/benchreport baseline add .benchmarks/regress-<timestamp>

# Optional: keep last 5 baselines
./cmd/benchreport/benchreport baseline prune --keep 5

# Commit
git add .benchmarks/baseline.jsonl
git commit -m "bench: update baseline after reducing allocations"
```

## 9. File Locations

| Path | Purpose |
|------|---------|
| `.data/` | Test datasets (gitignored) |
| `.benchmarks/baseline.jsonl` | Git-tracked baseline set (JSONL) for regression checks |
| `.benchmarks/history.jsonl` | Local-only run history |
| `.benchmarks/regress-*/` | Raw per-run artifacts |
| `.benchmarks/profiles/` | pprof and strace output |
| `cmd/benchreport/` | Comparison tool |
| `cmd/ticketgen/` | Data generator |

## 10. Script Options

### bench_regress.sh

```
--runs N          Base runs per benchmark (default: 10)
--warmup N        Warmup runs (default: 5)
--tag TAG         Tag for this run
--workers N       Override worker count
--gc N            Override GC percent
--require-ac      Fail if on battery
--verbose         Show full hyperfine output
--help            Show detailed help
```

### bench_profile.sh

```
--case CASE       Test case (required)
--all             Enable all profiling
--cpu             CPU profile
--mem             Memory profile
--strace          Syscall analysis
--repeat N        Iterations (default: 10)
--workers N       Override workers
--process NAME    bytes | read | stat (default: bytes)
--help            Show detailed help
```

### bench_sweep.sh

```
--case CASES      Comma-separated cases (default: all)
--workers LIST    Worker counts to test
--runs N          Runs per combination
--process NAME    bytes | read | stat (default: bytes)
--verbose         Show full output
--help            Show detailed help
```

### benchreport compare

```
--against MODE    prev, avg, baseline (default: prev)
--n N             Runs to average (avg = last N vs prev N; baseline = last N history vs baseline avg)
--fail-above PCT  Fail if regression exceeds threshold
--filter SIZES    Filter on specific sizes (e.g., 100k,1m)
--process NAME    bytes | read | stat (default: bytes)
--json            Output as JSON
```
