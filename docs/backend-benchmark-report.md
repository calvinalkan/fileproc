# Backend benchmark comparison (Linux vs Unix vs Generic)

**Date:** 2026-01-19

## Goal

Compare the performance impact of using different I/O backends on Linux:

- **Linux fast path** (`io_linux.go`): getdents64 + openat (baseline).
- **Unix backend** (`io_unix.go`): `ReadDir` + openat-based opens.
- **Generic backend** (`io_other.go`): stdlib `ReadDir` + `filepath.Join` open (no openat).

This captures how much speed we lose if we fall back to the portable backends on Linux.

## Methodology

- Dataset: `.data/tickets_{flat|nested1}_{1k|5k|100k|1m}`
- Benchmark runner: `.pi/skills/fileproc-benchmark/scripts/bench_regress.sh`
- Default settings (read=2048, workers=default, gc=default)
- For the Unix/Generic tests, the backend was forced on Linux via temporary build-tag overrides in the **experiment branch**:

  - **Generic backend:** `GOFLAGS="-tags=fileproc_generic"`
  - **Unix backend:** `GOFLAGS="-tags=fileproc_unix"`

  These overrides were only used for the experiment and are not part of mainline builds.

## Results (large datasets)

Baseline is the **Linux fast path** (average of the current 5-run baseline set).

| Backend | flat_100k (frontmatter) | nested1_100k (frontmatter) | flat_1m (frontmatter) | nested1_1m (frontmatter) | Notes |
| --- | --- | --- | --- | --- | --- |
| **Linux fast path** | **46.26 ms** | **46.56 ms** | **403.91 ms** | **406.60 ms** | Baseline (`io_linux.go`) |
| **Unix backend** | 57.21 ms (**+23.7%**) | 54.54 ms (**+17.2%**) | 441.48 ms (**+9.3%**) | 441.61 ms (**+8.6%**) | `io_unix.go` forced on Linux |
| **Generic backend** | 85.38 ms (**+84.6%**) | 86.33 ms (**+85.4%**) | 721.88 ms (**+78.7%**) | 735.61 ms (**+80.9%**) | `io_other.go` forced on Linux |

### Small datasets (1k/5k)

Small cases are also slower when using the portable backends, especially the generic backend:

- **Generic backend**
  - flat_5k: 18.96 ms vs 7.19 ms (**+164%**)
  - nested1_5k: 18.99 ms vs 6.89 ms (**+176%**)

- **Unix backend**
  - flat_5k: 12.76 ms vs 7.19 ms (**+77.6%**)
  - nested1_5k: 12.30 ms vs 6.89 ms (**+78.5%**)

## Key takeaways

- The Linux fast path remains significantly faster, especially for large datasets.
- The Unix backend is **~8–24% slower** on 100k/1m cases, but far better than the generic backend.
- The generic backend is **~1.8× slower** on large datasets and much worse on small datasets.
- Most of the gap comes from syscall efficiency (`getdents64` + `openat`) and avoiding per-file path allocations.

## Raw run artifacts

Experiment runs saved under:

- `regress-2026-01-19T00-23-01+0100-generic-backend`
- `regress-2026-01-19T00-28-16+0100-unix-backend`

Baseline reference is in `.benchmarks/baseline.jsonl` (5 entries).
