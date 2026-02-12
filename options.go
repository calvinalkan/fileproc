package fileproc

import (
	"runtime"
	"time"
)

// Option configures [Process], [Watch], and [Events].
// Options are applied in order.
type Option func(*options)

// WithFileWorkers sets the file worker count for concurrent processing.
//
// File workers execute user callbacks for file chunks. Each worker processes
// one chunk at a time, calling the user's [ProcessFunc] for each file.
//
// # Default
//
// Non-recursive: (GOMAXPROCS × 2) / 3, minimum 4 (e.g., 16 on 24-core).
// Recursive: NumCPU / 2, clamped to [4, 16].
//
// Defaults are tuned for I/O-bound workloads where callbacks primarily read
// file content via [File.Bytes] or [File.Read], or stat via [File.Stat].
// Profiling shows 90%+ of runtime is spent in syscalls (open/read/close),
// so kernel throughput is the bottleneck, not CPU.
//
// # Tuning guidance
//
// The optimal count depends on your workload:
//
//   - I/O bound (reading file content): 8–16 workers typically optimal.
//     More workers cause kernel VFS lock contention (open/read/close on
//     same directory), increasing per-syscall latency ~2× at 64+ workers.
//
//   - CPU bound (expensive callbacks): if your callback does significant
//     processing (parsing, compression, hashing), try scaling workers up
//     to NumCPU or beyond. The kernel contention overhead becomes less
//     relevant when CPU time dominates.
//
//   - Stat-only ([File.Stat]): similar to I/O bound; kernel contention
//     on fstatat limits gains beyond 16 workers.
//
// Benchmarks on 24-core (flat 100k files, bytes mode):
//
//	w=16:  39ms (optimal)
//	w=64:  41ms (+5%, syscalls 2× slower due to VFS contention)
//	w=256: 42ms (+7%)
//
// Values <= 0 use defaults.
func WithFileWorkers(n int) Option {
	return func(o *options) {
		o.Workers = n
	}
}

// WithScanWorkers sets the worker count for directory scanning.
//
// Scan workers read directory entries and split them into fixed-size chunks
// for file workers to process. Multiple scan workers allow parallel discovery
// of different directories in recursive mode.
//
// # Default
//
// max(1, min(4, fileWorkers/2)).
//
// # Tuning guidance
//
//   - Flat directory (non-recursive): only 1 scan worker is ever active,
//     so scan-workers=1 is optimal. Benchmarks show ~3% improvement at 1m
//     files due to reduced goroutine coordination overhead.
//
//   - Deep trees (recursive): higher values (2–4) help discover directories
//     in parallel, keeping file workers fed.
//
//   - Very wide trees: may benefit from more scan workers, but typically
//     file I/O is the bottleneck, not discovery.
//
// Values <= 0 use defaults.
func WithScanWorkers(n int) Option {
	return func(o *options) {
		o.ScanWorkers = n
	}
}

// WithSuffix filters files by suffix (e.g. ".md").
// Empty suffix matches all files.
func WithSuffix(suffix string) Option {
	return func(o *options) {
		o.Suffix = suffix
	}
}

// WithRecursive enables recursive processing.
//
// When disabled, discovered subdirectories are ignored.
func WithRecursive() Option {
	return func(o *options) {
		o.Recursive = true
	}
}

// WithChunkSize sets the number of entries (files) per work unit.
//
// Chunk size controls the granularity of work distribution. Scan workers
// split directory entries into fixed-size chunks, which file workers then
// process independently.
//
// # Default
//
// 128 entries per chunk.
//
// # Tuning guidance
//
// Trade-off between parallelism and overhead:
//
//   - Too small (8–32): excessive channel/scheduling overhead, especially
//     at high file counts. Benchmarks show 16–56% slower than optimal.
//
//   - Too large (512+): reduced parallelism, workers may sit idle while
//     others finish large chunks. Slightly worse for small directories.
//
//   - Sweet spot scales with file count: 64 for 5k files, 128 for 100k,
//     256–512 for 1m+. Default of 128 is a good middle ground.
//
// Benchmarks on 24-core, w=16 (flat dirs):
//
//	         c=64    c=128   c=256   c=512
//	5k:      4.1ms*  4.1ms   4.4ms   4.8ms
//	100k:    40ms    38ms*   39ms    39ms
//	1m:      350ms   351ms   344ms   339ms*
//
// Values <= 0 use default (128).
func WithChunkSize(n int) Option {
	return func(o *options) {
		o.ChunkSize = n
	}
}

// WithOnError registers an error handler for [IOError] and [ProcessError].
//
// ioErrs and procErrs are cumulative counts including the current error.
// The handler is serialized across goroutines.
//
// Return value controls error collection:
//   - true:  collect the error in the returned []error slice
//   - false: discard the error
//
// To stop processing, cancel the context. Use [context.WithCancelCause]
// to provide a custom stop reason retrievable via [context.Cause].
//
// If nil, all errors are collected (equivalent to always returning true).
//
// Example: stop after 100 total errors
//
//	ctx, cancel := context.WithCancelCause(ctx)
//	results, errs := Process(ctx, root, fn, WithOnError(func(err error, ioErrs, procErrs int) bool {
//		if ioErrs+procErrs >= 100 {
//			cancel(fmt.Errorf("too many errors: %d", ioErrs+procErrs))
//		}
//		return true
//	}))
func WithOnError(fn func(err error, ioErrs, procErrs int) bool) Option {
	return func(o *options) {
		o.OnError = fn
	}
}

// WithOnEvent registers a handler for watcher events.
//
// Only used by [Watch] and [Events].
// If nil, events are dropped.
func WithOnEvent(fn func(Event)) Option {
	return func(o *options) {
		o.OnEvent = fn
	}
}

// WithInterval sets the polling interval for [Watch] and [Events].
//
// Values <= 0 use the default interval.
func WithInterval(d time.Duration) Option {
	return func(o *options) {
		o.Interval = d
	}
}

// WithMinIdle sets a minimum idle time between scans for [Watch] and [Events].
//
// The watcher sleeps at least this duration after each scan completes, even if
// the interval would allow an immediate rescan. Values <= 0 disable the idle
// floor.
func WithMinIdle(d time.Duration) Option {
	return func(o *options) {
		o.MinIdle = d
	}
}

// WithEventBuffer sets the channel buffer size for [Events].
//
// Values <= 0 use the default buffer size.
func WithEventBuffer(n int) Option {
	return func(o *options) {
		o.EventBuffer = n
	}
}

// WithExpectedFiles pre-sizes watcher tables/arenas for the expected file count.
//
// Only used by [Watch] and [Events].
func WithExpectedFiles(n int) Option {
	return func(o *options) {
		o.ExpectedFiles = n
	}
}

// WithEmitBaseline enables Create events for files found during the initial scan.
//
// By default, the first scan silently populates the file index without emitting
// events. Only subsequent scans emit Create/Modify/Delete events for changes.
// This matches the behavior of event-based watchers (inotify, fsnotify) which
// only report changes after watching begins.
//
// When enabled, the initial scan emits a Create event for every existing file.
// This is useful when you need to process all files on startup, but be aware
// that watching a directory with 1M files will emit 1M Create events.
//
// Only used by [Watch] and [Events].
func WithEmitBaseline() Option {
	return func(o *options) {
		o.EmitBaseline = true
	}
}

// WithOwnedPaths copies watcher event paths into owned memory.
//
// Only used by [Watch] and [Events].
//
// When enabled, Event.Path is copied per event and does not reference internal
// watcher storage. This lets retained paths avoid pinning watcher memory and
// allows the watcher to compact its path index after heavy delete churn to
// reclaim memory. Compaction runs when dead path bytes dominate live bytes
// (≈2x) and exceed a minimum size (~4MB total, scaled by shard count).
//
// Use this only if you expect huge delete churn and need to reclaim memory.
// Downside: one allocation + copy per emitted event.
func WithOwnedPaths() Option {
	return func(o *options) {
		o.OwnedPaths = true
	}
}

type options struct {
	// Workers is the file worker count.
	Workers int
	// ScanWorkers is the directory scanning worker count.
	ScanWorkers int
	// ChunkSize is the number of entries per work unit.
	ChunkSize int
	// Suffix filters files by name suffix.
	Suffix string
	// Recursive enables recursive traversal.
	Recursive bool
	// OnError handles IO and callback errors.
	OnError func(err error, ioErrs, procErrs int) (collect bool)
	// OnEvent receives watcher events.
	OnEvent func(Event)
	// Interval controls polling cadence for Watch/Events.
	Interval time.Duration
	// MinIdle sets a minimum idle time between scans.
	MinIdle time.Duration
	// EventBuffer sets the Events channel buffer size.
	EventBuffer int
	// ExpectedFiles pre-sizes watcher tables/arenas.
	ExpectedFiles int
	// EmitBaseline emits Create events on the initial scan.
	EmitBaseline bool
	// OwnedPaths copies event paths into owned memory.
	OwnedPaths bool
	// WatchShards is the number of shards for the watcher index.
	WatchShards int
}

// applyOptions merges option values and applies defaults.
func applyOptions(opts []Option) options {
	cfg := options{}

	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}

	if cfg.Workers <= 0 {
		cfg.Workers = DefaultWorkers(cfg.Recursive)
	}

	if cfg.Workers > maxWorkers {
		cfg.Workers = maxWorkers
	}

	if cfg.ScanWorkers <= 0 {
		cfg.ScanWorkers = defaultScanWorkers(cfg.Workers)
	}

	if cfg.ScanWorkers > maxWorkers {
		cfg.ScanWorkers = maxWorkers
	}

	if cfg.ChunkSize <= 0 {
		cfg.ChunkSize = defaultChunkSize
	}

	return cfg
}

// DefaultWorkers returns the current default worker-count resolution used by
// [Process] when [WithFileWorkers] is not set.
//
// When recursive is false, this matches non-recursive [Process] defaults.
// When recursive is true, this matches [WithRecursive] defaults.
func DefaultWorkers(recursive bool) int {
	if recursive {
		// Recursive default: NumCPU/2, clamped to [4, 16].
		return min(max(runtime.NumCPU()/2, 4), 16)
	}

	// Non-recursive default: (GOMAXPROCS × 2) / 3, minimum 4.
	return max((runtime.GOMAXPROCS(0)*2)/3, 4)
}

// defaultScanWorkers returns the default scan worker count.
//
// Formula: max(1, min(2, workers/4)).
//
// Benchmarks show scan=2 optimal for both flat and nested dirs:
//   - Flat: only 1 dir, so extra workers idle (scan=1 marginally better)
//   - Nested: scan=2 beats scan=4+ by ~3-6% at 1m files
//
// Higher counts add coordinator round-trip overhead without benefit.
func defaultScanWorkers(workers int) int {
	return max(1, min(2, workers/4))
}
