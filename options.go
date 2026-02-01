package fileproc

import "runtime"

// Option configures [Process].
// Options are applied in order.
type Option func(*options)

// WithFileWorkers sets the file worker count for concurrent processing.
//
// File workers execute user callbacks for file chunks. Higher values can
// increase throughput if there are enough work units and I/O headroom, but
// also increase contention and memory usage. Values <= 0 use defaults.
func WithFileWorkers(n int) Option {
	return func(o *options) {
		o.Workers = n
	}
}

// WithScanWorkers sets the worker count for directory scanning.
//
// Scan workers read directory entries and emit fixed-size chunks. Higher
// values increase discovery rate but can increase open dir handles and
// memory pressure. Values <= 0 use defaults.
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
// Smaller sizes increase parallelism but add scheduling overhead. Larger
// sizes reduce overhead but can leave workers idle for small directories.
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
}

// applyOptions merges option values and applies defaults.
func applyOptions(opts []Option) options {
	cfg := options{}

	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}

	if cfg.Recursive {
		if cfg.Workers <= 0 {
			cfg.Workers = defaultRecursiveWorkers()
		}
	} else if cfg.Workers <= 0 {
		cfg.Workers = defaultWorkers()
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

// defaultWorkers returns the default file worker count.
func defaultWorkers() int {
	w := max((runtime.GOMAXPROCS(0)*2)/3, 4)

	return w
}

// defaultRecursiveWorkers returns the default worker count for tree traversal.
//
// Benchmarks on a 24-core machine showed 12 workers optimal for tree mode,
// with regression at 16+ workers due to futex contention from goroutine
// coordination (job queue + found channel for subdirectories).
//
// Formula: NumCPU/2, clamped to [4, 16].
func defaultRecursiveWorkers() int {
	return min(max(runtime.NumCPU()/2, 4), 16)
}

// defaultScanWorkers returns the default scan worker count.
func defaultScanWorkers(workers int) int {
	return max(1, min(4, workers/2))
}
