package fileproc

import "runtime"

// Option configures [Process].
// Options are applied in order.
type Option func(*options)

// WithWorkers sets the worker count for concurrent processing.
func WithWorkers(n int) Option {
	return func(o *options) {
		o.Workers = n
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
func WithRecursive() Option {
	return func(o *options) {
		o.Recursive = true
	}
}

// WithSmallFileThreshold sets the file-count cutoff below which sequential
// processing is used instead of pipelined workers.
func WithSmallFileThreshold(n int) Option {
	return func(o *options) {
		o.SmallFileThreshold = n
	}
}

// WithOnError registers an error handler for [IOError] and [ProcessError].
//
// ioErrs and procErrs are cumulative counts including the current error.
// The handler may be called concurrently from multiple goroutines.
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
	Workers            int
	Suffix             string
	Recursive          bool
	SmallFileThreshold int
	OnError            func(err error, ioErrs, procErrs int) (collect bool)
}

func applyOptions(opts []Option) options {
	cfg := options{}

	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}

	if cfg.SmallFileThreshold <= 0 {
		cfg.SmallFileThreshold = defaultSmallFileThreshold
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

	return cfg
}

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
