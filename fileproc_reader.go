package fileproc

import (
	"context"
	"io"
)

// readerProbeSize is the number of bytes read from a file before invoking a
// [ProcessReaderFunc].
//
// The probe serves two purposes:
//   - It reliably detects directories on Unix without an extra stat call
//     (read returns EISDIR).
//   - It primes the stream so that callbacks that only need a small prefix can
//     read it without an immediate additional syscall.
//
// The bytes read by the probe are replayed to the callback via a small wrapper
// reader, so the callback sees a stream starting at byte 0.
const readerProbeSize = 4096

// ProcessReaderFunc is called for each file with an [io.Reader] for its content.
//
// ProcessReaderFunc may be called concurrently and must be safe for concurrent
// use.
//
// path is relative to the directory passed to [ProcessReader]. The path slice
// aliases internal reusable buffers; it is only valid until ProcessReaderFunc
// returns. Copy it if needed.
//
// The reader is only valid until ProcessReaderFunc returns. The underlying file
// handle is owned and closed by the processor.
//
// Callbacks are not invoked for directories, symlinks, or other non-regular
// files.
//
// Empty files are passed to ProcessReaderFunc with a reader that returns EOF
// immediately.
//
// Return values:
//   - (*T, nil): emit the result
//   - (nil, nil): skip this file silently
//   - (_, error): skip and report the error as a [ProcessError]
//
// Whether [ProcessError]s (and [IOError]s) are included in the returned error
// slice depends on [Options.OnError]. If OnError is nil, all errors are
// collected.
//
// Panics are not recovered by this package. Callbacks must not panic; if you
// need to guard against panics, recover inside the callback.
//
// # Choosing an API
//
// Use [Process] when you only need a small prefix of each file (headers,
// metadata, magic bytes) and you can decide validity from the first N bytes. It
// is the fastest option and avoids per-file allocations in steady state.
//
// Use [ProcessReader] when you need to stream or fully consume file contents
// (e.g. hashing, decompressing, parsing large files). The callback controls how
// much to read and when to stop.
//
// Note: [Options.ReadSize] is ignored by ProcessReader.
type ProcessReaderFunc[T any] func(path []byte, r io.Reader) (*T, error)

// replayReader replays an already-read prefix before continuing with the
// underlying file handle.
//
// It exists to support ProcessReader's initial probe read without losing bytes
// from the stream.
type replayReader struct {
	fh fileHandle

	prefix []byte
	off    int

	// pendingErr is returned after prefix is drained. Kept for completeness.
	// In practice we avoid invoking the callback if the probe read returned an
	// error.
	pendingErr error
}

func (r *replayReader) Read(buf []byte) (int, error) {
	if len(buf) == 0 {
		return 0, nil
	}

	if r.off < len(r.prefix) {
		n := copy(buf, r.prefix[r.off:])
		r.off += n

		return n, nil
	}

	if r.pendingErr != nil {
		err := r.pendingErr
		r.pendingErr = nil

		return 0, err
	}

	return r.fh.Read(buf)
}

func (r *replayReader) reset(fh fileHandle, prefix []byte, pendingErr error) {
	r.fh = fh
	r.prefix = prefix
	r.off = 0
	r.pendingErr = pendingErr
}

// ProcessReader processes files in a directory, passing an [io.Reader] for each
// file to fn.
//
// By default, only the specified directory is processed. Set opts.Recursive to
// true to process subdirectories recursively.
//
// Like [Process], paths passed to fn are relative to path.
//
// ProcessReader is intended for streaming/full-content processing. It does not
// truncate reads; the callback controls how much to consume.
//
// Results are unordered when processed with multiple workers.
//
// Returns collected results and any errors ([IOError] or [ProcessError]). See
// [Process] for cancellation semantics.
func ProcessReader[T any](ctx context.Context, path string, fn ProcessReaderFunc[T], opts Options) ([]Result[T], []error) {
	proc := processor[T]{kind: procKindReader, fnReader: fn}

	return processEntry[T](ctx, path, opts, proc.run)
}
