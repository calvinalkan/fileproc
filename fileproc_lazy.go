package fileproc

import (
	"context"
	"errors"
	"syscall"
)

// File provides access to a file being processed by ProcessLazy.
//
// All methods are lazy: the underlying file is opened on first content access
// (Bytes, Read, or Fd). The handle is owned by fileproc and closed after the
// callback returns. File must not be retained beyond the callback.
//
// Bytes() and Read() are mutually exclusive per file. Calling one after
// the other returns an error.
type File struct {
	dh   dirHandle
	name []byte // NUL-terminated filename

	relPath  []byte // arena-backed relative path (no NUL)
	st       Stat
	statDone bool

	fh     *fileHandle
	fhOpen *bool

	dataBuf   *[]byte // temporary read buffer (reused across files)
	dataArena *[]byte // append-only arena for Bytes() results

	mode    fileMode
	openErr error
	statErr error
}

type fileMode uint8

const (
	fileModeNone fileMode = iota
	fileModeBytes
	fileModeReader
)

var (
	errBytesAfterRead     = errors.New("Bytes: cannot call after Read")
	errBytesAlreadyCalled = errors.New("Bytes: already called")
	errReadAfterBytes     = errors.New("Read: cannot call after Bytes")

	// errSkipFile is an internal sentinel indicating the file should be
	// silently skipped (e.g., became a directory due to race condition).
	// Not reported as an error to the user.
	errSkipFile = errors.New("skip file")
)

// RelPath returns the file path relative to the root directory passed to
// ProcessLazy.
//
// The returned slice points into internal storage and remains valid until
// ProcessLazy returns. Copy if needed beyond that.
func (f *File) RelPath() []byte {
	return f.relPath
}

// Stat returns file metadata.
//
// Lazy: the stat syscall is made on first call and cached. Subsequent calls
// return the cached value with no additional I/O.
//
// Returns zero Stat and an error if the stat syscall fails (e.g., file was
// deleted or became a non-regular file).
func (f *File) Stat() (Stat, error) {
	if !f.statDone {
		f.lazyStat()
	}

	return f.st, f.statErr
}

// BytesOption configures the behavior of [File.Bytes].
type BytesOption struct {
	sizeHint int
}

// WithSizeHint provides an expected file size to optimize buffer allocation.
//
// Use when file sizes are known or predictable (e.g., uniform log entries,
// fixed-format records) to avoid buffer resizing without a stat syscall.
//
// The hint is a suggestion, not a limit. Files larger than the hint are
// read completely; smaller files don't waste the extra space (only the
// actual content is stored in the arena).
//
// The hint is ignored if [File.Stat] was called previously, since the
// actual size is already known.
//
// Example:
//
//	// Pre-create option outside the processing loop (zero allocation)
//	opt := fileproc.WithSizeHint(4096)
//
//	fileproc.ProcessLazy(ctx, dir, func(f *fileproc.File, _ *fileproc.Scratch) (*T, error) {
//	    data, err := f.Bytes(opt)
//	    // ...
//	}, opts)
func WithSizeHint(size int) BytesOption {
	return BytesOption{sizeHint: size}
}

// Bytes reads and returns the full file content.
//
// The returned slice points into an internal arena and remains valid until
// ProcessLazy returns. Subslices share the same lifetime.
//
// Empty files return a non-nil empty slice ([]byte{}, nil).
//
// Single-use: Bytes can only be called once per File.
// Returns error if called after [File.Read], or on I/O failure.
//
// Memory: content is retained in the arena until ProcessLazy returns.
// For large files or memory-constrained use cases, consider [File.Read]
// with streaming processing instead.
//
// Buffer sizing: Bytes does not call stat internally. The buffer size is
// determined by (in priority order):
//  1. The actual size from [File.Stat], if it was called previously
//  2. The hint from [WithSizeHint], if provided
//  3. A default 512-byte buffer, grown as needed
//
// For workloads with known/uniform file sizes, use [WithSizeHint] to avoid
// buffer resizing without the overhead of a stat syscall.
func (f *File) Bytes(opts ...BytesOption) ([]byte, error) {
	if f.mode == fileModeReader {
		return nil, errBytesAfterRead
	}

	if f.mode == fileModeBytes {
		return nil, errBytesAlreadyCalled
	}

	f.mode = fileModeBytes

	// Open file if needed
	openErr := f.open()
	if openErr != nil {
		return nil, openErr
	}

	// Buffer size priority: stat > sizeHint > default
	const defaultBufSize = 384

	var size int
	if f.statDone && f.statErr == nil {
		size = int(f.st.Size)
	} else if len(opts) > 0 && opts[0].sizeHint > 0 {
		size = opts[0].sizeHint
	}

	readSize := max(
		// +1 to detect growth / read past expected size
		size+1, defaultBufSize)

	// Ensure dataBuf capacity
	if cap(*f.dataBuf) < readSize {
		*f.dataBuf = make([]byte, 0, readSize)
	}

	*f.dataBuf = (*f.dataBuf)[:readSize]
	buf := *f.dataBuf

	// Single read syscall using backend
	n, isDir, err := f.fh.readInto(buf)
	if isDir {
		return nil, errSkipFile
	}

	if err != nil {
		return nil, err
	}

	// Buffer was filled - file may be larger, continue reading
	if n == readSize {
		for {
			if n == len(buf) {
				growBy := max(len(buf), 4096)
				*f.dataBuf = append(*f.dataBuf, make([]byte, growBy)...)
				buf = *f.dataBuf
			}

			m, _, readErr := f.fh.readInto(buf[n:])
			n += m

			if m == 0 || readErr != nil {
				break
			}
		}
	}

	// Empty file
	if n == 0 {
		return []byte{}, nil
	}

	// Copy to arena, return subslice
	start := len(*f.dataArena)
	*f.dataArena = append(*f.dataArena, buf[:n]...)

	return (*f.dataArena)[start:], nil
}

// Read implements io.Reader for streaming access.
//
// Use when you need only a prefix or want to process in chunks without
// retaining the full content. Data read via Read() is NOT arena-allocated;
// caller provides and manages the buffer.
//
// Returns error if called after Bytes().
//
// Lazy: file is opened on first Read() call.
func (f *File) Read(p []byte) (int, error) {
	if f.mode == fileModeBytes {
		return 0, errReadAfterBytes
	}

	f.mode = fileModeReader

	err := f.open()
	if err != nil {
		return 0, err
	}

	return f.fh.Read(p)
}

// Fd returns the underlying file descriptor.
//
// Lazy: file is opened if not already open.
//
// Use for low-level operations (sendfile, mmap, etc.). The fd is owned by
// fileproc and will be closed after the callback returns.
func (f *File) Fd() uintptr {
	err := f.open()
	if err != nil {
		return 0
	}

	return f.fh.fdValue()
}

func (f *File) lazyStat() {
	st, kind, err := f.dh.statFile(f.name)
	f.statDone = true

	if err != nil {
		if errors.Is(err, syscall.ELOOP) || kind == statKindSymlink {
			f.statErr = errSkipFile
		} else {
			f.statErr = err
		}

		return
	}

	if kind != statKindReg {
		f.statErr = errSkipFile

		return
	}

	f.st = st
}

func (f *File) open() error {
	if f.fhOpen != nil && *f.fhOpen {
		return nil // already open
	}

	if f.openErr != nil {
		return f.openErr // previous attempt failed
	}

	fh, err := f.dh.openFile(f.name)
	if err != nil {
		// Symlink detected (race: was regular file during scan)
		if errors.Is(err, syscall.ELOOP) {
			f.openErr = errSkipFile

			return errSkipFile
		}

		f.openErr = err

		return err
	}

	*f.fh = fh
	*f.fhOpen = true

	return nil
}

// Scratch provides reusable temporary buffer space for callback processing.
//
// The buffer is reused across files within a worker. It is only valid during
// the current callback and will be overwritten for the next file.
//
// Use for temporary parsing work, not for data that must survive the callback.
type Scratch struct {
	buf *[]byte
}

// Get returns scratch space with at least the requested capacity.
//
// Returns a slice with len=0 and cap>=size, ready for append:
//
//	buf := scratch.Get(4096)
//	buf = append(buf, data...)  // no alloc if fits in capacity
//
// For use as a fixed-size read target, expand to full capacity:
//
//	buf := scratch.Get(4096)
//	buf = buf[:cap(buf)]
//	n, _ := io.ReadFull(r, buf)
//
// The capacity grows to accommodate the largest request seen across all
// files processed by this worker, then stabilizes.
//
// The returned slice is only valid during the current callback.
func (s *Scratch) Get(size int) []byte {
	if cap(*s.buf) < size {
		*s.buf = make([]byte, 0, size)
	}

	return (*s.buf)[:0]
}

// ProcessLazyFunc is the callback for ProcessLazy.
//
// f provides access to file metadata and content. scratch provides reusable
// temporary buffer space.
//
// ProcessLazyFunc may be called concurrently from multiple goroutines.
//
// Return values:
//   - (*T, nil): emit the result
//   - (nil, nil): skip this file silently
//   - (_, error): skip and report as ProcessError
type ProcessLazyFunc[T any] func(f *File, scratch *Scratch) (*T, error)

// ProcessLazy processes files in a directory, providing stat metadata and
// arena-backed content access.
//
// By default, only the specified directory is processed. Set opts.Recursive
// to true to process subdirectories recursively.
//
// Memory: When callbacks use f.Bytes(), file content is retained in per-worker
// arenas until ProcessLazy returns. Total memory usage scales with the sum of
// retained file content sizes. For memory-constrained use cases, use f.Read()
// with streaming processing, or skip large files based on f.Stat().Size.
//
// See [Process] for cancellation and error handling semantics.
func ProcessLazy[T any](
	ctx context.Context,
	path string,
	fn ProcessLazyFunc[T],
	opts Options,
) ([]Result[T], []error) {
	proc := processor[T]{kind: procKindLazy, fnLazy: fn}

	return processEntry(ctx, path, opts, proc.run)
}
