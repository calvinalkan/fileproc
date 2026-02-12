package fileproc

import (
	"errors"
	"io"
	"os"
	"syscall"
)

// Stat holds metadata for a file discovered by [Process].
//
// ModTime is expressed as Unix nanoseconds to avoid time.Time allocations
// in hot paths. Use time.Unix(0, st.ModTime) to convert when needed.
type Stat struct {
	// Size is the file size in bytes.
	Size int64
	// ModTime is the modification time in Unix nanoseconds.
	ModTime int64
	// Mode is the file mode bits (os.FileMode).
	Mode uint32
	// Inode is the inode number when available (0 on platforms without it).
	Inode uint64
}

// File provides access to a file being processed by [Process].
//
// All methods are lazy: the underlying file is opened on first content access
// (ReadAll, Read, or Fd). The handle is owned by fileproc and closed after the
// callback returns. File must not be retained beyond the callback.
//
// ReadAll() and Read() are mutually exclusive per file. Calling one after
// the other returns an error.
type File struct {
	// dh is the open directory handle used for openat/statat.
	dh dirHandle
	// name is the NUL-terminated basename for openat/statat.
	name nulTermName
	// base is the NUL-terminated directory path for building absolute paths.
	base nulTermPath
	// rootLen is the length of the root path (no trailing NUL).
	rootLen int
	// path caches the built absolute path (no trailing NUL).
	path []byte
	// pathScratch is a reusable buffer for building absolute paths.
	pathScratch *[]byte
	// pathBuilt tracks whether path is populated for this file.
	pathBuilt bool
	// st caches the file stat result.
	st Stat
	// statDone tracks whether stat has been attempted.
	statDone bool
	// fh points at the currently open file handle.
	fh *fileHandle
	// fhOpen indicates whether fh is open.
	fhOpen *bool
	// dataBuf is the temporary read buffer reused across files.
	dataBuf *[]byte
	// mode tracks the Bytes/Read usage state.
	mode fileMode
	// openErr caches open errors for lazy open.
	openErr error
	// statErr caches stat errors for lazy stat.
	statErr error
}

// AbsPath returns the absolute file path (without the trailing NUL).
//
// Prefer this over string concatenation: it reuses a scratch buffer to
// avoid per-file allocations and handles path separators correctly.
//
// The returned slice is ephemeral and only valid during the callback.
// Copy if you need to retain it.
func (f *File) AbsPath() []byte {
	if f.pathBuilt {
		return f.path
	}

	baseLen := f.base.lenWithoutNul()
	nameLen := f.name.lenWithoutNul()

	sep := 0
	if baseLen > 0 && f.base[baseLen-1] != os.PathSeparator {
		sep = 1
	}

	needed := baseLen + sep + nameLen

	buf := *f.pathScratch
	if cap(buf) < needed {
		buf = make([]byte, 0, needed)
	}

	buf = buf[:0]
	if baseLen > 0 {
		buf = append(buf, f.base[:baseLen]...)
		if sep == 1 {
			buf = append(buf, os.PathSeparator)
		}
	}

	buf = append(buf, f.name[:nameLen]...)

	*f.pathScratch = buf
	f.path = buf
	f.pathBuilt = true

	return buf
}

// RelPath returns the file path relative to the root passed to [Process].
//
// Prefer this over string concatenation: it reuses the AbsPath buffer to
// avoid per-file allocations and handles path separators correctly.
//
// The returned slice is ephemeral and only valid during the callback.
// Copy if you need to retain it.
func (f *File) RelPath() []byte {
	abs := f.AbsPath()
	if f.rootLen >= len(abs) {
		return abs
	}

	start := f.rootLen
	if abs[start] == os.PathSeparator {
		start++
	}

	return abs[start:]
}

// Stat returns file metadata.
//
// Lazy: the stat syscall is made on first call and cached. Subsequent calls
// return the cached value with no additional I/O.
//
// Returns zero Stat and an error if the stat syscall fails (e.g., file was
// deleted or became a non-regular file). If the file is now a directory or
// symlink, Stat returns a skip error that Process ignores when returned by the
// callback.
//
// Example (inside Process callback):
//
//	st, err := f.Stat()
//	if err != nil {
//	    return nil, err
//	}
//	if st.Size == 0 {
//	    return nil, fileproc.ErrSkip
//	}
//	return &Result{Size: st.Size}, nil
func (f *File) Stat() (Stat, error) {
	if !f.statDone {
		f.lazyStat()
	}

	return f.st, f.statErr
}

// ReadAllOption configures the behavior of [File.ReadAll].
type ReadAllOption struct {
	// sizeHint is the expected file size to pre-size buffers.
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
//	fileproc.Process(ctx, dir, func(f *fileproc.File, _ *fileproc.FileWorker) (*T, error) {
//	    data, err := f.ReadAll(opt)
//	    // ...
//	}, opts)
func WithSizeHint(size int) ReadAllOption {
	return ReadAllOption{sizeHint: size}
}

// ReadAll reads and returns the full file content.
//
// The returned slice points into a reusable buffer and is only valid during
// the callback. To retain data beyond the callback, copy it or use
// [FileWorker.RetainBytes].
//
// Empty files return a non-nil empty slice ([]byte{}, nil).
//
// Single-use: ReadAll can only be called once per File and is mutually exclusive
// with [File.Read].
//
// Returns error if called after [File.Read], or on I/O failure. If the file
// changes type (becomes a directory or symlink) between scan and read, ReadAll
// returns a skip error that Process ignores when returned by the callback.
//
// Memory: content is stored in a reusable buffer; it is not retained.
// For large files or memory-constrained use cases, consider [File.Read]
// with streaming processing instead.
//
// Buffer sizing: ReadAll does not call stat internally. The buffer size is
// determined by (in priority order):
//  1. The actual size from [File.Stat], if it was called previously
//  2. The hint from [WithSizeHint], if provided
//  3. A default 512-byte buffer, grown as needed
//
// For workloads with known/uniform file sizes, use [WithSizeHint] to avoid
// buffer resizing without the overhead of a stat syscall.
//
// Example (inside Process callback):
//
//	data, err := f.ReadAll()
//	if err != nil {
//	    return nil, err
//	}
//	keep := w.RetainBytes(data)
//	return &Result{Len: len(keep)}, nil
func (f *File) ReadAll(opts ...ReadAllOption) ([]byte, error) {
	if f.mode == fileModeReader {
		return nil, errBytesAfterRead
	}

	if f.mode == fileModeBytes {
		return nil, errBytesAlreadyCalled
	}

	// Open file if needed.
	openErr := f.open()
	if openErr != nil {
		return nil, openErr
	}

	f.mode = fileModeBytes

	// Buffer size priority: stat > sizeHint > default.
	maxInt := int(^uint(0) >> 1)
	maxSize := maxInt - 1

	var size int

	if f.statDone && f.statErr == nil {
		if f.st.Size > 0 {
			if f.st.Size > int64(maxSize) {
				size = maxSize
			} else {
				size = int(f.st.Size)
			}
		}
	} else if len(opts) > 0 && opts[0].sizeHint > 0 {
		size = min(opts[0].sizeHint, maxSize)
	}

	// +1 to detect growth / read past expected size.
	readSize := max(size+1, defaultReadBufSize)

	// Ensure dataBuf capacity.
	if cap(*f.dataBuf) < readSize {
		*f.dataBuf = make([]byte, 0, readSize)
	}

	*f.dataBuf = (*f.dataBuf)[:readSize]

	n, out, err := f.readAllInto(*f.dataBuf, 0, true)
	if err != nil {
		return nil, err
	}

	*f.dataBuf = out

	// Empty file.
	if n == 0 {
		return []byte{}, nil
	}

	return out[:n], nil
}

// Read implements io.Reader for streaming access.
//
// Use when you need only a prefix or want to process in chunks without
// retaining the full content. Data read via Read() is NOT arena-allocated;
// caller provides and manages the buffer.
//
// Read may be called multiple times until it returns io.EOF. It is mutually
// exclusive with [File.ReadAll] and returns an error if ReadAll was used first.
//
// If the file changes type (becomes a directory or symlink) between scan and
// read, Read returns a skip error that Process ignores when returned by the
// callback.
//
// Lazy: file is opened on first Read() call.
//
// Example (inside Process callback):
//
//	buf := w.Buf(32 * 1024)
//	buf = buf[:cap(buf)]
//	for {
//	    n, err := f.Read(buf)
//	    if n > 0 {
//	        // consume buf[:n]
//	    }
//	    if err != nil {
//	        if errors.Is(err, io.EOF) {
//	            break
//	        }
//	        return nil, err
//	    }
//	}
func (f *File) Read(p []byte) (int, error) {
	if f.mode == fileModeBytes {
		return 0, errReadAfterBytes
	}

	if f.mode != fileModeReader {
		err := f.open()
		if err != nil {
			return 0, err
		}

		f.mode = fileModeReader
	}

	n, err := f.fh.Read(p)
	if err != nil {
		if errors.Is(err, syscall.EISDIR) || errors.Is(err, syscall.ELOOP) {
			return 0, errSkipFile
		}
	}

	return n, err
}

// Fd returns the underlying file descriptor.
//
// Lazy: file is opened if not already open.
//
// Use for low-level operations (sendfile, mmap, etc.). The fd is owned by
// fileproc and will be closed after the callback returns.
//
// Returns ^uintptr(0) (i.e., -1) if the file cannot be opened.
func (f *File) Fd() uintptr {
	err := f.open()
	if err != nil {
		return ^uintptr(0)
	}

	return f.fh.fdValue()
}

// readAllInto reads file content into dst starting at destOff.
//
// When allowGrow is true, dst may be grown to fit full content.
// When allowGrow is false, io.ErrShortBuffer is returned if content exceeds
// remaining destination capacity.
func (f *File) readAllInto(dst []byte, destOffset int, allowGrow bool) (int, []byte, error) {
	// destOffset==len(dst) is valid (zero remaining capacity).
	if destOffset < 0 || destOffset > len(dst) {
		return 0, dst, errInvalidDestOffset
	}

	buffer := dst

	// Fast path: one read handles empty/small files without entering the grow loop.
	initialRead, isDir, err := f.fh.readInto(buffer[destOffset:])
	if isDir {
		return 0, buffer, errSkipFile
	}

	if err != nil {
		return 0, buffer, err
	}

	writeCursor := destOffset + initialRead

	// A full initial read means there may be more bytes; continue until EOF/error.
	if initialRead == len(buffer)-destOffset {
		var readFailure error

		for {
			if writeCursor == len(buffer) {
				if !allowGrow {
					// Distinguish exact fit vs truncation without growing destination:
					// if one more byte exists, caller buffer was too small.
					var probe [1]byte

					probeRead, isDir, err := f.fh.readInto(probe[:])
					if isDir {
						return writeCursor - destOffset, buffer, errSkipFile
					}

					if err != nil {
						return writeCursor - destOffset, buffer, err
					}

					if probeRead > 0 {
						return writeCursor - destOffset, buffer, io.ErrShortBuffer
					}

					break
				}

				// Geometric growth keeps realloc count low on large files.
				growBy := max(len(buffer), 4096)
				if growBy == 0 {
					growBy = defaultReadBufSize
				}

				buffer = append(buffer, make([]byte, growBy)...)
			}

			chunkRead, isDir, err := f.fh.readInto(buffer[writeCursor:])
			if isDir {
				return writeCursor - destOffset, buffer, errSkipFile
			}

			// Keep bytes read so far, then surface the read error after loop.
			if err != nil {
				readFailure = err
			}

			// Advance by actual bytes read, even on short reads.
			writeCursor += chunkRead

			// Stop on EOF/short-read (chunkRead==0) or any read failure.
			if chunkRead == 0 || readFailure != nil {
				break
			}
		}

		// Return buffered bytes alongside the terminal read failure.
		if readFailure != nil {
			return writeCursor - destOffset, buffer, readFailure
		}
	}

	return writeCursor - destOffset, buffer, nil
}

var (
	errBytesAfterRead     = errors.New("bytes: cannot call after read")
	errBytesAlreadyCalled = errors.New("bytes: already called")
	errReadAfterBytes     = errors.New("read: cannot call after bytes")
	errInvalidDestOffset  = errors.New("read all into: invalid destination offset")

	// errSkipFile is an internal sentinel indicating the file should be
	// silently skipped (e.g., became a directory due to race condition).
	// Not reported as an error to the user.
	errSkipFile = errors.New("skip file")
)

// fileMode tracks which content access method has been used for a File.
// It enforces the Bytes/Read exclusivity without extra flags.
type fileMode uint8

// Zero value means no content access yet.
const (
	// fileModeBytes indicates Bytes() was used.
	fileModeBytes fileMode = iota + 1
	// fileModeReader indicates Read() was used.
	fileModeReader
)

// statKind classifies stat results so callers can skip non-regular files
// without extra syscalls or mode checks.
type statKind uint8

const (
	// statKindReg indicates a regular file.
	statKindReg statKind = iota
	// statKindDir indicates a directory.
	statKindDir
	// statKindSymlink indicates a symlink.
	statKindSymlink
	// statKindOther indicates a non-regular, non-dir, non-symlink entry.
	statKindOther
)

// lazyStat performs a single stat call and caches the result.
// Non-regular files are mapped to errSkipFile so callers can ignore races.
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

// open lazily opens the file and caches the result.
// Symlink/dir races are normalized to errSkipFile so callers can skip silently.
func (f *File) open() error {
	if f.fhOpen != nil && *f.fhOpen {
		return nil // already open
	}

	if f.openErr != nil {
		return f.openErr // previous attempt failed
	}

	fh, err := f.dh.openFile(f.name)
	if err != nil {
		// Symlink detected (race: was regular file during scan).
		if errors.Is(err, syscall.ELOOP) || errors.Is(err, syscall.EISDIR) {
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
