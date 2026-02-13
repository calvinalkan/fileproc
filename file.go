package fileproc

import (
	"errors"
	"fmt"
	"os"
	"syscall"
)

var (
	// ErrFileTooLarge indicates file content exceeded the active ReadAll max-byte limit.
	ErrFileTooLarge = errors.New("file too large")

	// errSkipFile is an internal sentinel indicating the file should be
	// silently skipped (e.g., became a directory due to race condition).
	// Not reported as an error to the user.
	errSkipFile = errors.New("skip file")
)

// File provides access to a file being processed by [Process].
//
// All methods are lazy: the underlying file is opened on first content access
// (ReadAll, ReadAllOwned, Read, or Fd). The handle is owned by fileproc and
// closed after the callback returns. File must not be retained beyond the
// callback.
//
// ReadAll(), ReadAllOwned(), and Read() are mutually exclusive per file.
// Calling one after another returns an error.
type File struct {
	// File identity and path context (constant for this callback).
	// dh is the open directory handle used for openat/statat.
	dh dirHandle

	// name is the NUL-terminated basename for openat/statat.
	name nulTermName

	// dirPath is the NUL-terminated absolute directory path containing name.
	// Example: if file is "/src/photos/a.jpg", dirPath is "/src/photos".
	dirPath nulTermPath

	// processRootPathLen is len(path passed to [Process]), without trailing NUL.
	// [File.RelPath] strips this prefix from AbsPath in O(1) via slicing.
	// Example:
	//   Process path: "/src"
	//   processRootPathLen: 4
	//   AbsPath: "/src/photos/a.jpg"
	//   RelPath: "photos/a.jpg"
	processRootPathLen int

	// Cached absolute path (built lazily on first AbsPath call).
	// absPath caches the built absolute absPath (no trailing NUL).
	absPath []byte

	// Lazy-open/read state for ReadAll/Read/Fd paths.
	// fh points at the currently open file handle.
	fh *fileHandle

	// fhOpened indicates whether fh is open.
	fhOpened *bool

	// mode tracks ReadAll/ReadAllOwned vs Read usage state.
	mode fileMode

	// Worker-local scratch/reuse state shared across File methods.
	// worker provides internal scratch slots for path/read operations.
	worker *FileWorker
}

// AbsPath returns the absolute file path (without the trailing NUL).
//
// Prefer this over string concatenation: it reuses a scratch buffer to
// avoid per-file allocations and handles path separators correctly.
//
// The returned slice is ephemeral and only valid during the callback.
// Copy if you need to retain it.
func (f *File) AbsPath() []byte {
	if len(f.absPath) > 0 {
		return f.absPath
	}

	baseLen := f.dirPath.lenWithoutNul()
	nameLen := f.name.lenWithoutNul()

	sep := 0
	if baseLen > 0 && f.dirPath[baseLen-1] != os.PathSeparator {
		sep = 1
	}

	needed := baseLen + sep + nameLen

	buf := f.worker.allocateScratchAt(scratchSlotAbsPath, needed)

	buf = buf[:0]
	if baseLen > 0 {
		buf = append(buf, f.dirPath[:baseLen]...)
		if sep == 1 {
			buf = append(buf, os.PathSeparator)
		}
	}

	buf = append(buf, f.name[:nameLen]...)

	f.absPath = buf

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
	if f.processRootPathLen >= len(abs) {
		return abs
	}

	start := f.processRootPathLen
	if abs[start] == os.PathSeparator {
		start++
	}

	return abs[start:]
}

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

// Stat returns file metadata.
//
// Lazy: the stat syscall is made only when Stat is called.
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
	return f.fetchStat()
}

// ReadAllOption configures [File.ReadAll] and [File.ReadAllOwned].
//
// Defaults when no option sets a field:
//   - sizeHint: 0 (disabled)
//   - maxBytes: 2 GiB
//
// If multiple options are passed, the last non-zero value for each field wins.
type ReadAllOption struct {
	// sizeHint is the expected file size to pre-size buffers.
	// 0 means "no hint".
	sizeHint int
	// maxBytes is the maximum number of file bytes allowed to be read.
	// 0 means "use default" (2 GiB).
	maxBytes int
}

// WithSizeHint provides an expected file size to optimize buffer allocation.
//
// Use when file sizes are known or predictable (e.g., uniform log entries,
// fixed-format records) to avoid buffer resizing without a stat syscall.
//
// The hint is a suggestion, not a limit. Files larger than the hint are
// read completely; smaller files don't waste the extra space (only the
// actual content is returned).
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

// WithMaxBytes sets an upper bound on bytes read by [File.ReadAll] and
// [File.ReadAllOwned].
//
// Values must be > 0.
//
// If omitted, the default limit is 2 GiB.
func WithMaxBytes(limit int) ReadAllOption {
	return ReadAllOption{maxBytes: limit}
}

// ReadAll reads and returns the full file content.
//
// The returned slice points into a reusable buffer and is only valid during
// the callback. To retain data beyond the callback, copy it or allocate owned
// memory via [FileWorker.AllocateOwned].
//
// Empty files return a non-nil empty slice ([]byte{}, nil).
//
// Single-use: ReadAll can only be called once per File and is mutually exclusive
// with [File.Read] and [File.ReadAllOwned].
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
//  1. The hint from [WithSizeHint], if provided
//  2. A default 4KiB buffer, grown as needed
//
// ReadAll enforces a max-byte limit (default 2 GiB). Use [WithMaxBytes] to
// override it.
//
// If the file exceeds the limit, ReadAll returns [ErrFileTooLarge].
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
//	owned := w.AllocateOwned(len(data))
//	copy(owned.Buf, data)
//	return &Result{Len: len(owned.Buf)}, nil
func (f *File) ReadAll(opts ...ReadAllOption) ([]byte, error) {
	readOpts, err := parseReadAllOptions(opts)
	if err != nil {
		return nil, err
	}

	err = f.prepareReadAll()
	if err != nil {
		return nil, err
	}

	// Buffer size priority: sizeHint > default.
	var size int

	if readOpts.sizeHint > 0 {
		// sizeHint is advisory for pre-allocation only; never fail early on it.
		size = min(readOpts.sizeHint, readOpts.maxBytes)
	}

	// +1 to detect growth / read past expected size.
	readSize := min(max(size+1, defaultReadBufSize), readOpts.maxBytes)

	// Internal read scratch uses a reserved worker slot.
	// allocateScratchAt returns len=0, so reslice to expose readSize bytes.
	readBuf := f.worker.allocateScratchAt(scratchSlotFileBytes, readSize)[:readSize]

	n, out, err := f.readAllInto(readBuf, 0, readOpts.maxBytes, func(buf []byte, minLen int) ([]byte, error) {
		nextLen, growErr := nextReadAllBufferLen(len(buf), minLen, readOpts.maxBytes)
		if growErr != nil {
			return nil, growErr
		}

		return append(buf, make([]byte, nextLen-len(buf))...), nil
	})
	if err != nil {
		return nil, err
	}

	// readAllInto may grow via append; persist the grown slice in the slot.
	f.worker.perCallbackScratchSlots[scratchSlotFileBytes] = out

	// Empty file.
	if n == 0 {
		return []byte{}, nil
	}

	return out[:n], nil
}

// ReadAllOwned reads the full file content into dst starting at offset.
//
// It writes into dst.Buf[offset:] and returns the updated Owned plus the number
// of bytes written.
//
// The returned Owned may differ from dst when growth happens; callers must use
// the returned value for further use/free.
//
// If dst does not have enough space, ReadAllOwned allocates a larger owned
// buffer, copies existing prefix bytes up to offset, frees the old owned
// buffer, and continues.
//
// Empty files return n=0 and leave dst unchanged.
//
// Single-use: ReadAllOwned can only be called once per File and is mutually
// exclusive with [File.ReadAll] and [File.Read].
//
// Returns an error when offset is outside [0, len(dst.Buf)].
// If the file changes type (becomes a directory or symlink) between scan and
// read, ReadAllOwned returns a skip error that Process ignores when returned
// by the callback.
//
// ReadAllOwned enforces a max-byte limit (default 2 GiB). Use [WithMaxBytes]
// to override it.
//
// If the file exceeds the limit, ReadAllOwned returns [ErrFileTooLarge].
func (f *File) ReadAllOwned(dst Owned, offset int, opts ...ReadAllOption) (Owned, int, error) {
	// Validate destination before mutating file state.
	if offset < 0 || offset > len(dst.Buf) {
		return dst, 0, errors.New("invalid offset")
	}

	readOpts, err := parseReadAllOptions(opts)
	if err != nil {
		return dst, 0, err
	}

	err = f.prepareReadAll()
	if err != nil {
		return dst, 0, err
	}

	n, out, err := f.readAllInto(dst.Buf, offset, readOpts.maxBytes, func(buf []byte, minLen int) ([]byte, error) {
		currentDataLen := len(buf) - offset
		minDataLen := minLen - offset

		nextDataLen, growErr := nextReadAllBufferLen(currentDataLen, minDataLen, readOpts.maxBytes)
		if growErr != nil {
			return nil, growErr
		}

		grown := f.worker.AllocateOwned(offset + nextDataLen)
		copy(grown.Buf, buf[:minLen-1])

		dst.Free()
		dst = grown

		return dst.Buf, nil
	})
	if err != nil {
		return dst, n, err
	}

	dst.Buf = out

	return dst, n, nil
}

// Read implements io.Reader for streaming access.
//
// Use when you need only a prefix or want to process in chunks without
// retaining the full content. Data read via Read() is NOT arena-allocated;
// caller provides and manages the buffer.
//
// Read may be called multiple times until it returns io.EOF. It is mutually
// exclusive with [File.ReadAll] and [File.ReadAllOwned], and returns an error
// if one of those methods was used first.
//
// If the file changes type (becomes a directory or symlink) between scan and
// read, Read returns a skip error that Process ignores when returned by the
// callback.
//
// Lazy: file is opened on first Read() call.
//
// Example (inside Process callback):
//
//	buf := w.AllocateScratch(32 * 1024)
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
		return 0, errors.New("cannot call after ReadAll/ReadAllOwned")
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

// readAllGrowFn grows buf to at least minLen and returns the grown buffer.
type readAllGrowFn func(buf []byte, minLen int) ([]byte, error)

// readAllInto reads file content into dst starting at destOffset.
//
// maxBytes limits bytes read from the file (excluding existing dst prefix data
// before destOffset). When capacity is exhausted, grow is called.
func (f *File) readAllInto(dst []byte, destOffset int, maxBytes int, grow readAllGrowFn) (int, []byte, error) {
	// destOffset==len(dst) is valid (zero remaining capacity).
	if destOffset < 0 || destOffset > len(dst) {
		return 0, dst, errors.New("invalid destination offset")
	}

	if maxBytes <= 0 {
		return 0, dst, errors.New("max bytes must be > 0")
	}

	maxWriteCursor := destOffset + maxBytes
	if maxWriteCursor < destOffset {
		return 0, dst, ErrFileTooLarge
	}

	buffer := dst

	// Fast path: one read handles empty/small files without entering the grow loop.
	initialReadEnd := min(len(buffer), maxWriteCursor)

	initialRead, isDir, err := f.fh.readInto(buffer[destOffset:initialReadEnd])
	if isDir {
		return 0, buffer, errSkipFile
	}

	if err != nil {
		return 0, buffer, err
	}

	writeCursor := destOffset + initialRead

	// A full initial read means there may be more bytes; continue until EOF/error.
	if initialRead == initialReadEnd-destOffset {
		var readFailure error

		for {
			if writeCursor == len(buffer) || writeCursor == maxWriteCursor {
				// Probe for one more byte to distinguish exact-fit EOF from
				// "more data available" before growing.
				var probe [1]byte

				probeRead, isDir, err := f.fh.readInto(probe[:])
				if isDir {
					return writeCursor - destOffset, buffer, errSkipFile
				}

				if err != nil {
					return writeCursor - destOffset, buffer, err
				}

				if probeRead == 0 {
					break
				}

				if writeCursor == maxWriteCursor {
					return writeCursor - destOffset, buffer, ErrFileTooLarge
				}

				if grow == nil {
					return writeCursor - destOffset, buffer, ErrFileTooLarge
				}

				grown, growErr := grow(buffer, writeCursor+1)
				if growErr != nil {
					return writeCursor - destOffset, buffer, growErr
				}

				if len(grown) <= writeCursor {
					return writeCursor - destOffset, buffer, ErrFileTooLarge
				}

				buffer = grown
				buffer[writeCursor] = probe[0]
				writeCursor++

				continue
			}

			chunkReadEnd := min(len(buffer), maxWriteCursor)

			chunkRead, isDir, err := f.fh.readInto(buffer[writeCursor:chunkReadEnd])
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

// prepareReadAll enforces ReadAll/ReadAllOwned access rules and read state.
//
// It rejects invalid mode transitions (after Read, or repeated ReadAll reads),
// lazily opens the file handle, and marks the file as fileModeBytes.
func (f *File) prepareReadAll() error {
	if f.mode == fileModeReader {
		return errors.New("cannot call after Read")
	}

	if f.mode == fileModeBytes {
		return errors.New("already called")
	}

	// Open file if needed.
	openErr := f.open()
	if openErr != nil {
		return openErr
	}

	f.mode = fileModeBytes

	return nil
}

type readAllOptions struct {
	sizeHint int
	maxBytes int
}

func parseReadAllOptions(opts []ReadAllOption) (readAllOptions, error) {
	maxInt := int(^uint(0) >> 1)
	// ReadAll may compute `size+1` for probe headroom; keep maxBytes <= MaxInt-1
	// so that addition is always int-safe.
	maxSizePlusOneSafe := maxInt - 1

	readOpts := readAllOptions{
		maxBytes: defaultReadAllMaxBytes,
	}

	for _, opt := range opts {
		if opt.sizeHint != 0 {
			readOpts.sizeHint = opt.sizeHint
		}

		if opt.maxBytes != 0 {
			readOpts.maxBytes = opt.maxBytes
		}
	}

	if readOpts.maxBytes <= 0 {
		return readOpts, errors.New("max bytes must be > 0")
	}

	if readOpts.maxBytes > maxSizePlusOneSafe {
		return readOpts, fmt.Errorf("max bytes %d exceeds platform int limit %d", readOpts.maxBytes, maxSizePlusOneSafe)
	}

	return readOpts, nil
}

func nextReadAllBufferLen(currentLen, minLen, maxBytes int) (int, error) {
	if minLen > maxBytes {
		return 0, fmt.Errorf("%w (max bytes: %d)", ErrFileTooLarge, maxBytes)
	}

	// Geometric growth: double once buffers are large; use 4KiB minimum for
	// small buffers. If the immediate demand is larger, grow exactly to need.
	growBy := max(currentLen, 4096)

	need := minLen - currentLen
	if growBy < need {
		growBy = need
	}

	nextLen := currentLen + growBy
	if nextLen < currentLen {
		return 0, fmt.Errorf("%w (max bytes: %d)", ErrFileTooLarge, maxBytes)
	}

	if nextLen > maxBytes {
		nextLen = maxBytes
	}

	if nextLen < minLen {
		return 0, fmt.Errorf("%w (max bytes: %d)", ErrFileTooLarge, maxBytes)
	}

	return nextLen, nil
}

// fileMode tracks which content access method has been used for a File.
// It enforces ReadAll/ReadAllOwned vs Read exclusivity without extra flags.
type fileMode uint8

const (
	// fileModeBytes indicates ReadAll/ReadAllOwned was used.
	fileModeBytes fileMode = iota + 1
	// fileModeReader indicates Read() was used.
	fileModeReader
	// defaultReadAllMaxBytes is the default max bytes ReadAll/ReadAllOwned
	// will read before returning [ErrFileTooLarge].
	//
	// 32-bit builds are not supported right now.
	defaultReadAllMaxBytes = 2 << 30 // 2GiB
)

// fetchStat performs a stat call.
// Non-regular files are mapped to errSkipFile so callers can ignore races.
func (f *File) fetchStat() (Stat, error) {
	st, kind, err := f.dh.statFile(f.name)
	if err != nil {
		if errors.Is(err, syscall.ELOOP) || kind == statKindSymlink {
			return Stat{}, errSkipFile
		}

		return Stat{}, err
	}

	if kind != statKindReg {
		return Stat{}, errSkipFile
	}

	return st, nil
}

// open lazily opens the file.
// Symlink/dir races are normalized to errSkipFile so callers can skip silently.
func (f *File) open() error {
	if f.fhOpened != nil && *f.fhOpened {
		return nil // already open
	}

	fh, err := f.dh.openFile(f.name)
	if err != nil {
		// Symlink detected (race: was regular file during scan).
		if errors.Is(err, syscall.ELOOP) || errors.Is(err, syscall.EISDIR) {
			return errSkipFile
		}

		return err
	}

	*f.fh = fh
	*f.fhOpened = true

	return nil
}
