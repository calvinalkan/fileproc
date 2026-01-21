package fileproc

import (
	"context"
	"io"
)

// Internal stat classification used by stat-only processing.
type statKind uint8

const (
	statKindReg statKind = iota
	statKindDir
	statKindSymlink
	statKindOther
)

// Stat holds metadata for a file discovered by [ProcessStat].
//
// ModTime is expressed as Unix nanoseconds to avoid time.Time allocations
// in hot paths. Use time.Unix(0, st.ModTime) to convert when needed.
type Stat struct {
	Size    int64
	ModTime int64
	Mode    uint32
	Inode   uint64
}

// LazyFile provides lazy, on-demand access to file contents.
//
// The file is opened on first use (Read/Fd). The handle is owned by fileproc
// and is closed after the callback returns. LazyFile must not be retained.
//
// Implementations satisfy io.Reader so it can be used with io.ReadAll, io.Copy,
// bufio.NewReader, etc.
type LazyFile interface {
	io.Reader

	// Fd returns the underlying file descriptor/handle. Calling Fd may trigger
	// opening the file if it has not been opened yet.
	Fd() uintptr
}

type lazyFile struct {
	dh      dirHandle
	name    []byte
	fh      *fileHandle
	fhOpen  *bool
	openErr error
}

func newLazyFile(dh dirHandle, name []byte, fh *fileHandle, fhOpen *bool) *lazyFile {
	return &lazyFile{
		dh:     dh,
		name:   name,
		fh:     fh,
		fhOpen: fhOpen,
	}
}

func (l *lazyFile) Read(buf []byte) (int, error) {
	err := l.open()
	if err != nil {
		return 0, err
	}

	return l.fh.Read(buf)
}

func (l *lazyFile) Fd() uintptr {
	err := l.open()
	if err != nil {
		return 0
	}

	return l.fh.fdValue()
}

func (l *lazyFile) open() error {
	if l.fhOpen != nil && *l.fhOpen {
		return l.openErr
	}

	if l.openErr != nil {
		return l.openErr
	}

	fh, err := l.dh.openFile(l.name)
	if err != nil {
		l.openErr = err

		return err
	}

	*l.fh = fh
	*l.fhOpen = true

	return nil
}

// ProcessStatFunc is called for each file with its metadata and a LazyFile.
//
// The path slice aliases internal reusable buffers and is only valid until
// the callback returns. Copy it if needed.
//
// The LazyFile may be nil for operations that do not require file contents.
// It is opened on first use and closed after the callback returns.
//
// Return values follow the same semantics as [ProcessFunc].
type ProcessStatFunc[T any] func(path []byte, st Stat, f LazyFile) (*T, error)

// ProcessStat processes files in a directory, passing stat metadata and a
// lazy-opening file handle to fn.
//
// By default, only the specified directory is processed. Set opts.Recursive
// to true to process subdirectories recursively.
func ProcessStat[T any](ctx context.Context, path string, fn ProcessStatFunc[T], opts Options) ([]Result[T], []error) {
	proc := processor[T]{kind: procKindStat, fnStat: fn}

	return processEntry[T](ctx, path, opts, proc.run)
}
