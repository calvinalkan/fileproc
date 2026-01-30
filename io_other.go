//go:build windows || android || ios || solaris || illumos || aix

// io_other.go implements the internal I/O backend contract (see io_contract.go)
// for platforms where we don't (currently) maintain a syscall-level fast path.
//
// This backend intentionally uses only portable stdlib APIs (os.Open,
// (*os.File).ReadDir, filepath.Join, etc.). It is used for:
//   - windows
//   - android
//   - ios
//   - solaris / illumos
//   - aix
//
// The high-level pipeline remains the same across all platforms; only the I/O
// primitives differ.
package fileproc

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"syscall"
)

// appendString copies a filename into the batch, appending a NUL terminator.
// Non-Linux backends use string names from os.File.ReadDir.
func (b *nameBatch) appendString(name string) {
	start := len(b.storage)
	b.storage = append(b.storage, name...)
	b.storage = append(b.storage, 0)
	b.names = append(b.names, b.storage[start:len(b.storage)])
}

const readDirBatchSize = 4096

// ============================================================================
// Directory enumeration (readdirHandle + readDirBatch)
// ============================================================================

// readdirHandle wraps a directory for enumeration.
//
// Part of the internal I/O backend contract (see io_contract.go).
type readdirHandle struct {
	fd   int // kept for cross-platform symmetry; unused by this backend
	f    *os.File
	path string
}

// openDirEnumerator opens a directory for entry enumeration.
// path must include its trailing NUL terminator.
func openDirEnumerator(path []byte) (readdirHandle, error) {
	p := pathStr(path)

	info, err := os.Lstat(p)
	if err != nil {
		return readdirHandle{fd: -1}, err
	}
	if info.Mode()&fs.ModeSymlink != 0 {
		return readdirHandle{fd: -1}, syscall.ELOOP
	}
	if !info.IsDir() {
		return readdirHandle{fd: -1}, syscall.ENOTDIR
	}

	f, err := os.Open(p)
	if err != nil {
		return readdirHandle{fd: -1}, err
	}
	return readdirHandle{fd: 0, f: f, path: p}, nil
}

func (h readdirHandle) closeHandle() error {
	if h.f == nil {
		return nil
	}

	err := h.f.Close()
	if err != nil {
		return fmt.Errorf("close readdir: %w", err)
	}

	return nil
}

// readDirBatchImpl enumerates directory entries using (*os.File).ReadDir.
//
// Names appended to batch include their trailing NUL terminator.
//
// If reportSubdir is non-nil, it is called for each discovered subdirectory
// entry name (without a trailing NUL).
func readDirBatchImpl(rh readdirHandle, _ []byte, suffix string, batch *nameBatch, reportSubdir func(name []byte)) error {
	entries, err := rh.f.ReadDir(readDirBatchSize)
	for _, e := range entries {
		// Use Type() instead of IsDir() to avoid following symlinks.
		typ := e.Type()

		// Ignore symlinks entirely.
		if typ&fs.ModeSymlink != 0 {
			continue
		}

		// Directories (only relevant in recursive mode where reportSubdir != nil).
		if typ.IsDir() {
			if reportSubdir != nil {
				reportSubdir([]byte(e.Name()))
			}
			continue
		}

		// When Type() is unknown, lstat to avoid following symlinks and to
		// reliably detect directories and regular files.
		if typ&fs.ModeType == 0 {
			name := e.Name()
			if reportSubdir == nil && !hasSuffix(name, suffix) {
				continue
			}

			info, statErr := os.Lstat(filepath.Join(rh.path, name))
			if statErr != nil {
				continue
			}

			if info.Mode()&fs.ModeSymlink != 0 {
				continue
			}

			if info.IsDir() {
				if reportSubdir != nil {
					reportSubdir([]byte(name))
				}
				continue
			}

			if info.Mode().IsRegular() && hasSuffix(name, suffix) {
				batch.appendString(name)
			}

			continue
		}

		// Ignore non-regular file types (pipes, sockets, devices, irregular files).
		if typ&fs.ModeType != 0 {
			continue
		}

		name := e.Name()
		if !hasSuffix(name, suffix) {
			continue
		}

		batch.appendString(name)
	}

	if err == nil {
		return nil
	}
	if err == io.EOF {
		return io.EOF
	}
	return err
}

// ============================================================================
// Directory + file handles for processing (dirHandle/fileHandle)
// ============================================================================

// dirHandle wraps a directory path for file operations.
//
// This backend does not use openat; it constructs full paths via filepath.Join.
type dirHandle struct {
	path string
}

// fileHandle wraps an open file.
type fileHandle struct {
	f *os.File
}

// openDir opens a directory for file operations.
// path must include its trailing NUL terminator.
func openDir(path []byte) (dirHandle, error) {
	p := pathStr(path)

	info, err := os.Lstat(p)
	if err != nil {
		return dirHandle{}, err
	}
	if info.Mode()&fs.ModeSymlink != 0 {
		return dirHandle{}, syscall.ELOOP
	}
	if !info.IsDir() {
		return dirHandle{}, syscall.ENOTDIR
	}

	return dirHandle{path: p}, nil
}

// openDirFromReaddir creates a dirHandle from an already-open readdirHandle.
//
// The pipeline passes the directory string as known by the orchestration layer.
// In this backend we just store it and use filepath.Join for file opens.
func openDirFromReaddir(_ readdirHandle, path string) (dirHandle, error) {
	return dirHandle{path: path}, nil
}

func (d dirHandle) closeHandle() error {
	return nil // nothing to close
}

// openFile opens a file relative to this directory.
// name must include its trailing NUL terminator.
func (d dirHandle) openFile(name []byte) (fileHandle, error) {
	if len(name) <= 1 {
		return fileHandle{}, os.ErrNotExist
	}

	fullPath := filepath.Join(d.path, string(name[:len(name)-1]))

	f, err := os.Open(fullPath)
	if err != nil {
		return fileHandle{}, err
	}

	return fileHandle{f: f}, nil
}

func (d dirHandle) statFile(name []byte) (Stat, statKind, error) {
	if len(name) <= 1 {
		return Stat{}, statKindOther, os.ErrNotExist
	}

	fullPath := filepath.Join(d.path, string(name[:len(name)-1]))

	info, err := os.Lstat(fullPath)
	if err != nil {
		return Stat{}, statKindOther, err
	}

	kind := statKindOther
	if info.Mode()&os.ModeSymlink != 0 {
		kind = statKindSymlink
	} else if info.IsDir() {
		kind = statKindDir
	} else if info.Mode().IsRegular() {
		kind = statKindReg
	}

	return Stat{
		Size:    info.Size(),
		ModTime: info.ModTime().UnixNano(),
		Mode:    uint32(info.Mode()),
		Inode:   0,
	}, kind, nil
}

func (f fileHandle) readInto(buf []byte) (n int, isDir bool, err error) {
	n, err = f.f.Read(buf)
	if err != nil {
		// Treat EOF as a successful short read.
		if errors.Is(err, io.EOF) {
			return n, false, nil
		}

		// On some platforms, attempting to read a directory returns a generic
		// error; detect directories via Stat on the error path.
		if info, statErr := f.f.Stat(); statErr == nil && info.IsDir() {
			return 0, true, nil
		}

		return n, false, fmt.Errorf("read: %w", err)
	}

	return n, false, nil
}

// Read implements io.Reader.
func (f fileHandle) Read(buf []byte) (int, error) {
	n, err := f.f.Read(buf)
	if err == nil {
		return n, nil
	}
	if errors.Is(err, io.EOF) {
		return n, io.EOF
	}

	// If the file became a directory, surface EISDIR so File.Read can skip it.
	if info, statErr := f.f.Stat(); statErr == nil && info.IsDir() {
		return 0, syscall.EISDIR
	}

	return n, fmt.Errorf("read: %w", err)
}

func (f fileHandle) closeHandle() error {
	if f.f == nil {
		return nil
	}

	err := f.f.Close()
	if err != nil {
		return fmt.Errorf("close file: %w", err)
	}

	return nil
}

func (f fileHandle) fdValue() uintptr {
	if f.f == nil {
		return 0
	}

	return f.f.Fd()
}
