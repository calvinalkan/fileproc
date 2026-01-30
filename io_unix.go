//go:build (darwin && !ios) || freebsd || openbsd || netbsd || dragonfly

// io_unix.go implements the internal I/O backend contract (see io_contract.go)
// for "mainstream" non-Linux Unix platforms:
//   - macOS (darwin, excluding iOS)
//   - the BSD family (FreeBSD/OpenBSD/NetBSD/DragonFly)
//
// The goal of this backend is to keep a reasonably fast, syscall-oriented
// implementation (openat-relative opens, no per-file path joins) without having
// to support the more unusual Unix variants (solaris/illumos/aix), which are
// handled by the "other" backend.
package fileproc

import (
	"fmt"
	"io"
	"io/fs"
	"os"
	"syscall"
	"unsafe"

	"golang.org/x/sys/unix"
)

// appendString copies a filename into the batch, appending a NUL terminator.
// Non-Linux backends use string names from os.File.ReadDir.
func (b *nameBatch) appendString(name string) {
	start := len(b.storage)
	b.storage = append(b.storage, name...)
	b.storage = append(b.storage, 0)
	b.names = append(b.names, b.storage[start:len(b.storage)])
}

// ============================================================================
// Directory enumeration (readdirHandle + readDirBatch)
// ============================================================================

const readDirBatchSize = 4096

// readdirHandle wraps a directory for enumeration.
//
// We store both:
//   - fd: used for openat-based operations
//   - f:  *os.File wrapper used for (*os.File).ReadDir
//
// Part of the internal I/O backend contract (see io_contract.go).
type readdirHandle struct {
	fd int
	f  *os.File
}

// openDirEnumerator opens a directory for entry enumeration.
// path must include its trailing NUL terminator.
func openDirEnumerator(path []byte) (readdirHandle, error) {
	p := pathStr(path)

	for {
		fd, err := unix.Open(p, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
		if err == syscall.EINTR {
			continue
		}
		if err != nil {
			return readdirHandle{fd: -1}, err
		}

		f := os.NewFile(uintptr(fd), p)
		return readdirHandle{fd: fd, f: f}, nil
	}
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

		// When Type() is unknown (common on some filesystems), we must lstat to
		// enforce "skip symlinks and non-regular files" semantics.
		if typ&fs.ModeType == 0 {
			name := e.Name()
			if reportSubdir == nil && !hasSuffix(name, suffix) {
				continue
			}

			kind, statErr := classifyAt(rh.fd, name)
			if statErr != nil {
				continue
			}

			switch kind {
			case statKindDir:
				if reportSubdir != nil {
					reportSubdir([]byte(name))
				}
			case statKindReg:
				if hasSuffix(name, suffix) {
					batch.appendString(name)
				}
			default:
				// Skip symlinks and special file types.
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

func classifyAt(dirfd int, name string) (statKind, error) {
	var st unix.Stat_t
	for {
		err := unix.Fstatat(dirfd, name, &st, unix.AT_SYMLINK_NOFOLLOW)
		if err == syscall.EINTR {
			continue
		}
		if err != nil {
			return statKindOther, err
		}
		break
	}

	switch st.Mode & unix.S_IFMT {
	case unix.S_IFDIR:
		return statKindDir, nil
	case unix.S_IFREG:
		return statKindReg, nil
	case unix.S_IFLNK:
		return statKindSymlink, nil
	default:
		return statKindOther, nil
	}
}

// ============================================================================
// Directory + file handles for processing (dirHandle/fileHandle)
// ============================================================================

// dirHandle wraps an open directory for openat-based file operations.
type dirHandle struct {
	fd     int
	ownsFd bool
}

// fileHandle wraps an open file descriptor.
type fileHandle struct {
	fd int
}

// openDir opens a directory for file operations.
// path must include its trailing NUL terminator.
func openDir(path []byte) (dirHandle, error) {
	for {
		fd, _, errno := syscall.Syscall(
			unix.SYS_OPEN,
			uintptr(unsafe.Pointer(&path[0])),
			uintptr(unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW),
			0,
		)
		if errno == syscall.EINTR {
			continue
		}
		if errno != 0 {
			return dirHandle{fd: -1}, errno
		}
		return dirHandle{fd: int(fd), ownsFd: true}, nil
	}
}

// openDirFromReaddir creates a dirHandle from an already-open readdirHandle.
// The returned dirHandle borrows the fd; closing the readdirHandle closes it.
func openDirFromReaddir(rh readdirHandle, _ string) (dirHandle, error) {
	return dirHandle{fd: rh.fd, ownsFd: false}, nil
}

func (d dirHandle) closeHandle() error {
	if !d.ownsFd || d.fd < 0 {
		return nil
	}

	err := syscall.Close(d.fd)
	if err != nil {
		return fmt.Errorf("close dir: %w", err)
	}

	return nil
}

// openat opens a file relative to a directory fd.
// name must include its trailing NUL terminator.
func openat(dirfd int, name []byte) (int, error) {
	for {
		fd, _, errno := syscall.Syscall6(
			unix.SYS_OPENAT,
			uintptr(dirfd),
			uintptr(unsafe.Pointer(&name[0])),
			uintptr(unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW|unix.O_NONBLOCK),
			0, 0, 0,
		)
		if errno == syscall.EINTR {
			continue
		}
		if errno != 0 {
			return -1, errno
		}
		return int(fd), nil
	}
}

func (d dirHandle) openFile(name []byte) (fileHandle, error) {
	if len(name) <= 1 { // empty or just NUL
		return fileHandle{fd: -1}, syscall.ENOENT
	}

	fd, err := openat(d.fd, name)
	if err != nil {
		return fileHandle{fd: -1}, err
	}

	return fileHandle{fd: fd}, nil
}

func (d dirHandle) statFile(name []byte) (Stat, statKind, error) {
	if len(name) <= 1 {
		return Stat{}, statKindOther, syscall.ENOENT
	}

	nameStr := string(name[:nameLen(name)])

	var st unix.Stat_t
	for {
		err := unix.Fstatat(d.fd, nameStr, &st, unix.AT_SYMLINK_NOFOLLOW)
		if err == syscall.EINTR {
			continue
		}
		if err != nil {
			return Stat{}, statKindOther, err
		}
		break
	}

	kind := statKindOther
	switch st.Mode & unix.S_IFMT {
	case unix.S_IFREG:
		kind = statKindReg
	case unix.S_IFDIR:
		kind = statKindDir
	case unix.S_IFLNK:
		kind = statKindSymlink
	}

	return Stat{
		Size:    st.Size,
		ModTime: st.Mtim.Nano(),
		Mode:    uint32(st.Mode),
		Inode:   st.Ino,
	}, kind, nil
}

// Read implements io.Reader.
func (f fileHandle) Read(buf []byte) (int, error) {
	if len(buf) == 0 {
		return 0, nil
	}

	var (
		bytesRead int
		err       error
	)
	for {
		bytesRead, err = syscall.Read(f.fd, buf)
		if err == syscall.EINTR {
			continue
		}
		break
	}

	if err != nil {
		return 0, fmt.Errorf("read: %w", err)
	}

	if bytesRead == 0 {
		return 0, io.EOF
	}

	return bytesRead, nil
}

func (f fileHandle) readInto(buf []byte) (int, bool, error) {
	var (
		bytesRead int
		err       error
	)
	for {
		bytesRead, err = syscall.Read(f.fd, buf)
		if err == syscall.EINTR {
			continue
		}
		break
	}

	if err == syscall.EISDIR {
		return 0, true, nil
	}

	if err != nil {
		return bytesRead, false, fmt.Errorf("read: %w", err)
	}

	return bytesRead, false, nil
}

func (f fileHandle) closeHandle() error {
	if f.fd < 0 {
		return nil
	}

	err := syscall.Close(f.fd)
	if err != nil {
		return fmt.Errorf("close file: %w", err)
	}

	return nil
}

func (f fileHandle) fdValue() uintptr {
	if f.fd < 0 {
		return 0
	}

	return uintptr(f.fd)
}
