//go:build linux && !android

package fileproc

// io_linux.go implements the internal I/O backend contract (see io_contract.go)
// for Linux.
//
// Linux is the performance-critical backend:
//   - Directory enumeration uses getdents64 (via syscall.ReadDirent) and parses
//     raw dirent64 structures in-place (low allocation).
//   - File opens use openat(2) relative to an open directory fd.
//
// The pipeline and worker orchestration code (fileproc.go, processor.go)
// is OS-agnostic and relies on the functions and types provided here.

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"syscall"
	"unsafe"

	"golang.org/x/sys/unix"
)

// ============================================================================
// Low-level open/openat helpers
// ============================================================================

// openat opens a file relative to a directory fd using a raw syscall.
//
// name must include its trailing NUL terminator.
func openat(dirfd int, name nulTermName) (int, error) {
	// Retry on EINTR without an upper bound, matching Go's standard library.
	for {
		fd, _, errno := syscall.Syscall6(
			syscall.SYS_OPENAT,
			uintptr(dirfd),
			uintptr(unsafe.Pointer(name.Ptr())),
			uintptr(unix.O_RDONLY|unix.O_CLOEXEC|unix.O_LARGEFILE|unix.O_NOFOLLOW|unix.O_NONBLOCK),
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

// ============================================================================
// Directory enumeration (readDirBatchImpl)
// ============================================================================

// linux_dirent64 offsets (from linux/dirent.h):
//
//	struct linux_dirent64 {
//	    ino64_t        d_ino;    // 8 bytes  (offset 0)
//	    off64_t        d_off;    // 8 bytes  (offset 8)
//	    unsigned short d_reclen; // 2 bytes  (offset 16)
//	    unsigned char  d_type;   // 1 byte   (offset 18)
//	    char           d_name[]; // variable (offset 19)
//	};
const (
	direntReclenOffset = 16
	direntTypeOffset   = 18
	direntNameOffset   = 19
	direntMinSize      = direntNameOffset

	// atFDCWD is AT_FDCWD (-100) as a uintptr for use with syscall.Syscall6.
	atFDCWD = ^uintptr(0) - 99
)

var errInvalidDirent = errors.New("invalid dirent")

// readDirBatchImpl reads directory entries using getdents64 (syscall.ReadDirent)
// and appends matching file names to batch.
//
// Names appended to batch include a trailing NUL terminator.
//
// If reportSubdir is non-nil, it is called for each discovered subdirectory
// entry name (without a trailing NUL).
func readDirBatchImpl(dh dirHandle, dirPath nulTermPath, buf []byte, suffix string, batch *pathArena, reportSubdir reportSubdirFunc) error {
	// Retry ReadDirent on EINTR without an upper bound, matching Go's stdlib.
	var (
		read int
		err  error
	)
	for {
		read, err = syscall.ReadDirent(dh.fd, buf)
		if err == syscall.EINTR {
			continue
		}

		break
	}

	if err != nil {
		return fmt.Errorf("readdirent: %w", err)
	}

	if read <= 0 {
		return io.EOF
	}

	data := buf[:read]
	for len(data) > 0 {
		if len(data) < direntMinSize {
			return errInvalidDirent
		}

		reclen := int(binary.NativeEndian.Uint16(data[direntReclenOffset:]))
		if reclen < direntMinSize || reclen > len(data) {
			return errInvalidDirent
		}

		entry := data[:reclen]
		data = data[reclen:]

		// Extract filename (ends at first NUL byte).
		nameBytes := entry[direntNameOffset:reclen]
		nulPos := 0

		for i, b := range nameBytes {
			if b == 0 {
				nulPos = i

				break
			}
		}

		if nulPos == 0 || isDotEntry(nameBytes[:nulPos]) {
			continue
		}

		entryType := entry[direntTypeOffset]
		name := nulTermName(nameBytes[:nulPos+1])

		switch entryType {
		case syscall.DT_DIR:
			if reportSubdir != nil {
				reportSubdir(name)
			}

		case syscall.DT_REG:
			if name.HasSuffix(suffix) {
				batch.addPath(dirPath, name)
			}

		case syscall.DT_UNKNOWN:
			info, statErr := classifyAt(dh.fd, name)
			if statErr != nil {
				// Can't classify (racy entry, permissions, etc.). Skip safely.
				break
			}

			if info.isSymlink {
				break
			}

			if info.isDir {
				if reportSubdir != nil {
					reportSubdir(name)
				}

				break
			}

			if info.isReg && name.HasSuffix(suffix) {
				batch.addPath(dirPath, name)
			}

		default:
			// Ignore symlinks and special file types (fifo, sockets, devices, ...).
		}
	}

	return nil
}

func isDotEntry(name []byte) bool {
	if len(name) == 1 && name[0] == '.' {
		return true
	}

	return len(name) == 2 && name[0] == '.' && name[1] == '.'
}

type classifyResult struct {
	isDir     bool
	isReg     bool
	isSymlink bool
}

// classifyAt classifies the named entry using fstatat(AT_SYMLINK_NOFOLLOW).
//
// Only used when d_type == DT_UNKNOWN.
func classifyAt(dirfd int, name nulTermName) (classifyResult, error) {
	var st unix.Stat_t

	for {
		_, _, errno := syscall.Syscall6(
			syscall.SYS_NEWFSTATAT,
			uintptr(dirfd),
			uintptr(unsafe.Pointer(name.Ptr())),
			uintptr(unsafe.Pointer(&st)),
			unix.AT_SYMLINK_NOFOLLOW,
			0, 0,
		)
		if errno == syscall.EINTR {
			continue
		}

		if errno != 0 {
			return classifyResult{}, fmt.Errorf("fstatat: %w", errno)
		}

		break
	}

	res := classifyResult{}

	switch st.Mode & unix.S_IFMT {
	case unix.S_IFDIR:
		res.isDir = true
	case unix.S_IFREG:
		res.isReg = true
	case unix.S_IFLNK:
		res.isSymlink = true
	}

	return res, nil
}

// ============================================================================
// Directory + file handles for processing (dirHandle/fileHandle)
// ============================================================================

// dirHandle wraps an open directory for getdents64/openat-based operations.
type dirHandle struct {
	fd int
}

// fileHandle wraps an open file descriptor.
type fileHandle struct {
	fd int
}

// openDir opens a directory for file operations.
// path must include its trailing NUL terminator.
func openDir(path nulTermPath) (dirHandle, error) {
	for {
		fd, _, errno := syscall.Syscall6(
			syscall.SYS_OPENAT,
			atFDCWD,
			uintptr(unsafe.Pointer(path.Ptr())),
			uintptr(unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_LARGEFILE|unix.O_NOFOLLOW),
			0, 0, 0,
		)
		if errno == syscall.EINTR {
			continue
		}

		if errno != 0 {
			return dirHandle{fd: -1}, errno
		}

		return dirHandle{fd: int(fd)}, nil
	}
}

func (d dirHandle) close() error {
	if d.fd < 0 {
		return nil
	}

	err := syscall.Close(d.fd)
	if err != nil {
		return fmt.Errorf("close dir: %w", err)
	}

	return nil
}

func (d dirHandle) openFile(name nulTermName) (fileHandle, error) {
	if len(name) <= 1 { // empty or just NUL
		return fileHandle{fd: -1}, syscall.ENOENT
	}

	fd, err := openat(d.fd, name)
	if err != nil {
		return fileHandle{fd: -1}, err
	}

	return fileHandle{fd: fd}, nil
}

func (d dirHandle) statFile(name nulTermName) (Stat, statKind, error) {
	if len(name) <= 1 {
		return Stat{}, statKindOther, syscall.ENOENT
	}

	var st unix.Stat_t
	for {
		_, _, errno := syscall.Syscall6(
			syscall.SYS_NEWFSTATAT,
			uintptr(d.fd),
			uintptr(unsafe.Pointer(name.Ptr())),
			uintptr(unsafe.Pointer(&st)),
			unix.AT_SYMLINK_NOFOLLOW,
			0, 0,
		)
		if errno == syscall.EINTR {
			continue
		}

		if errno != 0 {
			return Stat{}, statKindOther, fmt.Errorf("fstatat: %w", errno)
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
		Mode:    st.Mode,
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

func (f fileHandle) close() error {
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
		return ^uintptr(0)
	}

	return uintptr(f.fd)
}
