package fileproc

import "os"

// ============================================================================
// Path types
// ============================================================================
//
// These types provide compile-time safety for the different path representations
// used throughout the package. All are zero-cost wrappers around []byte.
//
// NUL-terminated types (for syscalls):
//   - NulTermPath: path with trailing NUL (e.g., "/foo/bar\x00")
//   - NulTermName: entry name with trailing NUL (e.g., "file.txt\x00")
//
// Non-NUL types:
//   - RootRelPath: relative path without NUL (e.g., "subdir/file.txt")
//
// Why NUL-terminated?
//
// Unix syscalls (open, openat, stat, etc.) expect C-style NUL-terminated strings.
// Go's string type is not NUL-terminated, so we must append a NUL byte before
// passing to syscalls. These types make the requirement explicit and prevent
// accidentally passing non-NUL-terminated paths to syscalls.

// nulTermPath is a NUL-terminated absolute path for syscalls.
//
// The trailing NUL byte is always present and included in len().
// Used with: openDirEnumerator, openDir, and similar syscall wrappers.
//
// Create with NewNulTermPath or nulTermPath.Join.
type nulTermPath []byte

// newNulTermPath creates a NUL-terminated path from a string.
// This allocates; use sparingly (typically once at Process entry).
// Caller must ensure string is not already NUL-terminated.
func newNulTermPath(s string) nulTermPath {
	b := make([]byte, 0, len(s)+1)
	b = append(b, s...)
	b = append(b, 0)

	return b
}

// String returns the path without the trailing NUL.
// Allocates a new string; use for error messages and debugging.
// Used in io_unix.go and io_other.go backends.
func (p nulTermPath) String() string {
	if len(p) > 0 && p[len(p)-1] == 0 {
		return string(p[:len(p)-1])
	}

	return string(p)
}

// Ptr returns a pointer to the first byte for use with syscalls.
// The caller must ensure p is non-empty.
func (p nulTermPath) Ptr() *byte {
	return &p[0]
}

// LenWithoutNul returns the path length excluding the trailing NUL.
func (p nulTermPath) LenWithoutNul() int {
	if len(p) > 0 && p[len(p)-1] == 0 {
		return len(p) - 1
	}

	return len(p)
}

// nulTermName is a NUL-terminated entry name for syscalls.
//
// Used for both file and directory names from directory listings.
// Unlike [nulTermPath], this is just a name (no path separator components).
// The trailing NUL byte is always present and included in len().
//
// Stored in nameBatch.names and File.name.
type nulTermName []byte

// String returns the name without the trailing NUL.
// Used in io_unix.go and io_other.go backends.
func (n nulTermName) String() string {
	if len(n) > 0 && n[len(n)-1] == 0 {
		return string(n[:len(n)-1])
	}

	return string(n)
}

// LenWithoutNul returns the name length excluding the trailing NUL.
func (n nulTermName) LenWithoutNul() int {
	if len(n) > 0 && n[len(n)-1] == 0 {
		return len(n) - 1
	}

	return len(n)
}

// Ptr returns a pointer to the first byte for use with syscalls.
// The caller must ensure n is non-empty.
func (n nulTermName) Ptr() *byte {
	return &n[0]
}

// Bytes returns the name without the trailing NUL as a byte slice.
// The returned slice shares memory with n; do not modify.
func (n nulTermName) Bytes() []byte {
	if len(n) > 0 && n[len(n)-1] == 0 {
		return n[:len(n)-1]
	}

	return n
}

// HasSuffix reports whether the filename ends with suffix.
// Empty suffix matches all filenames.
func (n nulTermName) HasSuffix(suffix string) bool {
	if suffix == "" {
		return true
	}

	nameLen := n.LenWithoutNul()
	suffixLen := len(suffix)

	if nameLen < suffixLen {
		return false
	}

	start := nameLen - suffixLen
	for i := range suffixLen {
		if n[start+i] != suffix[i] {
			return false
		}
	}

	return true
}

// rootRelPath is a path relative to the root directory passed to [Process].
//
// Does NOT include a NUL terminator. Used for user-facing output:
// File.RelPathBorrowed(), error messages, etc. Never passed to syscalls.
//
// Example: for root "/home/user/project" and file at
// "/home/user/project/src/main.go", rootRelPath is "src/main.go".
type rootRelPath []byte

// String returns the path as a string.
// Returns "." for empty/root paths.
func (p rootRelPath) String() string {
	if len(p) == 0 {
		return "."
	}

	return string(p)
}

// Len returns the path length.
func (p rootRelPath) Len() int {
	return len(p)
}

// AppendTo appends p/name to buf and returns the resulting slice.
// Used for buffer reuse patterns where allocation is amortized.
func (p rootRelPath) AppendTo(buf []byte, name nulTermName) rootRelPath {
	buf = buf[:0]

	nameLen := name.LenWithoutNul()
	if len(p) == 0 {
		return append(buf, name[:nameLen]...)
	}

	buf = append(buf, p...)
	buf = append(buf, os.PathSeparator)

	return append(buf, name[:nameLen]...)
}
