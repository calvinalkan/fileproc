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
//   - (none)
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
// Stored in pathArena entries and File.name.
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

// ============================================================================
// pathArena: Arena-Style Allocation for Directory Entries
// ============================================================================
//
// pathArena collects full paths using an "arena" allocation pattern that
// minimizes heap allocations when reading directories with many files.
//
// THE PROBLEM:
//
// A naive implementation would allocate each path separately:
//
//	paths := []string{}
//	for _, entry := range dirEntries {
//	    paths = append(paths, filepath.Join(dir, entry.Name())) // ALLOCATES per file
//	}
//
// For a directory with 1000 files, this causes 1000+ allocations. When
// processing thousands of directories, allocation overhead dominates.
//
// THE SOLUTION: Arena-Style Storage
//
// Instead of allocating each path separately, we pack all paths into a single
// contiguous byte buffer ("storage"), then create slices that point into this
// buffer ("entries"). This reduces allocations from O(files) to O(1).
//
// MEMORY LAYOUT:
//
// After appending "/root/file1", "/root/file2", and "/root/doc":
//
//	storage (one contiguous []byte allocation, includes trailing NULs):
//	┌───┬───┬───┬───┬───┬───┬───┬───┬───┬───┬───┬───┬───┬───┬───┬───┬───┬───┬───┬───┬───┬───┬───┬───┬───┬───┐
//	│ / │ r │ o │ o │ t │ / │ f │ 1 │\0 │ / │ r │ o │ o │ t │ / │ f │ 2 │\0 │ / │ r │ o │ o │ t │ / │ d │\0 │
//	└───┴───┴───┴───┴───┴───┴───┴───┴───┴───┴───┴───┴───┴───┴───┴───┴───┴───┴───┴───┴───┴───┴───┴───┴───┴───┘
//	  0   1   2   3   4   5   6   7   8   9  10  11  12  13  14  15  16  17  18  19  20  21  22  23  24  25
//	  ▲                               ▲   ▲                               ▲   ▲                           ▲
//	  │                               │   │                               │   │                           │
//	  └─────── entries[0] ────────────┘   └─────── entries[1] ────────────┘   └────── entries[2] ───────────┘
//	        storage[0:9]                        storage[9:18]                     storage[18:26]
//
//	entries ([]pathEntry - slice of headers, NOT new allocations):
//	┌─────────────────┬─────────────────┬─────────────────┐
//	│ ptr=&storage[0] │ ptr=&storage[9] │ ptr=&storage[18]│
//	│ len=9, cap=9    │ len=9, cap=9    │ len=8, cap=8    │
//	└─────────────────┴─────────────────┴─────────────────┘
//	      entries[0]        entries[1]        entries[2]
//
// KEY INSIGHT: Each entry in `entries` is just a couple slice headers (24 bytes
// each on 64-bit: pointer + length + capacity). The headers point into
// `storage` rather than owning their own memory. No per-file allocation occurs!
//
// WHY NUL TERMINATORS?
//
// The NUL byte (\0) after each name is required for Unix syscalls. By storing
// the NUL as part of each path and name slice, we can pass names directly to
// syscalls without any conversion or allocation.
//
// ALLOCATION COMPARISON:
//
//	Directory with 1000 files:
//	┌─────────────────────┬──────────────────────────────────┐
//	│ Approach            │ Heap Allocations                 │
//	├─────────────────────┼──────────────────────────────────┤
//	│ []string            │ ~1000 (one per filename)         │
//	│ pathArena (arena)   │ ~2 (storage + entries slice)     │
//	└─────────────────────┴──────────────────────────────────┘
//
// USAGE PATTERNS:
//
//	// For syscalls (NUL included, ready to use):
//	fd, err := unix.Openat(dirfd, &name[0], flags, 0)
//
//	// For display/logging (exclude NUL):
//	fmt.Println(path.String()) // "/root/file1"
type pathArena struct {
	// storage is the arena: a single contiguous byte buffer that holds
	// all paths packed together with NUL terminators between them.
	// Pre-sized to avoid growth during directory reading.
	storage []byte

	// entries contains headers pointing into storage. Each entry provides
	// both full path and basename (both NUL-terminated). These are NOT
	// separate allocations - they're just "views" into storage.
	entries []pathEntry
}

type pathEntry struct {
	path nulTermPath // NUL-terminated full path
	name nulTermName // NUL-terminated basename (slice into path)
}

// reset prepares the arena for reuse, preserving allocated capacity.
//
// The storageCap parameter hints at expected total bytes for all paths.
// Typically set to len(dirBuf) * 2, estimating that names occupy about
// half of the raw dirent data returned by getdents64 (prefix added later).
//
// After reset, the arena is empty but retains its backing arrays, enabling
// zero-allocation reuse across multiple directories.
func (b *pathArena) reset(storageCap int) {
	// Grow storage capacity if needed, otherwise just reset length to 0.
	// The backing array is retained for reuse.
	if storageCap > 0 && cap(b.storage) < storageCap {
		b.storage = make([]byte, 0, storageCap)
	} else {
		b.storage = b.storage[:0]
	}

	// Pre-size the entries slice to avoid growth allocations during append.
	//
	// Heuristic: assume average filename is ~20 bytes (including NUL).
	// For storageCap=64KB, this pre-allocates space for ~3200 entries.
	// This eliminates the repeated allocations that occur when a slice
	// grows: 0→1→2→4→8→16→...→2048 (about 11 allocations for 1000 files).
	//
	// If the estimate is wrong, append still works - it just allocates.
	// But for typical directories, this eliminates all entries slice growth.
	namesCap := storageCap / 20
	if namesCap > 0 && cap(b.entries) < namesCap {
		b.entries = make([]pathEntry, 0, namesCap)
	} else {
		b.entries = b.entries[:0]
	}
}

// addPath appends a full path and basename to the arena.
// dirPath and name must include their trailing NUL terminators.
func (b *pathArena) addPath(dirPath nulTermPath, name nulTermName) {
	start := len(b.storage)
	dirLen := dirPath.LenWithoutNul()

	if dirLen > 0 {
		b.storage = append(b.storage, dirPath[:dirLen]...)
		if dirPath[dirLen-1] != os.PathSeparator {
			b.storage = append(b.storage, os.PathSeparator)
		}
	}

	nameStart := len(b.storage)
	b.storage = append(b.storage, name...) // name already includes NUL
	full := b.storage[start:len(b.storage)]
	b.entries = append(b.entries, pathEntry{
		path: full,
		name: full[nameStart-start:],
	})
}
