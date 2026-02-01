package fileproc

import (
	"context"
	"os"
	"sync/atomic"
)

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
// Used with: openDir and similar syscall wrappers.
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

// ptr returns a pointer to the first byte for use with syscalls.
// The caller must ensure p is non-empty.
func (p nulTermPath) ptr() *byte {
	return &p[0]
}

// lenWithoutNul returns the path length excluding the trailing NUL.
func (p nulTermPath) lenWithoutNul() int {
	if len(p) > 0 && p[len(p)-1] == 0 {
		return len(p) - 1
	}

	return len(p)
}

// joinName appends a NUL-terminated name to p and returns a new NUL-terminated path.
// Used to build child directory paths without string allocations.
func (p nulTermPath) joinName(name nulTermName) nulTermPath {
	parentLen := p.lenWithoutNul()
	child := make([]byte, 0, parentLen+1+len(name))

	if parentLen > 0 {
		child = append(child, p[:parentLen]...)
		if p[parentLen-1] != os.PathSeparator {
			child = append(child, os.PathSeparator)
		}
	}

	child = append(child, name...) // includes NUL

	return child
}

// joinNameString joins p and name and returns a string without the trailing NUL.
// Used for error messages and logging.
func (p nulTermPath) joinNameString(name nulTermName) string {
	parentLen := p.lenWithoutNul()
	nameLen := name.lenWithoutNul()

	sep := 0
	if parentLen > 0 && p[parentLen-1] != os.PathSeparator {
		sep = 1
	}

	buf := make([]byte, 0, parentLen+sep+nameLen)
	if parentLen > 0 {
		buf = append(buf, p[:parentLen]...)
		if sep == 1 {
			buf = append(buf, os.PathSeparator)
		}
	}

	buf = append(buf, name[:nameLen]...)

	return string(buf)
}

// nulTermName is a NUL-terminated entry name for syscalls.
//
// Used for both file and directory names from directory listings.
// Unlike [nulTermPath], this is just a name (no path separator components).
// The trailing NUL byte is always present and included in len().
//
// Stored in pathArena entries and File.name.
type nulTermName []byte

// lenWithoutNul returns the name length excluding the trailing NUL.
func (n nulTermName) lenWithoutNul() int {
	if len(n) > 0 && n[len(n)-1] == 0 {
		return len(n) - 1
	}

	return len(n)
}

// ptr returns a pointer to the first byte for use with syscalls.
// The caller must ensure n is non-empty.
func (n nulTermName) ptr() *byte {
	return &n[0]
}

// hasSuffix reports whether the filename ends with suffix.
// Empty suffix matches all filenames.
func (n nulTermName) hasSuffix(suffix string) bool {
	if suffix == "" {
		return true
	}

	nameLen := n.lenWithoutNul()
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
// pathArena collects entry names using an "arena" allocation pattern that
// minimizes heap allocations when reading directories with many files.
//
// THE PROBLEM:
//
// A naive implementation would allocate each name separately:
//
//	names := []string{}
//	for _, entry := range dirEntries {
//	    names = append(names, entry.Name()) // ALLOCATES per file
//	}
//
// For a directory with 1000 files, this causes 1000+ allocations. When
// processing thousands of directories, allocation overhead dominates.
//
// THE SOLUTION: Arena-Style Storage
//
// Instead of allocating each name separately, we pack all names into a single
// contiguous byte buffer ("storage"), then create slices that point into this
// buffer ("entries"). This reduces allocations from O(files) to O(1).
//
// MEMORY LAYOUT:
//
// After appending "file1", "file2", and "doc":
//
//	storage (one contiguous []byte allocation, includes trailing NULs):
//	┌───┬───┬───┬───┬───┬───┬───┬───┬───┬───┬───┬───┬───┬───┐
//	│ f │ i │ l │ e │ 1 │\0 │ f │ i │ l │ e │ 2 │\0 │ d │\0 │
//	└───┴───┴───┴───┴───┴───┴───┴───┴───┴───┴───┴───┴───┴───┘
//	  0   1   2   3   4   5   6   7   8   9  10  11  12  13
//	  ▲                   ▲   ▲                   ▲   ▲   ▲
//	  │                   │   │                   │   │   │
//	  └── entries[0] ─────┘   └── entries[1] ─────┘   └ entries[2]
//	      storage[0:6]             storage[6:12]          storage[12:14]
//
//	entries ([]nulTermName - slice of headers, NOT new allocations):
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
// the NUL as part of each name slice, we can pass names directly to syscalls
// without any conversion or allocation.
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
type pathArena struct {
	// storage is the arena: a single contiguous byte buffer that holds
	// all names packed together with NUL terminators between them.
	// Pre-sized to avoid growth during directory reading.
	storage []byte

	// entries contains headers pointing into storage. Each entry provides
	// the basename (NUL-terminated). These are NOT separate allocations -
	// they're just "views" into storage.
	entries []nulTermName

	// pending tracks in-flight chunk refs plus the scan worker's dispatch ref.
	pending atomic.Int32
}

// reset prepares the arena for reuse, preserving allocated capacity.
//
// The storageCap parameter hints at expected total bytes for all names.
// Typically set to len(dirBuf) * 2, estimating that names occupy about
// half of the raw dirent data returned by getdents64.
//
// After reset, the arena is empty but retains its backing arrays, enabling
// zero-allocation reuse across multiple directories.
func (b *pathArena) reset(storageCap int) {
	b.pending.Store(0)
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
		b.entries = make([]nulTermName, 0, namesCap)
	} else {
		b.entries = b.entries[:0]
	}
}

// addPath appends a basename to the arena.
// name must include its trailing NUL terminator.
func (b *pathArena) addPath(_ nulTermPath, name nulTermName) {
	start := len(b.storage)
	b.storage = append(b.storage, name...) // name already includes NUL
	stored := b.storage[start:len(b.storage)]
	b.entries = append(b.entries, nulTermName(stored))
}

// arenaPool wraps a free-list channel with cancellation awareness.
type arenaPool struct {
	// free is bounded to cap retained arena memory.
	free chan *pathArena
}

func newArenaPool(n int) arenaPool {
	p := arenaPool{free: make(chan *pathArena, n)}
	for range n {
		p.free <- &pathArena{}
	}

	return p
}

func (p arenaPool) get(ctx context.Context, storageCap int) *pathArena {
	select {
	case arena := <-p.free:
		arena.reset(storageCap)

		return arena
	case <-ctx.Done():
		return nil
	}
}

func (p arenaPool) put(arena *pathArena) {
	arena.reset(0)

	select {
	case p.free <- arena:
	default:
	}
}
