package fileproc

// ============================================================================
// nameBatch: Arena-Style Allocation for Directory Entries
// ============================================================================
//
// nameBatch collects filenames using an "arena" allocation pattern that
// minimizes heap allocations when reading directories with many files.
//
// THE PROBLEM:
//
// A naive implementation would allocate each filename separately:
//
//	names := []string{}
//	for _, entry := range dirEntries {
//	    names = append(names, entry.Name())  // ALLOCATES for each file!
//	}
//
// For a directory with 1000 files, this causes 1000+ allocations. When
// processing thousands of directories, allocation overhead dominates.
//
// THE SOLUTION: Arena-Style Storage
//
// Instead of allocating each filename separately, we pack all filenames
// into a single contiguous byte buffer ("storage"), then create slices
// that point into this buffer ("names"). This reduces allocations from
// O(files) to O(1).
//
// MEMORY LAYOUT:
//
// After appending "file1.md", "file2.md", and "doc.txt":
//
//	storage (one contiguous []byte allocation):
//	┌───┬───┬───┬───┬───┬───┬───┬───┬───┬───┬───┬───┬───┬───┬───┬───┬───┬───┬───┬───┬───┬───┬───┬───┬───┬───┐
//	│ f │ i │ l │ e │ 1 │ . │ m │ d │\0 │ f │ i │ l │ e │ 2 │ . │ m │ d │\0 │ d │ o │ c │ . │ t │ x │ t │\0 │
//	└───┴───┴───┴───┴───┴───┴───┴───┴───┴───┴───┴───┴───┴───┴───┴───┴───┴───┴───┴───┴───┴───┴───┴───┴───┴───┘
//	  0   1   2   3   4   5   6   7   8   9  10  11  12  13  14  15  16  17  18  19  20  21  22  23  24  25
//	  ▲                               ▲   ▲                               ▲   ▲                           ▲
//	  │                               │   │                               │   │                           │
//	  └───────── names[0] ────────────┘   └───────── names[1] ────────────┘   └──────── names[2] ─────────┘
//	         storage[0:9]                        storage[9:18]                      storage[18:26]
//
//	names ([][]byte - slice of slice headers, NOT new allocations):
//	┌─────────────────┬─────────────────┬─────────────────┐
//	│ ptr=&storage[0] │ ptr=&storage[9] │ ptr=&storage[18]│
//	│ len=9, cap=9    │ len=9, cap=9    │ len=8, cap=8    │
//	└─────────────────┴─────────────────┴─────────────────┘
//	      names[0]          names[1]          names[2]
//
// KEY INSIGHT: Each entry in `names` is just a slice header (24 bytes:
// pointer + length + capacity). The slice header points into `storage`
// rather than owning its own memory. No per-filename allocation occurs!
//
// WHY NUL TERMINATORS?
//
// The NUL byte (\0) after each filename is required for Unix syscalls.
// Functions like openat(2) expect C-style NUL-terminated strings. By
// storing the NUL as part of each name slice, we can pass names directly
// to syscalls without any conversion or allocation.
//
// ALLOCATION COMPARISON:
//
//	Directory with 1000 files:
//	┌─────────────────────┬──────────────────────────────────┐
//	│ Approach            │ Heap Allocations                 │
//	├─────────────────────┼──────────────────────────────────┤
//	│ []string            │ ~1000 (one per filename)         │
//	│ nameBatch (arena)   │ ~2 (storage + names slice)       │
//	└─────────────────────┴──────────────────────────────────┘
//
// USAGE PATTERNS:
//
//	// For syscalls (NUL included, ready to use):
//	fd, err := unix.Openat(dirfd, &name[0], flags, 0)
//
//	// For display/logging (exclude NUL):
//	fmt.Println(name.String())  // "file1.md"
//
//	// For path building (get length without NUL):
//	pathLen := prefixLen + name.LenWithoutNul()  // 8, not 9
//
// INVARIANT: Every slice in names includes its NUL terminator, so
// name[len(name)-1] == 0 always holds. This makes the contract explicit
// and allows direct use with syscalls.
type nameBatch struct {
	// storage is the arena: a single contiguous byte buffer that holds
	// all filenames packed together with NUL terminators between them.
	// Pre-sized to avoid growth during directory reading.
	storage []byte

	// names contains slice headers pointing into storage. Each slice
	// represents one filename INCLUDING its NUL terminator. These are
	// NOT separate allocations - they're just "views" into storage.
	names []nulTermName
}

// reset prepares the batch for reuse, preserving allocated capacity.
//
// The storageCap parameter hints at expected total bytes for all filenames.
// Typically set to len(dirBuf) * 2, estimating that filenames occupy about
// half of the raw dirent data returned by getdents64.
//
// After reset, the batch is empty but retains its backing arrays, enabling
// zero-allocation reuse across multiple directories.
func (b *nameBatch) reset(storageCap int) {
	// Grow storage capacity if needed, otherwise just reset length to 0.
	// The backing array is retained for reuse.
	if storageCap > 0 && cap(b.storage) < storageCap {
		b.storage = make([]byte, 0, storageCap)
	} else {
		b.storage = b.storage[:0]
	}

	// Pre-size the names slice to avoid growth allocations during append.
	//
	// Heuristic: assume average filename is ~20 bytes (including NUL).
	// For storageCap=64KB, this pre-allocates space for ~3200 names.
	// This eliminates the repeated allocations that occur when a slice
	// grows: 0→1→2→4→8→16→...→2048 (about 11 allocations for 1000 files).
	//
	// If the estimate is wrong, append still works - it just allocates.
	// But for typical directories, this eliminates all names slice growth.
	namesCap := storageCap / 20
	if namesCap > 0 && cap(b.names) < namesCap {
		b.names = make([]nulTermName, 0, namesCap)
	} else {
		b.names = b.names[:0]
	}
}

// addName appends a name to the batch. The name must include its NUL terminator.
func (b *nameBatch) addName(name nulTermName) {
	start := len(b.storage)
	b.storage = append(b.storage, name...) // name already includes NUL
	b.names = append(b.names, b.storage[start:len(b.storage)])
}
