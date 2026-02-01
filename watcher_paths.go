package fileproc

import (
	"math/bits"
	"os"
	"sync"
	"unsafe"
)

// watcher_paths.go implements the watcher's persistent path index as an open-addressed hash table.
//
// Data flow (scan path -> event):
//   1) watcher_scan.go provides (base,name) for each entry.
//   2) hashPath(base,name) picks a shard to reduce lock contention.
//   3) pathTable.findSlot probes a linear-probed table:
//        - hit: compare stored bytes via pathEqual (no allocs)
//        - miss: append full path bytes into pathStore, insert new entry
//   4) entry.st/gen are updated to drive Modify/Delete detection.
//   5) sweepDeletes iterates entries and tombstones those not seen in gen.
//
// Memory layout:
//   - pathStore: append-only []byte holding full paths; entries store (off,len).
//   - pathTable: []pathEntry + []slots (open addressing, linear probe).
//
// Why append-only:
//   - stored path bytes are referenced by entries and exposed via events.
//   - reusing space would invalidate existing offsets; append-only avoids that.
//
// Sizing and load factor:
//   - table is kept under ~0.7 load (count+tombstones) to cap probe length.
//   - tombstone-heavy tables are rebuilt to restore probe locality.
//
// Design goals:
//   - steady-state scans allocate zero per-file (only new files append bytes)
//   - event emission reuses stored path bytes (no rebuild)
//   - predictable memory: append-only store + bounded probe factor

const (
	// slotEmpty marks an unused slot in the open-addressed table.
	slotEmpty uint32 = 0
	// slotTombstone marks a deleted slot to preserve probe chains.
	slotTombstone uint32 = 1
	// slotIndexOffset keeps 0/1 reserved for empty/tombstone sentinels.
	slotIndexOffset uint32 = 2

	// defaultTableSize prevents tiny tables that rehash too often.
	defaultTableSize = 16
	// expectedPathBytesPerFile is a coarse pre-size hint for the path store.
	expectedPathBytesPerFile = 64
)

// pathStore is an append-only arena that holds full path bytes.
// Each entry stores (offset,length) into this buffer.
//
// Note: append-only is required because event emission may occur long after
// insertion; reusing space would invalidate path offsets.
type pathStore struct {
	// buf is the arena backing storage; never shrinks, only grows.
	buf []byte
}

// init pre-allocates the arena buffer based on expected size.
func (s *pathStore) init(expectedBytes int) {
	if expectedBytes <= 0 {
		return
	}

	s.buf = make([]byte, 0, expectedBytes)
}

// bytes returns a subslice for the stored path (or nil on bounds error).
func (s *pathStore) bytes(off, length uint32) []byte {
	start := int(off)
	end := start + int(length)
	if start < 0 || end < start || end > len(s.buf) {
		return nil
	}

	return s.buf[start:end]
}

// pathString returns a string view of the stored path without allocation.
// This uses unsafe conversion; callers must not mutate the underlying bytes.
func (s *pathStore) pathString(off, length uint32) string {
	b := s.bytes(off, length)

	if len(b) == 0 {
		return ""
	}

	return unsafe.String(&b[0], len(b))
}

// appendPath appends base+sep+name to the arena and returns (off,len).
// The stored bytes are used for event emission without rebuilding paths.
func (s *pathStore) appendPath(base nulTermPath, name nulTermName) (uint32, uint32) {
	baseLen, nameLen, sep, total := pathParts(base, name)
	if total == 0 {
		return 0, 0
	}

	off := s.grow(total)
	buf := s.buf

	if baseLen > 0 {
		copy(buf[off:off+baseLen], base[:baseLen])
	}
	if sep == 1 {
		buf[off+baseLen] = os.PathSeparator
	}
	if nameLen > 0 {
		copy(buf[off+baseLen+sep:off+total], name[:nameLen])
	}

	return uint32(off), uint32(total)
}

// grow extends the arena by size bytes and returns the previous length offset.
// It doubles capacity to keep amortized growth O(1) and avoid per-append allocs.
func (s *pathStore) grow(size int) int {
	off := len(s.buf)
	need := off + size
	if need < off {
		panic("pathStore overflow")
	}

	if need > cap(s.buf) {
		newCap := max(cap(s.buf)*2, need)
		newBuf := make([]byte, need, newCap)
		copy(newBuf, s.buf)
		s.buf = newBuf

		return off
	}

	s.buf = s.buf[:need]
	return off
}

// pathEntry is the watcher's record for a single file path.
type pathEntry struct {
	// hash is the FNV-1a hash of the full path bytes.
	hash uint64
	// pathOff/pathLen locate the path bytes inside pathStore.buf.
	pathOff uint32
	pathLen uint32
	// gen tracks the last scan generation that observed this entry.
	gen uint32
	// slot is the current slot index in the open-addressed table.
	slot uint32
	// alive indicates the entry is active; false => tombstoned.
	alive bool
	// st caches the last observed stat for Modify detection.
	st Stat
}

// pathTable is a linear-probed hash table of pathEntry indices.
//
// Slots store indices+offset to keep 0/1 reserved for empty/tombstone.
// Deletions leave tombstones so probe chains remain valid until rehash.
type pathTable struct {
	// entries stores all records (including tombstoned ones).
	entries []pathEntry
	// slots maps hash-probe positions to entry indices.
	slots []uint32
	// count is number of live entries.
	count int
	// tombstones tracks deleted slots to decide when to rehash.
	tombstones int
}

// init sizes the table based on expected entries.
func (t *pathTable) init(expected int) {
	if expected < 1 {
		expected = 1
	}

	// Keep load factor under ~0.7 at steady state.
	size := max(nextPow2((expected*10)/7+1), defaultTableSize)

	t.entries = make([]pathEntry, 0, expected)
	t.slots = make([]uint32, size)
	t.count = 0
	t.tombstones = 0
}

// ensure grows the table if additional entries would exceed the load factor.
// A low load factor keeps linear probes short and predictable.
func (t *pathTable) ensure(extra int) {
	if len(t.slots) == 0 {
		t.init(extra)

		return
	}

	load := t.count + t.tombstones + extra
	if load*10 < len(t.slots)*7 {
		return
	}

	newSize := max(nextPow2(len(t.slots)*2), defaultTableSize)

	t.rehash(newSize)
}

// rehash rebuilds slots into a fresh table of the given size.
// This removes tombstones and tightens probe chains for faster lookups.
func (t *pathTable) rehash(size int) {
	slots := make([]uint32, size)
	mask := uint64(size - 1)

	for i := range t.entries {
		entry := &t.entries[i]
		if !entry.alive {
			continue
		}

		pos := entry.hash & mask
		for {
			slot := slots[pos]
			if slot == slotEmpty {
				break
			}
			pos = (pos + 1) & mask
		}

		slots[pos] = uint32(i) + slotIndexOffset
		entry.slot = uint32(pos)
	}

	t.slots = slots
	t.tombstones = 0
}

// findSlot locates a matching entry or the best insertion slot.
// Returns (slotIdx, entryIdx, found).
func (t *pathTable) findSlot(hash uint64, base nulTermPath, name nulTermName, store *pathStore) (int, int, bool) {
	mask := uint64(len(t.slots) - 1)
	pos := hash & mask
	firstTomb := -1

	for {
		slot := t.slots[pos]
		switch slot {
		case slotEmpty:
			// Prefer first tombstone to keep clusters short.
			if firstTomb != -1 {
				return firstTomb, -1, false
			}
			return int(pos), -1, false
		case slotTombstone:
			// Keep probing; remember first tombstone for insertion.
			if firstTomb == -1 {
				firstTomb = int(pos)
			}
		default:
			idx := int(slot - slotIndexOffset)
			entry := &t.entries[idx]
			if entry.alive && entry.hash == hash && pathEqual(store, entry, base, name) {
				return int(pos), idx, true
			}
		}

		pos = (pos + 1) & mask
	}
}

// insert adds a new entry at the provided slot and returns its index.
func (t *pathTable) insert(hash uint64, off, length uint32, st Stat, gen uint32, slot int) int {
	idx := len(t.entries)
	entry := pathEntry{
		hash:    hash,
		pathOff: off,
		pathLen: length,
		gen:     gen,
		slot:    uint32(slot),
		alive:   true,
		st:      st,
	}
	if idx < len(t.entries) {
		t.entries[idx] = entry
	} else {
		t.entries = append(t.entries, entry)
	}

	prev := t.slots[slot]
	t.slots[slot] = uint32(idx) + slotIndexOffset
	if prev == slotTombstone {
		t.tombstones--
	}
	t.count++

	return idx
}

// remove tombstones the entry and updates table counters.
// Tombstones preserve probe chains for existing entries.
func (t *pathTable) remove(entry *pathEntry) {
	if !entry.alive {
		return
	}

	t.slots[entry.slot] = slotTombstone
	entry.alive = false
	t.count--
	t.tombstones++
}

// shouldRehash reports whether tombstone buildup warrants a rebuild.
// Rebuilds are triggered when tombstones dominate or load is too high.
func (t *pathTable) shouldRehash() bool {
	if len(t.slots) == 0 {
		return false
	}

	if t.tombstones > t.count {
		return true
	}

	load := t.count + t.tombstones
	return load*10 >= len(t.slots)*7
}

// watchShard bundles a table+store pair under one lock.
// Sharding reduces contention from concurrent worker updates.
type watchShard struct {
	mu    sync.Mutex
	table pathTable
	store pathStore
}

// watchIndex is a sharded hash index keyed by path hash.
type watchIndex struct {
	// shards hold independent table/store pairs.
	shards []watchShard
	// mask is len(shards)-1 for fast modulo with power-of-two shards.
	// Why: shard selection is on the hot path; bitmask avoids division.
	mask uint64
}

// newWatchIndex builds a sharded index sized for expected file count.
// Shard count is rounded to power-of-two for cheap masking.
func newWatchIndex(shardCount int, expectedFiles int) watchIndex {
	if shardCount < 1 {
		shardCount = 1
	}

	shardCount = nextPow2(shardCount)

	idx := watchIndex{
		shards: make([]watchShard, shardCount),
		mask:   uint64(shardCount - 1),
	}

	perShard := 0
	if expectedFiles > 0 {
		perShard = (expectedFiles + shardCount - 1) / shardCount
	}

	for i := range idx.shards {
		idx.shards[i].table.init(perShard)
		idx.shards[i].store.init(perShard * expectedPathBytesPerFile)
	}

	return idx
}

// shard returns the shard responsible for the given hash.
// Sharding reduces lock contention in concurrent scans.
func (i *watchIndex) shard(hash uint64) *watchShard {
	return &i.shards[hash&i.mask]
}

// pathParts returns path lengths and separator for base+name joining.
func pathParts(base nulTermPath, name nulTermName) (baseLen, nameLen, sep, total int) {
	baseLen = base.lenWithoutNul()
	nameLen = name.lenWithoutNul()
	sep = 0
	if baseLen > 0 && base[baseLen-1] != os.PathSeparator {
		sep = 1
	}
	total = baseLen + sep + nameLen

	return
}

// pathEqual compares a stored path to base+name without allocating.
// This avoids building absolute paths on the hot path.
func pathEqual(store *pathStore, entry *pathEntry, base nulTermPath, name nulTermName) bool {
	baseLen, nameLen, sep, total := pathParts(base, name)
	if total != int(entry.pathLen) {
		return false
	}

	path := store.bytes(entry.pathOff, entry.pathLen)
	if len(path) != total {
		return false
	}

	for i := range baseLen {
		if path[i] != base[i] {
			return false
		}
	}
	if sep == 1 {
		if path[baseLen] != os.PathSeparator {
			return false
		}
	}
	for i := range nameLen {
		if path[baseLen+sep+i] != name[i] {
			return false
		}
	}

	return true
}

// hashPath computes the FNV-1a hash for base+sep+name.
// FNV-1a is fast, stable, and good enough for in-memory table distribution.
// Using bytes avoids constructing full path strings on hot paths.
func hashPath(base nulTermPath, name nulTermName) uint64 {
	const (
		fnvOffset = 14695981039346656037
		fnvPrime  = 1099511628211
	)

	h := uint64(fnvOffset)
	baseLen := base.lenWithoutNul()
	for i := range baseLen {
		h ^= uint64(base[i])
		h *= fnvPrime
	}

	if baseLen > 0 && base[baseLen-1] != os.PathSeparator {
		h ^= uint64(os.PathSeparator)
		h *= fnvPrime
	}

	nameLen := name.lenWithoutNul()
	for i := range nameLen {
		h ^= uint64(name[i])
		h *= fnvPrime
	}

	return h
}

// nextPow2 rounds n up to the next power of two.
func nextPow2(n int) int {
	if n <= 1 {
		return 1
	}

	return 1 << bits.Len(uint(n-1))
}
