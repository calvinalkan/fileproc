package fileproc

import (
	"bytes"
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
//      (base hash is cached per directory to avoid re-hashing base bytes.)
//   3) pathTable.findSlot probes a linear-probed table:
//        - hit: compare stored bytes via pathEqual (no allocs)
//        - miss: append full path bytes into pathStore, insert new entry
//   4) entry.st/gen are updated to drive Modify/Delete detection.
//   5) sweepDeletes iterates entries and tombstones those not seen in gen.
//
// Memory layout:
//   - pathStore: append-only []byte holding full paths; entries store (off,len).
//   - pathTable: []pathEntry + []slots + []tags (open addressing, linear probe).
//
// Why append-only:
//   - stored path bytes are referenced by entries and exposed via events.
//   - reusing space would invalidate existing offsets; append-only avoids that.
//
// Memory model:
//   - By default Event.Path is a string view into pathStore.buf.
//   - Retaining Event.Path keeps the underlying buffer alive (GC),
//     even after the watcher stops.
//   - WithOwnedPaths copies Event.Path and allows compaction to reclaim bytes.
//   - Deletions do not reclaim path bytes; the store only grows unless compacted.
//
// Sizing and load factor:
//   - table is kept under ~0.7 load (count+tombstones) to cap probe length.
//   - tombstone-heavy tables are rebuilt to restore probe locality.
//   - 8-bit hash tags avoid touching entries on most probe mismatches.
//
// Design goals:
//   - optimize for scan throughput and delete sweeps; point lookups are secondary
//   - steady-state scans allocate zero per-file (only new files append bytes)
//   - event emission reuses stored path bytes (no rebuild)
//   - predictable memory: append-only store + bounded probe factor

const (
	// slotEmpty marks an unused slot in the open-addressed table.
	slotEmpty uint32 = 0
	// slotIndexOffset keeps 0/1 reserved so slot values never collide with tags.
	slotIndexOffset uint32 = 2

	// tagEmpty marks an unused slot tag.
	tagEmpty uint8 = 0
	// tagTombstone marks a deleted slot tag.
	tagTombstone uint8 = 1

	// defaultTableSize prevents tiny tables that rehash too often.
	defaultTableSize = 16
	// expectedPathBytesPerFile is a coarse pre-size hint for the path store.
	expectedPathBytesPerFile = 64

	// loadFactorNum/loadFactorDen define the max target load factor.
	// 0.7 keeps probe chains short with reasonable slot/tag overhead.
	loadFactorNum = 7
	loadFactorDen = 10

	// compactionMinDeadBytesTotal is the minimum dead byte threshold to compact paths,
	// scaled across shards.
	compactionMinDeadBytesTotal = 4 << 20
	// compactionDeadToLiveRatio triggers compaction when dead bytes dominate.
	compactionDeadToLiveRatio = 2
)

// maxInt avoids int overflow on 32-bit when pre-sizing buffers.
const maxInt = int(^uint(0) >> 1)

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

	// Avoid allocation on the event hot path; bytes remain stable in the arena.
	return unsafe.String(&b[0], len(b))
}

// appendPath appends base+sep+name to the arena and returns (off,len).
// The stored bytes are used for event emission without rebuilding paths.
func (s *pathStore) appendPath(base nulTermPath, name nulTermName) (uint32, uint32) {
	parts := pathParts(base, name)
	if parts.total == 0 {
		return 0, 0
	}

	off := s.grow(parts.total)
	buf := s.buf

	if parts.baseLen > 0 {
		copy(buf[off:off+parts.baseLen], base[:parts.baseLen])
	}

	if parts.sep == 1 {
		buf[off+parts.baseLen] = os.PathSeparator
	}

	if parts.nameLen > 0 {
		copy(buf[off+parts.baseLen+parts.sep:off+parts.total], name[:parts.nameLen])
	}

	return uint32(off), uint32(parts.total)
}

// appendBytes appends raw bytes to the arena and returns (off,len).
// Used by compaction to rewrite only live paths.
func (s *pathStore) appendBytes(b []byte) (uint32, uint32) {
	if len(b) == 0 {
		return 0, 0
	}

	off := s.grow(len(b))
	copy(s.buf[off:off+len(b)], b)

	return uint32(off), uint32(len(b))
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
// Slots store indices+offset; tags store 8-bit hash prefixes.
// Deletions leave tombstones so probe chains remain valid until rehash.
type pathTable struct {
	// entries stores all records (including tombstoned ones).
	entries []pathEntry
	// slots maps hash-probe positions to entry indices.
	slots []uint32
	// tags store 8-bit hash tags to skip entry loads on mismatches.
	tags []uint8
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

	// Keep load factor under ~0.5 at steady state.
	size := max(nextPow2((expected*loadFactorDen)/loadFactorNum+1), defaultTableSize)

	t.entries = make([]pathEntry, 0, expected)
	t.slots = make([]uint32, 0, size)
	t.slots = t.slots[:size]
	t.tags = make([]uint8, 0, size)
	t.tags = t.tags[:size]
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
	if load*loadFactorDen < len(t.slots)*loadFactorNum {
		return
	}

	// Double size to keep amortized probe cost low.
	newSize := max(nextPow2(len(t.slots)*2), defaultTableSize)

	t.rehash(newSize)
}

// rehash rebuilds slots into a fresh table of the given size.
// This removes tombstones, compacts entries, and tightens probe chains.
func (t *pathTable) rehash(size int) {
	slots := make([]uint32, 0, size)
	slots = slots[:size]
	tags := make([]uint8, 0, size)
	tags = tags[:size]
	mask := uint64(size - 1)

	// Compact entries: copy only live entries to new slice.
	newEntries := make([]pathEntry, 0, t.count)
	for i := range t.entries {
		entry := &t.entries[i]
		if !entry.alive {
			continue
		}

		newIdx := len(newEntries)
		newEntries = append(newEntries, *entry)

		tag := hashTag(entry.hash)

		pos := entry.hash & mask
		for tags[pos] != tagEmpty {
			pos = (pos + 1) & mask
		}

		slots[pos] = uint32(newIdx) + slotIndexOffset
		tags[pos] = tag
		newEntries[newIdx].slot = uint32(pos)
	}

	t.entries = newEntries
	t.slots = slots
	t.tags = tags
	t.tombstones = 0
}

// findSlot locates a matching entry or the best insertion slot.
// Uses an 8-bit hash tag to skip entry loads on most mismatches.
// Returns (slotIdx, entryIdx, found).
func (t *pathTable) findSlot(hash uint64, base nulTermPath, name nulTermName, store *pathStore) (int, int, bool) {
	mask := uint64(len(t.slots) - 1)
	pos := hash & mask
	firstTomb := -1
	tag := hashTag(hash)

	for {
		switch t.tags[pos] {
		case tagEmpty:
			// Prefer first tombstone to keep clusters short.
			if firstTomb != -1 {
				return firstTomb, -1, false
			}

			return int(pos), -1, false
		case tagTombstone:
			// Keep probing; remember first tombstone for insertion.
			if firstTomb == -1 {
				firstTomb = int(pos)
			}
		default:
			if t.tags[pos] == tag {
				idx := int(t.slots[pos] - slotIndexOffset)

				entry := &t.entries[idx]
				if entry.alive && entry.hash == hash && pathEqual(store, entry, base, name) {
					return int(pos), idx, true
				}
			}
		}

		// Linear probing; masked wrap keeps modulo cheap.
		pos = (pos + 1) & mask
	}
}

// insert adds a new entry at the provided slot and returns its index.
func (t *pathTable) insert(hash uint64, off, length uint32, st Stat, gen uint32, slot int) int {
	idx := len(t.entries)
	tag := hashTag(hash)

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

	t.slots[slot] = uint32(idx) + slotIndexOffset
	prevTag := t.tags[slot]

	t.tags[slot] = tag
	if prevTag == tagTombstone {
		// Reuse tombstones to keep load factor accurate.
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

	// Tombstone preserves probe chains for other entries.
	t.slots[entry.slot] = slotEmpty
	t.tags[entry.slot] = tagTombstone
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

	return load*loadFactorDen >= len(t.slots)*loadFactorNum
}

// watchShard bundles a table+store pair under one lock.
// Sharding reduces contention from concurrent worker updates.
type watchShard struct {
	// mu guards table/store updates within a shard.
	mu sync.Mutex
	// table holds the open-addressed entries for this shard.
	table pathTable
	// store holds path bytes referenced by this shard's entries.
	store pathStore
	// liveBytes tracks bytes referenced by live entries.
	liveBytes uint64
	// deadBytes tracks bytes referenced by tombstoned entries.
	deadBytes uint64
}

// shouldCompactPaths reports whether path store compaction is warranted.
func (s *watchShard) shouldCompactPaths(minDeadBytes uint64) bool {
	if s.deadBytes < minDeadBytes {
		return false
	}

	return s.deadBytes >= s.liveBytes*compactionDeadToLiveRatio
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

// rehashAndCompactPaths rehashes the table (compacting entries) and optionally
// compacts the path store if ownedPaths is true.
// Caller must hold shard.mu.
func (s *watchShard) rehashAndCompactPaths(ownedPaths bool) {
	if !ownedPaths {
		// Just rehash entries, don't touch path store.
		s.table.rehash(len(s.table.slots))

		return
	}

	// OwnedPaths enabled: compact both entries and paths.
	if s.table.count == 0 {
		s.table = pathTable{}
		s.table.init(1)
		s.store = pathStore{}
		s.liveBytes = 0
		s.deadBytes = 0

		return
	}

	newTable := pathTable{}
	newTable.init(s.table.count)

	newStore := pathStore{}
	newStore.init(clampToInt(s.liveBytes))

	newEntries := make([]pathEntry, 0, s.table.count)
	mask := uint64(len(newTable.slots) - 1)

	var liveBytes uint64

	for i := range s.table.entries {
		entry := &s.table.entries[i]
		if !entry.alive {
			continue
		}

		path := s.store.bytes(entry.pathOff, entry.pathLen)
		off, length := newStore.appendBytes(path)

		newEntry := pathEntry{
			hash:    entry.hash,
			pathOff: off,
			pathLen: length,
			gen:     entry.gen,
			alive:   true,
			st:      entry.st,
		}

		idx := len(newEntries)
		newEntries = append(newEntries, newEntry)

		pos := entry.hash & mask
		for newTable.tags[pos] != tagEmpty {
			pos = (pos + 1) & mask
		}

		newTable.slots[pos] = uint32(idx) + slotIndexOffset
		newTable.tags[pos] = hashTag(entry.hash)
		newEntries[idx].slot = uint32(pos)
		liveBytes += uint64(length)
	}

	newTable.entries = newEntries
	newTable.count = len(newEntries)
	newTable.tombstones = 0

	s.table = newTable
	s.store = newStore
	s.liveBytes = liveBytes
	s.deadBytes = 0
}

func clampToInt(n uint64) int {
	if n > uint64(maxInt) {
		return maxInt
	}

	return int(n)
}

type pathPartsInfo struct {
	baseLen int
	nameLen int
	sep     int
	total   int
}

// pathParts returns path lengths and separator for base+name joining.
func pathParts(base nulTermPath, name nulTermName) pathPartsInfo {
	baseLen := base.lenWithoutNul()

	sep := 0
	if baseLen > 0 && base[baseLen-1] != os.PathSeparator {
		sep = 1
	}

	nameLen := name.lenWithoutNul()
	total := baseLen + sep + nameLen

	return pathPartsInfo{baseLen: baseLen, nameLen: nameLen, sep: sep, total: total}
}

// pathEqual compares a stored path to base+name without allocating.
// This avoids building absolute paths on the hot path.
func pathEqual(store *pathStore, entry *pathEntry, base nulTermPath, name nulTermName) bool {
	parts := pathParts(base, name)
	if parts.total != int(entry.pathLen) {
		return false
	}

	path := store.bytes(entry.pathOff, entry.pathLen)
	if len(path) != parts.total {
		return false
	}

	if parts.baseLen > 0 && !bytes.Equal(path[:parts.baseLen], base[:parts.baseLen]) {
		return false
	}

	if parts.sep == 1 {
		if path[parts.baseLen] != os.PathSeparator {
			return false
		}
	}

	if parts.nameLen > 0 && !bytes.Equal(path[parts.baseLen+parts.sep:], name[:parts.nameLen]) {
		return false
	}

	return true
}

// hashPath computes the FNV-1a hash for base+sep+name.
// FNV-1a constants for hash functions.
const (
	fnvOffset uint64 = 14695981039346656037
	fnvPrime  uint64 = 1099511628211
)

// FNV-1a is fast, stable, and good enough for in-memory table distribution.
// Using bytes avoids constructing full path strings on hot paths.
func hashPath(base nulTermPath, name nulTermName) uint64 {
	return hashPathFromBaseWithPrime(hashBaseWithPrime(base, fnvOffset, fnvPrime), name, fnvPrime)
}

// hashBase precomputes the FNV-1a hash for base (+sep).
// Use this once per directory and extend with hashPathFromBase.
func hashBase(base nulTermPath) uint64 {
	return hashBaseWithPrime(base, fnvOffset, fnvPrime)
}

// hashPathFromBase extends a pre-hashed base with the name bytes.
// This avoids re-hashing the base path for every entry in the directory.
func hashPathFromBase(baseHash uint64, name nulTermName) uint64 {
	return hashPathFromBaseWithPrime(baseHash, name, fnvPrime)
}

// hashBaseWithPrime is the core base hashing used by hashBase/hashPath.
// It hashes base bytes and the optional path separator.
func hashBaseWithPrime(base nulTermPath, offset, prime uint64) uint64 {
	h := offset

	baseLen := base.lenWithoutNul()
	for i := range baseLen {
		h ^= uint64(base[i])
		h *= prime
	}

	if baseLen > 0 && base[baseLen-1] != os.PathSeparator {
		h ^= uint64(os.PathSeparator)
		h *= prime
	}

	return h
}

// hashPathFromBaseWithPrime is the core extender used by hashPathFromBase.
// It hashes only the name bytes onto the pre-hashed base state.
func hashPathFromBaseWithPrime(baseHash uint64, name nulTermName, prime uint64) uint64 {
	h := baseHash

	nameLen := name.lenWithoutNul()
	for i := range nameLen {
		h ^= uint64(name[i])
		h *= prime
	}

	return h
}

// hashTag returns an 8-bit tag used to skip entry loads on mismatched probes.
// Values 0 and 1 are reserved for empty/tombstone markers.
func hashTag(hash uint64) uint8 {
	tag := uint8(hash >> 56)
	if tag < 2 {
		tag += 2
	}

	return tag
}

// nextPow2 rounds n up to the next power of two.
func nextPow2(n int) int {
	if n <= 1 {
		return 1
	}

	return 1 << bits.Len(uint(n-1))
}
