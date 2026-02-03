package fileproc

import (
	"fmt"
	"strings"
	"testing"
)

func Test_EventPath_Copies_When_OwnedPaths_Enabled(t *testing.T) {
	t.Parallel()

	base := newNulTermPath("/dummy")

	var shard watchShard
	shard.table.init(1)
	shard.store.init(0)

	shard.mu.Lock()
	idx := insertShardEntry(&shard, base, "file.txt")
	entry := &shard.table.entries[idx]

	w := &Watcher{cfg: options{OwnedPaths: true}}
	path := w.eventPath(&shard, entry.pathOff, entry.pathLen)

	snapshot := strings.Clone(path)
	if snapshot == "" {
		shard.mu.Unlock()
		t.Fatal("expected non-empty path")
	}

	shard.store.buf[entry.pathOff] = 'z'
	shard.mu.Unlock()

	if path != snapshot {
		t.Fatal("expected owned path to be stable after buffer mutation")
	}
}

func Test_Watcher_Compacts_PathStore_When_OwnedPaths_Enabled(t *testing.T) {
	t.Parallel()

	base := newNulTermPath("/dummy")
	w := &Watcher{
		cfg:   options{OwnedPaths: true},
		index: newWatchIndex(1, 0),
	}
	shard := &w.index.shards[0]

	shard.mu.Lock()

	// Insert 100 entries with gen=1.
	for i := range 100 {
		name := fmt.Sprintf("file-%03d.txt", i)
		insertShardEntry(shard, base, name)
	}

	shard.mu.Unlock()

	beforeLen := len(shard.store.buf)
	if beforeLen == 0 {
		t.Fatal("expected non-empty store before compaction")
	}

	// sweepDeletes with gen=2 deletes all entries (they have gen=1).
	// This triggers rehash (tombstones > count) which compacts paths.
	w.sweepDeletes(t.Context(), 2)

	if len(shard.store.buf) != 0 {
		t.Fatal("expected store to be compacted")
	}

	if shard.deadBytes != 0 {
		t.Fatal("expected dead bytes to be cleared after compaction")
	}

	if shard.table.count != 0 {
		t.Fatal("expected no live entries after full delete")
	}
}

func Test_WatchShard_Compacts_PathStore_On_Rehash_When_OwnedPaths_Enabled(t *testing.T) {
	t.Parallel()

	base := newNulTermPath("/dummy")

	var shard watchShard
	shard.table.init(100)
	shard.store.init(0)

	// Insert 100 entries (well under 4MB total threshold).
	indices := make([]int, 0, 100)

	shard.mu.Lock()

	for i := range 100 {
		name := fmt.Sprintf("file-%03d.txt", i)
		idx := insertShardEntry(&shard, base, name)
		indices = append(indices, idx)
	}

	// Remove 90 entries (triggers rehash due to tombstones > count).
	for i := range 90 {
		entry := &shard.table.entries[indices[i]]

		length := uint64(entry.pathLen)
		if shard.liveBytes >= length {
			shard.liveBytes -= length
		} else {
			shard.liveBytes = 0
		}

		shard.deadBytes += length
		shard.table.remove(entry)
	}

	if shard.table.tombstones <= shard.table.count {
		shard.mu.Unlock()
		t.Fatal("expected tombstones > count to trigger rehash")
	}

	storeBytesBefore := len(shard.store.buf)
	deadBytesBefore := shard.deadBytes

	// Simulate what sweepDeletes does: rehash when shouldRehash().
	// With OwnedPaths, this should also compact the path store.
	if shard.table.shouldRehash() {
		shard.rehashAndCompactPaths(true) // true = OwnedPaths enabled
	}

	shard.mu.Unlock()

	// Entries should be compacted.
	if len(shard.table.entries) != 10 {
		t.Fatalf("expected 10 entries after rehash, got %d", len(shard.table.entries))
	}

	// Path store should also be compacted when OwnedPaths enabled.
	if len(shard.store.buf) >= storeBytesBefore {
		t.Fatalf("expected path store to shrink: before=%d, after=%d", storeBytesBefore, len(shard.store.buf))
	}

	if shard.deadBytes >= deadBytesBefore {
		t.Fatalf("expected deadBytes to decrease: before=%d, after=%d", deadBytesBefore, shard.deadBytes)
	}
}

func Test_Watcher_Compacts_PathStore_When_DeadBytes_Dominate(t *testing.T) {
	t.Parallel()

	base := newNulTermPath("/dummy")
	w := &Watcher{
		cfg:   options{OwnedPaths: true},
		index: newWatchIndex(1, 0),
	}
	shard := &w.index.shards[0]

	const (
		largeCount = 80
		smallCount = maxWorkers
		largeSize  = 128 * 1024
	)

	largeSuffix := strings.Repeat("a", largeSize)
	smallIdx := make([]int, 0, smallCount)

	shard.mu.Lock()

	for i := range largeCount {
		name := fmt.Sprintf("large-%03d-%s", i, largeSuffix)
		insertShardEntry(shard, base, name)
	}

	for i := range smallCount {
		name := fmt.Sprintf("small-%03d", i)
		idx := insertShardEntry(shard, base, name)
		smallIdx = append(smallIdx, idx)
	}

	for _, idx := range smallIdx {
		shard.table.entries[idx].gen = 2
	}

	storeBytesBefore := len(shard.store.buf)

	shard.mu.Unlock()

	w.sweepDeletes(t.Context(), 2)

	if shard.table.count != smallCount {
		t.Fatalf("expected %d live entries after delete, got %d", smallCount, shard.table.count)
	}

	if shard.deadBytes != 0 {
		t.Fatalf("expected deadBytes to be cleared after compaction, got %d", shard.deadBytes)
	}

	if len(shard.store.buf) >= storeBytesBefore {
		t.Fatalf("expected path store to shrink: before=%d, after=%d", storeBytesBefore, len(shard.store.buf))
	}
}

func insertShardEntry(shard *watchShard, base nulTermPath, name string) int {
	shard.table.ensure(1)

	n := nulTermName(append([]byte(name), 0))
	hash := hashPath(base, n)

	slot, _, found := shard.table.findSlot(hash, base, n, &shard.store)
	if found {
		panic("duplicate insert")
	}

	off, length := shard.store.appendPath(base, n)
	shard.liveBytes += uint64(length)

	return shard.table.insert(hash, off, length, Stat{Size: int64(length)}, 1, slot)
}
