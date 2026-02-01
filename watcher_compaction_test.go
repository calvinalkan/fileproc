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

func Test_WatchShard_Compacts_PathStore_When_DeadBytes_Exceed_Threshold(t *testing.T) {
	t.Parallel()

	base := newNulTermPath("/dummy")

	var shard watchShard
	shard.table.init(1)
	shard.store.init(0)

	payload := strings.Repeat("a", 4096)
	names := make([]string, 0, 32)
	indices := make([]int, 0, 32)

	shard.mu.Lock()

	for shard.liveBytes < compactionMinBytes*2 {
		name := fmt.Sprintf("%s-%d", payload, len(names))
		idx := insertShardEntry(&shard, base, name)
		names = append(names, name)
		indices = append(indices, idx)
	}

	keepName := names[len(names)-1]
	for i := range len(indices) - 1 {
		entry := &shard.table.entries[indices[i]]
		if !entry.alive {
			continue
		}

		length := uint64(entry.pathLen)
		if shard.liveBytes >= length {
			shard.liveBytes -= length
		} else {
			shard.liveBytes = 0
		}

		shard.deadBytes += length
		shard.table.remove(entry)
	}

	if !shard.shouldCompact() {
		shard.mu.Unlock()
		t.Fatal("expected compaction threshold to be met")
	}

	beforeLen := len(shard.store.buf)
	shard.compactLocked()

	if shard.deadBytes != 0 {
		shard.mu.Unlock()
		t.Fatal("expected dead bytes to be cleared after compaction")
	}

	if shard.table.tombstones != 0 {
		shard.mu.Unlock()
		t.Fatal("expected tombstones to be cleared after compaction")
	}

	if shard.table.count != 1 {
		shard.mu.Unlock()
		t.Fatalf("expected 1 live entry, got %d", shard.table.count)
	}

	if len(shard.store.buf) != int(shard.liveBytes) {
		shard.mu.Unlock()
		t.Fatal("expected store len to match live bytes")
	}

	if len(shard.store.buf) >= beforeLen {
		shard.mu.Unlock()
		t.Fatal("expected store to shrink after compaction")
	}

	n := nulTermName(append([]byte(keepName), 0))
	hash := hashPath(base, n)

	_, idx, found := shard.table.findSlot(hash, base, n, &shard.store)
	if !found || !shard.table.entries[idx].alive {
		shard.mu.Unlock()
		t.Fatal("expected kept entry to survive compaction")
	}

	shard.mu.Unlock()
}

func Test_Watcher_Compacts_PathStore_When_OwnedPaths_Enabled(t *testing.T) {
	t.Parallel()

	base := newNulTermPath("/dummy")
	w := &Watcher{
		cfg:   options{OwnedPaths: true},
		index: newWatchIndex(1, 0),
	}
	shard := &w.index.shards[0]

	payload := strings.Repeat("b", 4096)

	shard.mu.Lock()

	for shard.liveBytes < compactionMinBytes {
		name := fmt.Sprintf("%s-%d", payload, shard.table.count)
		insertShardEntry(shard, base, name)
	}

	shard.mu.Unlock()

	beforeLen := len(shard.store.buf)
	if beforeLen == 0 {
		t.Fatal("expected non-empty store before compaction")
	}

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
