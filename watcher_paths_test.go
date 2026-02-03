package fileproc

import (
	"fmt"
	"math/rand"
	"testing"
)

func Test_PathTable_Preserves_Live_Entries_When_Rehashing_During_Churn(t *testing.T) {
	t.Parallel()

	base := newNulTermPath("/dummy")

	var (
		store pathStore
		table pathTable
	)

	table.init(1)
	table.rehash(8)

	live := make(map[string]int)

	insert := func(name string) {
		idx := insertPathEntry(&table, &store, base, name)
		live[name] = idx
	}

	remove := func(name string) {
		idx, ok := live[name]
		if !ok {
			t.Fatalf("missing live entry %q", name)
		}

		table.remove(&table.entries[idx])
		delete(live, name)
	}

	for i := range 20 {
		insert(fmt.Sprintf("f-%02d", i))
	}

	for i := 0; i < 20; i += 2 {
		remove(fmt.Sprintf("f-%02d", i))
	}

	if table.shouldRehash() {
		table.rehash(len(table.slots))
	}

	for i := 20; i < 30; i++ {
		insert(fmt.Sprintf("f-%02d", i))
	}

	assertLiveEntries(t, &table, &store, base, mapKeys(live))
}

func Test_PathTable_Finds_Live_Entries_When_Randomized_Churn(t *testing.T) {
	t.Parallel()

	base := newNulTermPath("/dummy")

	var (
		store pathStore
		table pathTable
	)

	table.init(1)
	table.rehash(8)

	names := make([]string, 0, 64)
	rng := rand.New(rand.NewSource(1))
	nextID := 0

	insert := func() {
		name := fmt.Sprintf("f-%d", nextID)
		nextID++
		_ = insertPathEntry(&table, &store, base, name)
		names = append(names, name)
	}

	remove := func() {
		if len(names) == 0 {
			return
		}

		i := rng.Intn(len(names))
		name := names[i]

		// Lookup entry by name (indices change after rehash compaction).
		n := nulTermName(append([]byte(name), 0))
		hash := hashPath(base, n)

		_, idx, found := table.findSlot(hash, base, n, &store)
		if !found {
			return
		}

		table.remove(&table.entries[idx])

		last := names[len(names)-1]
		names[i] = last
		names = names[:len(names)-1]
	}

	const (
		ops     = 200
		maxLive = 50
	)

	for i := range ops {
		if len(names) == 0 || (len(names) < maxLive && rng.Intn(100) < 60) {
			insert()
		} else {
			remove()
		}

		if table.shouldRehash() {
			table.rehash(len(table.slots))
		}

		if i%20 == 0 {
			assertLiveEntries(t, &table, &store, base, names)
		}
	}

	assertLiveEntries(t, &table, &store, base, names)
}

func insertPathEntry(table *pathTable, store *pathStore, base nulTermPath, name string) int {
	table.ensure(1)

	n := nulTermName(append([]byte(name), 0))
	hash := hashPath(base, n)

	slot, _, found := table.findSlot(hash, base, n, store)
	if found {
		panic("duplicate insert")
	}

	off, length := store.appendPath(base, n)

	return table.insert(hash, off, length, Stat{Size: int64(length)}, 1, slot)
}

func assertLiveEntries(t *testing.T, table *pathTable, store *pathStore, base nulTermPath, names []string) {
	t.Helper()

	for _, name := range names {
		n := nulTermName(append([]byte(name), 0))
		hash := hashPath(base, n)

		_, idx, found := table.findSlot(hash, base, n, store)
		if !found {
			t.Fatalf("expected live entry %q", name)
		}

		if !table.entries[idx].alive {
			t.Fatalf("expected entry %q to be alive", name)
		}
	}
}

func mapKeys(in map[string]int) []string {
	keys := make([]string, 0, len(in))
	for k := range in {
		keys = append(keys, k)
	}

	return keys
}

func Test_PathTable_Rehash_Compacts_Entries_When_Tombstones_Exist(t *testing.T) {
	t.Parallel()

	base := newNulTermPath("/dummy")

	var (
		store pathStore
		table pathTable
	)

	table.init(100)

	// Insert 100 entries.
	live := make(map[string]int)

	for i := range 100 {
		name := fmt.Sprintf("file-%03d", i)
		idx := insertPathEntry(&table, &store, base, name)
		live[name] = idx
	}

	if len(table.entries) != 100 {
		t.Fatalf("expected 100 entries, got %d", len(table.entries))
	}

	// Remove 90 entries (keep 10).
	for i := range 90 {
		name := fmt.Sprintf("file-%03d", i)
		idx := live[name]
		table.remove(&table.entries[idx])
		delete(live, name)
	}

	if table.count != 10 {
		t.Fatalf("expected count=10, got %d", table.count)
	}

	if table.tombstones != 90 {
		t.Fatalf("expected tombstones=90, got %d", table.tombstones)
	}

	// Entries slice still has 100 items (90 dead).
	if len(table.entries) != 100 {
		t.Fatalf("expected entries slice len=100 before rehash, got %d", len(table.entries))
	}

	// Rehash should compact entries to only live ones.
	table.rehash(len(table.slots))

	// After rehash, entries slice should only contain live entries.
	if len(table.entries) != 10 {
		t.Fatalf("expected entries slice len=10 after rehash, got %d", len(table.entries))
	}

	if table.count != 10 {
		t.Fatalf("expected count=10 after rehash, got %d", table.count)
	}

	if table.tombstones != 0 {
		t.Fatalf("expected tombstones=0 after rehash, got %d", table.tombstones)
	}

	// Verify all live entries are still findable.
	assertLiveEntries(t, &table, &store, base, mapKeys(live))
}
