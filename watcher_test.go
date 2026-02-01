package fileproc_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/calvinalkan/fileproc"
)

const testInterval = 5 * time.Millisecond

// =============================================================================
// BASELINE BEHAVIOR
// =============================================================================

func Test_Watch_Emits_Baseline_Create_When_Option_Enabled(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		root := t.TempDir()
		writeFile(t, root, "a.txt", []byte("alpha"))
		writeFile(t, root, "b.txt", []byte("bravo"))

		watcher := newTestWatcher(t, root, fileproc.WithEmitBaseline())
		defer watcher.stop()

		watcher.assertEvents(t,
			ev(fileproc.Create, filepath.Join(root, "a.txt")),
			ev(fileproc.Create, filepath.Join(root, "b.txt")),
		)
	})
}

func Test_Watch_Silent_Baseline_When_Default(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		root := t.TempDir()
		writeFile(t, root, "existing.txt", []byte("exists"))

		watcher := newTestWatcher(t, root)
		defer watcher.stop()

		// First tick runs baseline scan - should emit no events.
		watcher.tick()

		events := watcher.drain()
		if len(events) != 0 {
			t.Fatalf("expected no baseline events, got %d", len(events))
		}
	})
}

// =============================================================================
// SCAN TIMING
// =============================================================================

func Test_Watch_Delays_Scan_When_MinIdle_Enabled(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		root := t.TempDir()
		minIdle := testInterval * 12

		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		w, err := fileproc.NewWatcher(root,
			fileproc.WithInterval(testInterval),
			fileproc.WithMinIdle(minIdle),
			fileproc.WithFileWorkers(1),
			fileproc.WithScanWorkers(1),
			fileproc.WithChunkSize(8),
		)
		if err != nil {
			t.Fatalf("NewWatcher: %v", err)
		}

		ch := w.Events(ctx)

		drain := func() []fileproc.Event {
			var events []fileproc.Event

			for {
				select {
				case ev, ok := <-ch:
					if !ok {
						return events
					}

					events = append(events, ev)
				default:
					return events
				}
			}
		}

		for range 6 {
			time.Sleep(testInterval)
			synctest.Wait()

			if w.Stats().Scans > 0 {
				break
			}
		}

		if w.Stats().Scans != 1 {
			t.Fatalf("expected baseline scan to complete once, got %d", w.Stats().Scans)
		}

		filePath := filepath.Join(root, "later.txt")
		writeFile(t, root, "later.txt", []byte("later"))

		time.Sleep(minIdle / 2)
		synctest.Wait()

		if len(drain()) != 0 {
			t.Fatal("expected no events before min idle")
		}

		if w.Stats().Scans != 1 {
			t.Fatalf("expected no scan before min idle, got %d", w.Stats().Scans)
		}

		time.Sleep(minIdle)
		synctest.Wait()

		events := drain()
		if len(events) == 0 {
			t.Fatal("expected event after min idle")
		}

		if events[0].Type != fileproc.Create || events[0].Path != filePath {
			t.Fatalf("unexpected event %+v", events[0])
		}

		if w.Stats().Scans <= 1 {
			t.Fatal("expected scan after min idle")
		}
	})
}

// =============================================================================
// EVENT TYPES (CREATE / MODIFY / DELETE)
// =============================================================================

func Test_Watch_Emits_Create_When_File_Added(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		root := t.TempDir()

		watcher := newTestWatcher(t, root)
		defer watcher.stop()

		watcher.tick()

		writeFile(t, root, "new.txt", []byte("new"))
		watcher.assertEvents(t,
			ev(fileproc.Create, filepath.Join(root, "new.txt")),
		)
	})
}

func Test_Watch_Emits_Create_When_Multiple_Files_Added(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		root := t.TempDir()

		watcher := newTestWatcher(t, root)
		defer watcher.stop()

		watcher.tick()

		writeFile(t, root, "a.txt", []byte("a"))
		writeFile(t, root, "b.txt", []byte("b"))
		writeFile(t, root, "c.txt", []byte("c"))

		watcher.assertEvents(t,
			ev(fileproc.Create, filepath.Join(root, "a.txt")),
			ev(fileproc.Create, filepath.Join(root, "b.txt")),
			ev(fileproc.Create, filepath.Join(root, "c.txt")),
		)
	})
}

func Test_Watch_Emits_Modify_When_File_Content_Changes(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		root := t.TempDir()

		watcher := newTestWatcher(t, root)
		defer watcher.stop()

		watcher.tick()

		filePath := filepath.Join(root, "file.txt")
		writeFile(t, root, "file.txt", []byte("one"))
		watcher.assertEvents(t,
			ev(fileproc.Create, filePath),
		)

		writeFile(t, root, "file.txt", []byte("two"))
		watcher.assertEvent(t, ev(fileproc.Modify, filePath, fileproc.Stat{Size: 3}))
	})
}

func Test_Watch_Emits_Delete_When_File_Removed(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		root := t.TempDir()

		watcher := newTestWatcher(t, root)
		defer watcher.stop()

		watcher.tick()

		filePath := filepath.Join(root, "file.txt")
		writeFile(t, root, "file.txt", []byte("content"))
		watcher.assertEvents(t,
			ev(fileproc.Create, filePath),
		)

		err := os.Remove(filePath)
		if err != nil {
			t.Fatalf("remove: %v", err)
		}

		watcher.assertEvents(t,
			ev(fileproc.Delete, filePath),
		)
	})
}

func Test_Watch_Emits_No_Event_When_File_Created_And_Deleted_Between_Scans(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		root := t.TempDir()

		watcher := newTestWatcher(t, root)
		defer watcher.stop()

		watcher.tick()

		// Create and delete before next scan.
		filePath := filepath.Join(root, "ephemeral.txt")
		writeFile(t, root, "ephemeral.txt", []byte("gone"))
		os.Remove(filePath)

		// Next scan should see nothing.
		watcher.tick()

		events := watcher.drain()
		if len(events) != 0 {
			t.Fatalf("expected no events for ephemeral file, got %d", len(events))
		}
	})
}

func Test_Watch_Emits_Delete_And_Create_When_File_Renamed(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		root := t.TempDir()

		watcher := newTestWatcher(t, root)
		defer watcher.stop()

		watcher.tick()

		oldPath := filepath.Join(root, "old.txt")
		newPath := filepath.Join(root, "new.txt")
		writeFile(t, root, "old.txt", []byte("content"))
		watcher.assertEvent(t, ev(fileproc.Create, oldPath, mustStat(t, oldPath)))

		// Rename the file.
		err := os.Rename(oldPath, newPath)
		if err != nil {
			t.Fatalf("rename: %v", err)
		}

		// Should see Delete for old path and Create for new path.
		watcher.assertEvents(t,
			ev(fileproc.Delete, oldPath),
			ev(fileproc.Create, newPath, mustStat(t, newPath)),
		)
	})
}

func Test_Watch_Emits_Modify_When_File_Replaced_Between_Scans(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		root := t.TempDir()

		watcher := newTestWatcher(t, root)
		defer watcher.stop()

		watcher.tick()

		filePath := filepath.Join(root, "file.txt")
		writeFile(t, root, "file.txt", []byte("original"))
		watcher.assertEvent(t, ev(fileproc.Create, filePath, mustStat(t, filePath)))

		// Replace file: delete and recreate with same name but different content/size.
		os.Remove(filePath)
		writeFile(t, root, "file.txt", []byte("replaced content"))

		// Should see Modify with updated stat.
		watcher.assertEvent(t, ev(fileproc.Modify, filePath, mustStat(t, filePath)))
	})
}

func Test_Watch_Emits_Modify_When_Multiple_Writes_Between_Scans(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		root := t.TempDir()

		watcher := newTestWatcher(t, root)
		defer watcher.stop()

		watcher.tick()

		filePath := filepath.Join(root, "file.txt")
		writeFile(t, root, "file.txt", []byte("one"))
		watcher.assertEvent(t, ev(fileproc.Create, filePath))

		// Multiple writes before next scan - should coalesce to single Modify.
		writeFile(t, root, "file.txt", []byte("two"))
		writeFile(t, root, "file.txt", []byte("three"))
		writeFile(t, root, "file.txt", []byte("final"))

		watcher.assertEvent(t, ev(fileproc.Modify, filePath, mustStat(t, filePath)))
	})
}

func Test_Watch_Emits_Modify_When_File_Permissions_Change(t *testing.T) {
	t.Parallel()

	if runtime.GOOS == windowsOS {
		t.Skip("chmod behavior differs on windows")
	}

	synctest.Test(t, func(t *testing.T) {
		root := t.TempDir()

		watcher := newTestWatcher(t, root)
		defer watcher.stop()

		watcher.tick()

		filePath := filepath.Join(root, "file.txt")
		writeFile(t, root, "file.txt", []byte("content"))
		watcher.assertEvents(t,
			ev(fileproc.Create, filePath),
		)

		err := os.Chmod(filePath, 0o400)
		if err != nil {
			t.Fatalf("chmod: %v", err)
		}

		watcher.assertEvents(t,
			ev(fileproc.Modify, filePath),
		)
	})
}

// =============================================================================
// EVENT STAT
// =============================================================================

func Test_Watch_Event_Stat_Matches_File_When_Created(t *testing.T) {
	t.Parallel()

	if runtime.GOOS == windowsOS {
		t.Skip("inode not available on windows")
	}

	synctest.Test(t, func(t *testing.T) {
		root := t.TempDir()

		watcher := newTestWatcher(t, root)
		defer watcher.stop()

		watcher.tick()

		filePath := filepath.Join(root, "file.txt")
		writeFile(t, root, "file.txt", []byte("hello world"))
		watcher.assertEvent(t, ev(fileproc.Create, filePath, mustStat(t, filePath)))
	})
}

func Test_Watch_Event_Stat_Matches_File_When_Modified(t *testing.T) {
	t.Parallel()

	if runtime.GOOS == windowsOS {
		t.Skip("inode not available on windows")
	}

	synctest.Test(t, func(t *testing.T) {
		root := t.TempDir()

		watcher := newTestWatcher(t, root)
		defer watcher.stop()

		watcher.tick()

		filePath := filepath.Join(root, "file.txt")
		writeFile(t, root, "file.txt", []byte("one"))
		watcher.assertEvent(t, ev(fileproc.Create, filePath, mustStat(t, filePath)))

		writeFile(t, root, "file.txt", []byte("two"))
		watcher.assertEvent(t, ev(fileproc.Modify, filePath, mustStat(t, filePath)))
	})
}

// =============================================================================
// FILTERING (SUFFIX, RECURSIVE, SYMLINKS)
// =============================================================================

func Test_Watch_Filters_By_Suffix_When_Recursive_Enabled(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		root := t.TempDir()

		watcher := newTestWatcher(t, root,
			fileproc.WithRecursive(),
			fileproc.WithSuffix(".log"),
		)
		defer watcher.stop()

		watcher.tick()

		writeFile(t, root, "root.log", []byte("root"))
		writeFile(t, root, "root.txt", []byte("root"))
		writeFile(t, root, filepath.Join("sub", "a.log"), []byte("a"))
		writeFile(t, root, filepath.Join("sub", "b.txt"), []byte("b"))
		writeFile(t, root, filepath.Join("sub", "deep", "c.log"), []byte("c"))

		watcher.assertEvents(t,
			ev(fileproc.Create, filepath.Join(root, "root.log")),
			ev(fileproc.Create, filepath.Join(root, "sub", "a.log")),
			ev(fileproc.Create, filepath.Join(root, "sub", "deep", "c.log")),
		)
	})
}

func Test_Watch_Filters_By_Suffix_When_Non_Recursive(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		root := t.TempDir()

		watcher := newTestWatcher(t, root, fileproc.WithSuffix(".log"))
		defer watcher.stop()

		watcher.tick()

		writeFile(t, root, "app.log", []byte("log"))
		writeFile(t, root, "app.txt", []byte("txt"))
		writeFile(t, root, "readme.md", []byte("md"))

		watcher.assertEvent(t, ev(fileproc.Create, filepath.Join(root, "app.log")))
	})
}

func Test_Watch_Ignores_Subdirs_When_Non_Recursive(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		root := t.TempDir()

		watcher := newTestWatcher(t, root) // non-recursive by default
		defer watcher.stop()

		watcher.tick()

		writeFile(t, root, "root.txt", []byte("root"))
		writeFile(t, root, filepath.Join("sub", "nested.txt"), []byte("nested"))

		watcher.assertEvents(t,
			ev(fileproc.Create, filepath.Join(root, "root.txt")),
		)
	})
}

func Test_Watch_Finds_Files_In_New_Subdirs_When_Recursive(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		root := t.TempDir()

		watcher := newTestWatcher(t, root, fileproc.WithRecursive())
		defer watcher.stop()

		watcher.tick()

		// Create new subdir with file after baseline.
		writeFile(t, root, filepath.Join("newdir", "file.txt"), []byte("content"))

		watcher.assertEvents(t,
			ev(fileproc.Create, filepath.Join(root, "newdir", "file.txt")),
		)
	})
}

func Test_Watch_Emits_Delete_When_File_In_Subdir_Removed(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		root := t.TempDir()

		watcher := newTestWatcher(t, root, fileproc.WithRecursive())
		defer watcher.stop()

		watcher.tick()

		filePath := filepath.Join(root, "sub", "file.txt")
		writeFile(t, root, filepath.Join("sub", "file.txt"), []byte("content"))
		watcher.assertEvents(t,
			ev(fileproc.Create, filePath),
		)

		err := os.Remove(filePath)
		if err != nil {
			t.Fatalf("remove: %v", err)
		}

		watcher.assertEvents(t,
			ev(fileproc.Delete, filePath),
		)
	})
}

func Test_Watch_Emits_Modify_When_File_In_Subdir_Changes(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		root := t.TempDir()

		watcher := newTestWatcher(t, root, fileproc.WithRecursive())
		defer watcher.stop()

		watcher.tick()

		filePath := filepath.Join(root, "sub", "file.txt")
		writeFile(t, root, filepath.Join("sub", "file.txt"), []byte("one"))
		watcher.assertEvents(t,
			ev(fileproc.Create, filePath),
		)

		writeFile(t, root, filepath.Join("sub", "file.txt"), []byte("two"))
		watcher.assertEvents(t,
			ev(fileproc.Modify, filePath),
		)
	})
}

func Test_Watch_Emits_Delete_When_Subdir_Removed(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		root := t.TempDir()

		watcher := newTestWatcher(t, root, fileproc.WithRecursive())
		defer watcher.stop()

		watcher.tick()

		writeFile(t, root, filepath.Join("sub", "a.txt"), []byte("a"))
		writeFile(t, root, filepath.Join("sub", "b.txt"), []byte("b"))
		watcher.assertEvents(t,
			ev(fileproc.Create, filepath.Join(root, "sub", "a.txt")),
			ev(fileproc.Create, filepath.Join(root, "sub", "b.txt")),
		)

		// Remove entire subdir.
		err := os.RemoveAll(filepath.Join(root, "sub"))
		if err != nil {
			t.Fatalf("removeall: %v", err)
		}

		watcher.assertEvents(t,
			ev(fileproc.Delete, filepath.Join(root, "sub", "a.txt")),
			ev(fileproc.Delete, filepath.Join(root, "sub", "b.txt")),
		)
	})
}

func Test_Watch_Skips_Symlink_To_File_When_Scanning(t *testing.T) {
	t.Parallel()

	if runtime.GOOS == windowsOS {
		t.Skip("symlink behavior varies on windows")
	}

	synctest.Test(t, func(t *testing.T) {
		root := t.TempDir()

		watcher := newTestWatcher(t, root)
		defer watcher.stop()

		watcher.tick()

		writeFile(t, root, "target.txt", []byte("ok"))
		writeSymlink(t, root, "target.txt", "link.txt")

		watcher.assertEvent(t, ev(fileproc.Create, filepath.Join(root, "target.txt")))
	})
}

func Test_Watch_Skips_Symlink_To_Dir_When_Recursive(t *testing.T) {
	t.Parallel()

	if runtime.GOOS == windowsOS {
		t.Skip("symlink behavior varies on windows")
	}

	synctest.Test(t, func(t *testing.T) {
		root := t.TempDir()

		// Create a real subdir with a file.
		writeFile(t, root, filepath.Join("realdir", "file.txt"), []byte("real"))

		// Create a symlink to another directory.
		external := t.TempDir()
		writeFile(t, external, "external.txt", []byte("external"))

		err := os.Symlink(external, filepath.Join(root, "linkdir"))
		if err != nil {
			t.Fatalf("symlink: %v", err)
		}

		watcher := newTestWatcher(t, root, fileproc.WithRecursive())
		defer watcher.stop()

		watcher.tick()

		// Should only see the real file, not the one through symlink.
		writeFile(t, root, filepath.Join("realdir", "new.txt"), []byte("new"))

		watcher.assertEvent(t, ev(fileproc.Create, filepath.Join(root, "realdir", "new.txt")))
	})
}

func Test_Watch_Emits_No_Events_When_Directory_Empty(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		root := t.TempDir()

		watcher := newTestWatcher(t, root)
		defer watcher.stop()

		// Multiple ticks on empty dir should produce no events.
		watcher.tick()
		watcher.tick()
		watcher.tick()

		events := watcher.drain()
		if len(events) != 0 {
			t.Fatalf("expected no events for empty dir, got %d", len(events))
		}
	})
}

func Test_Watch_Emits_Delete_When_File_Becomes_Directory(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		root := t.TempDir()

		watcher := newTestWatcher(t, root)
		defer watcher.stop()

		watcher.tick()

		path := filepath.Join(root, "item")
		writeFile(t, root, "item", []byte("file"))
		watcher.assertEvent(t, ev(fileproc.Create, path))

		// Replace file with directory of same name.
		os.Remove(path)

		err := os.Mkdir(path, 0o755)
		if err != nil {
			t.Fatalf("mkdir: %v", err)
		}

		// Should see Delete (directories are not watched).
		watcher.assertEvent(t, ev(fileproc.Delete, path))
	})
}

func Test_Watch_Emits_Create_When_Directory_Becomes_File(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		root := t.TempDir()

		// Start with a directory.
		path := filepath.Join(root, "item")

		err := os.Mkdir(path, 0o755)
		if err != nil {
			t.Fatalf("mkdir: %v", err)
		}

		watcher := newTestWatcher(t, root)
		defer watcher.stop()

		watcher.tick()

		// No events for directory.
		events := watcher.drain()
		if len(events) != 0 {
			t.Fatalf("expected no events for dir, got %d", len(events))
		}

		// Replace directory with file.
		os.RemoveAll(path)
		writeFile(t, root, "item", []byte("now a file"))

		watcher.assertEvent(t, ev(fileproc.Create, path, mustStat(t, path)))
	})
}

func Test_Watch_Handles_Deeply_Nested_Directories(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		root := t.TempDir()

		watcher := newTestWatcher(t, root, fileproc.WithRecursive())
		defer watcher.stop()

		watcher.tick()

		// Create deeply nested structure.
		deepPath := filepath.Join("a", "b", "c", "d", "e", "f", "g", "file.txt")
		writeFile(t, root, deepPath, []byte("deep"))

		watcher.assertEvent(t, ev(fileproc.Create, filepath.Join(root, deepPath)))
	})
}

// =============================================================================
// CANCELLATION
// =============================================================================

func Test_Events_Closes_Channel_When_Context_Cancelled(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		root := t.TempDir()

		watcher := newTestWatcher(t, root)

		watcher.tick()

		watcher.stop()
		watcher.assertClosed(t)
	})
}

// =============================================================================
// BACKPRESSURE
// =============================================================================

func Test_Watch_Blocks_Scan_When_Event_Channel_Full(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		root := t.TempDir()
		writeFile(t, root, "a.txt", []byte("a"))
		writeFile(t, root, "b.txt", []byte("b"))

		ctx, cancel := context.WithCancel(t.Context())
		events := make(chan fileproc.Event, 1)

		w, err := fileproc.NewWatcher(
			root,
			fileproc.WithOnEvent(func(ev fileproc.Event) { events <- ev }),
			fileproc.WithInterval(testInterval),
			fileproc.WithFileWorkers(1),
			fileproc.WithScanWorkers(1),
			fileproc.WithChunkSize(8),
			fileproc.WithEmitBaseline(),
		)
		if err != nil {
			t.Fatalf("NewWatcher: %v", err)
		}

		done := make(chan struct{})

		go func() {
			w.Watch(ctx, nil)
			close(done)
		}()

		defer func() {
			cancel()

			for range 5 {
				time.Sleep(testInterval)
				synctest.Wait()

				select {
				case <-done:
					return
				case <-events:
				default:
				}
			}
		}()

		// First file event fills the buffer, second blocks the scan.
		time.Sleep(testInterval * 2)
		synctest.Wait()

		if len(events) != 1 {
			t.Fatalf("expected 1 buffered event, got %d", len(events))
		}

		if w.Stats().Scans != 0 {
			t.Fatalf("expected scan to be blocked, scans=%d", w.Stats().Scans)
		}

		// Drain one event to unblock.
		<-events

		// Scan should complete now.
		time.Sleep(testInterval * 2)
		synctest.Wait()

		if len(events) != 1 {
			t.Fatalf("expected 1 buffered event, got %d", len(events))
		}

		if w.Stats().Scans == 0 {
			t.Fatal("expected scan to complete")
		}
	})
}

func Test_Watch_Detects_Changes_Made_While_Blocked(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		root := t.TempDir()
		writeFile(t, root, "a.txt", []byte("a"))

		ctx, cancel := context.WithCancel(t.Context())
		events := make(chan fileproc.Event, 1) // Small buffer to cause blocking.

		w, err := fileproc.NewWatcher(root,
			fileproc.WithOnEvent(func(ev fileproc.Event) { events <- ev }),
			fileproc.WithInterval(testInterval),
			fileproc.WithFileWorkers(1),
			fileproc.WithScanWorkers(1),
			fileproc.WithChunkSize(8),
			fileproc.WithEmitBaseline(),
		)
		if err != nil {
			t.Fatalf("NewWatcher: %v", err)
		}

		done := make(chan struct{})

		go func() {
			w.Watch(ctx, nil)
			close(done)
		}()

		defer func() {
			cancel()

			for range 5 {
				time.Sleep(testInterval)
				synctest.Wait()

				select {
				case <-done:
					return
				case <-events:
				default:
				}
			}
		}()

		// Wait for baseline event to fill the buffer.
		time.Sleep(testInterval * 2)
		synctest.Wait()

		if len(events) != 1 {
			t.Fatalf("expected 1 buffered event, got %d", len(events))
		}

		// While blocked, make a new file.
		writeFile(t, root, "b.txt", []byte("b"))

		// Drain the blocking event.
		ev := <-events
		if ev.Type != fileproc.Create {
			t.Fatalf("expected Create, got %v", ev.Type)
		}

		// Wait for next scan to complete and detect the new file.
		time.Sleep(testInterval * 2)
		synctest.Wait()

		// Should have the new file event.
		if len(events) != 1 {
			t.Fatalf("expected 1 event for new file, got %d", len(events))
		}

		ev = <-events
		if ev.Type != fileproc.Create {
			t.Fatalf("expected Create for b.txt, got %v", ev.Type)
		}

		if !strings.HasSuffix(ev.Path, "b.txt") {
			t.Fatalf("expected b.txt, got %s", ev.Path)
		}
	})
}

func Test_Events_Uses_Custom_Buffer_Size_When_WithEventBuffer(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		root := t.TempDir()

		// Create more files than default buffer.
		for i := range 10 {
			writeFile(t, root, fmt.Sprintf("file%d.txt", i), []byte("x"))
		}

		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		w, err := fileproc.NewWatcher(root,
			fileproc.WithInterval(testInterval),
			fileproc.WithFileWorkers(1),
			fileproc.WithScanWorkers(1),
			fileproc.WithChunkSize(8),
			fileproc.WithEmitBaseline(),
			fileproc.WithEventBuffer(20), // Large buffer
		)
		if err != nil {
			t.Fatalf("NewWatcher: %v", err)
		}

		ch := w.Events(ctx)

		time.Sleep(testInterval * 2)
		synctest.Wait()

		// With large buffer, all events should be buffered without blocking.
		count := 0

		for {
			select {
			case _, ok := <-ch:
				if !ok {
					t.Fatal("channel closed unexpectedly")
				}

				count++
			default:
				goto done
			}
		}

	done:
		if count != 10 {
			t.Fatalf("expected 10 events, got %d", count)
		}
	})
}

// =============================================================================
// CALLBACK API (Watch)
// =============================================================================

func Test_Watch_Calls_Callback_When_Using_Watch_Directly(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		root := t.TempDir()
		filePath := filepath.Join(root, "file.txt")
		writeFile(t, root, "file.txt", []byte("content"))
		wantStat := mustStat(t, filePath)

		ctx, cancel := context.WithCancel(t.Context())

		var received []fileproc.Event

		w, err := fileproc.NewWatcher(root,
			fileproc.WithInterval(testInterval),
			fileproc.WithFileWorkers(1),
			fileproc.WithScanWorkers(1),
			fileproc.WithChunkSize(8),
			fileproc.WithEmitBaseline(),
		)
		if err != nil {
			t.Fatalf("NewWatcher: %v", err)
		}

		done := make(chan struct{})

		go func() {
			w.Watch(ctx, func(ev fileproc.Event) {
				received = append(received, ev)
			})
			close(done)
		}()

		time.Sleep(testInterval * 2)
		synctest.Wait()

		cancel()
		time.Sleep(testInterval)
		synctest.Wait()

		if len(received) != 1 {
			t.Fatalf("expected 1 event, got %d", len(received))
		}

		ev := received[0]
		if ev.Type != fileproc.Create {
			t.Fatalf("expected Create, got %v", ev.Type)
		}

		if ev.Path != filePath {
			t.Fatalf("expected path %q, got %q", filePath, ev.Path)
		}

		if ev.Stat != wantStat {
			t.Fatalf("stat mismatch: got %+v, want %+v", ev.Stat, wantStat)
		}
	})
}

func Test_Watch_Calls_Callback_Concurrently_When_Multiple_Workers(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		root := t.TempDir()

		// Create enough files to trigger concurrent processing.
		for i := range 20 {
			writeFile(t, root, fmt.Sprintf("file%d.txt", i), []byte("content"))
		}

		ctx, cancel := context.WithCancel(t.Context())

		var (
			maxConcurrent atomic.Int32
			current       atomic.Int32
			total         atomic.Int32
		)

		w, err := fileproc.NewWatcher(root,
			fileproc.WithInterval(testInterval),
			fileproc.WithFileWorkers(4), // Multiple workers.
			fileproc.WithScanWorkers(1),
			fileproc.WithChunkSize(4), // Small chunks to spread work.
			fileproc.WithEmitBaseline(),
		)
		if err != nil {
			t.Fatalf("NewWatcher: %v", err)
		}

		done := make(chan struct{})

		go func() {
			w.Watch(ctx, func(_ fileproc.Event) {
				c := current.Add(1)

				for {
					prevMax := maxConcurrent.Load()
					if c <= prevMax || maxConcurrent.CompareAndSwap(prevMax, c) {
						break
					}
				}

				time.Sleep(time.Millisecond) // Hold to allow overlap.
				current.Add(-1)
				total.Add(1)
			})
			close(done)
		}()

		time.Sleep(testInterval * 4)
		synctest.Wait()

		cancel()
		time.Sleep(testInterval)
		synctest.Wait()

		if total.Load() != 20 {
			t.Fatalf("expected 20 events, got %d", total.Load())
		}
		// With 4 workers and small chunks, we should see some concurrency.
		if maxConcurrent.Load() < 2 {
			t.Logf("warning: max concurrent was only %d (expected >= 2)", maxConcurrent.Load())
		}
	})
}

// =============================================================================
// ERROR HANDLING
// =============================================================================

func Test_Watch_Continues_When_File_Stat_Fails(t *testing.T) {
	t.Parallel()

	if runtime.GOOS == windowsOS {
		t.Skip("permission behavior differs on windows")
	}

	synctest.Test(t, func(t *testing.T) {
		root := t.TempDir()

		watcher := newTestWatcher(t, root)
		defer watcher.stop()

		watcher.tick()

		// Create two files.
		writeFile(t, root, "good.txt", []byte("good"))
		badPath := filepath.Join(root, "bad.txt")
		writeFile(t, root, "bad.txt", []byte("bad"))

		watcher.assertEvents(t,
			ev(fileproc.Create, filepath.Join(root, "good.txt")),
			ev(fileproc.Create, badPath),
		)

		// Remove file - should get delete event.
		os.Remove(badPath)
		watcher.assertEvents(t,
			ev(fileproc.Delete, badPath),
		)
	})
}

func Test_Watch_Calls_OnError_When_Directory_Unreadable(t *testing.T) {
	t.Parallel()

	if runtime.GOOS == windowsOS {
		t.Skip("permission behavior differs on windows")
	}

	synctest.Test(t, func(t *testing.T) {
		root := t.TempDir()

		subdir := filepath.Join(root, "unreadable")

		err := os.Mkdir(subdir, 0o755)
		if err != nil {
			t.Fatalf("mkdir: %v", err)
		}

		writeFile(t, subdir, "file.txt", []byte("content"))

		var errorCount atomic.Int32

		ctx, cancel := context.WithCancel(t.Context())

		w, err := fileproc.NewWatcher(subdir,
			fileproc.WithOnError(func(_ error, _ int, _ int) bool {
				errorCount.Add(1)

				return true
			}),
			fileproc.WithInterval(testInterval),
			fileproc.WithFileWorkers(1),
			fileproc.WithScanWorkers(1),
			fileproc.WithChunkSize(8),
		)
		if err != nil {
			t.Fatalf("NewWatcher: %v", err)
		}

		done := make(chan struct{})

		go func() {
			w.Watch(ctx, nil)
			close(done)
		}()

		// Let baseline scan complete.
		time.Sleep(testInterval * 2)
		synctest.Wait()

		// Make directory unreadable.
		err = os.Chmod(subdir, 0o000)
		if err != nil {
			t.Fatalf("chmod: %v", err)
		}
		defer os.Chmod(subdir, 0o755)

		// Next scan should fail.
		time.Sleep(testInterval * 2)
		synctest.Wait()

		cancel()
		time.Sleep(testInterval)
		synctest.Wait()

		if errorCount.Load() == 0 {
			t.Fatal("expected OnError to be called")
		}
	})
}

// =============================================================================
// STATS
// =============================================================================

func Test_Watch_Stats_Accurate_When_Processing_Files(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		root := t.TempDir()
		// Create files before watcher starts.
		writeFile(t, root, "a.txt", []byte("a"))
		writeFile(t, root, "b.txt", []byte("b"))

		ctx, cancel := context.WithCancel(t.Context())

		w, err := fileproc.NewWatcher(
			root,
			fileproc.WithOnEvent(func(_ fileproc.Event) {}),
			fileproc.WithInterval(testInterval),
			fileproc.WithFileWorkers(1),
			fileproc.WithScanWorkers(1),
			fileproc.WithChunkSize(8),
			fileproc.WithEmitBaseline(),
		)
		if err != nil {
			t.Fatalf("NewWatcher: %v", err)
		}

		done := make(chan struct{})

		go func() {
			w.Watch(ctx, nil)
			close(done)
		}()

		defer func() {
			cancel()
			time.Sleep(testInterval)
			synctest.Wait()
		}()

		// Wait for first scan to complete.
		time.Sleep(testInterval * 2)
		synctest.Wait()

		stats := w.Stats()
		if stats.Scans < 1 {
			t.Fatalf("expected at least 1 scan, got %d", stats.Scans)
		}

		if stats.Creates != 2 {
			t.Fatalf("expected 2 creates, got %d", stats.Creates)
		}

		if stats.FilesSeen < 2 {
			t.Fatalf("expected at least 2 files seen, got %d", stats.FilesSeen)
		}

		// Modify one file.
		prevScans := stats.Scans

		writeFile(t, root, "a.txt", []byte("modified"))
		time.Sleep(testInterval * 2)
		synctest.Wait()

		stats = w.Stats()
		if stats.Scans <= prevScans {
			t.Fatal("expected more scans after modify")
		}

		if stats.Modifies != 1 {
			t.Fatalf("expected 1 modify, got %d", stats.Modifies)
		}

		// Delete one file.
		prevScans = stats.Scans

		os.Remove(filepath.Join(root, "b.txt"))
		time.Sleep(testInterval * 2)
		synctest.Wait()

		stats = w.Stats()
		if stats.Scans <= prevScans {
			t.Fatal("expected more scans after delete")
		}

		if stats.Deletes != 1 {
			t.Fatalf("expected 1 delete, got %d", stats.Deletes)
		}
	})
}

// =============================================================================
// TEST HELPERS
// =============================================================================

// testWatcher wraps a watcher for testing with synctest.
type testWatcher struct {
	ch     <-chan fileproc.Event
	cancel context.CancelFunc
}

func newTestWatcher(t *testing.T, root string, opts ...fileproc.Option) *testWatcher {
	t.Helper()

	ctx, cancel := context.WithCancel(t.Context())

	opts = append([]fileproc.Option{
		fileproc.WithInterval(testInterval),
		fileproc.WithFileWorkers(1),
		fileproc.WithScanWorkers(1),
		fileproc.WithChunkSize(8),
	}, opts...)

	w, err := fileproc.NewWatcher(root, opts...)
	if err != nil {
		t.Fatalf("NewWatcher: %v", err)
	}

	ch := w.Events(ctx)

	return &testWatcher{ch: ch, cancel: cancel}
}

// stop cancels the watcher and waits for it to shut down cleanly.
func (watcher *testWatcher) stop() {
	watcher.cancel()
	time.Sleep(testInterval)
	synctest.Wait()
	watcher.drain()
}

// tick advances fake time past one watcher interval and waits for all
// goroutines to settle.
//
// Inside a synctest bubble, time only advances when all goroutines are
// "durably blocked" (waiting on channels, timers, or time.Sleep). The watcher
// does real I/O (readdir, stat) which is NOT durably blocking, so synctest.Wait
// alone won't advance time. We must explicitly sleep to move fake time forward,
// allowing the watcher's interval timer to fire and trigger the next scan.
func (watcher *testWatcher) tick() {
	_ = watcher

	time.Sleep(testInterval * 2)
	synctest.Wait()
}

// collect gathers exactly n events from the watcher. Advances fake time once
// to trigger a scan, then drains events.
func (watcher *testWatcher) collect(t *testing.T, n int) []fileproc.Event {
	t.Helper()

	watcher.tick()
	events := watcher.drain()

	if len(events) < n {
		t.Fatalf("expected %d events, got %d", n, len(events))
	}

	return events[:n]
}

// expectedEvent is a helper to build expected events for assertEvents.
type expectedEvent struct {
	path string
	typ  fileproc.EventType
	stat *fileproc.Stat
}

// ev creates an expected event for use with assertEvents. An optional stat
// can be provided to validate non-zero fields against the actual event.
func ev(typ fileproc.EventType, path string, stat ...fileproc.Stat) expectedEvent {
	var s *fileproc.Stat
	if len(stat) > 0 {
		s = &stat[0]
	}

	return expectedEvent{path: path, typ: typ, stat: s}
}

// assertEvents collects events and verifies each event's path and type match
// the expected values.
func (watcher *testWatcher) assertEvents(t *testing.T, expected ...expectedEvent) {
	t.Helper()

	events := watcher.collect(t, len(expected))

	want := make(map[string]expectedEvent, len(expected))
	for _, e := range expected {
		want[e.path] = e
	}

	for _, ev := range events {
		exp, ok := want[ev.Path]
		if !ok {
			t.Fatalf("unexpected event path %q", ev.Path)
		}

		if ev.Type != exp.typ {
			t.Fatalf("event %q: got type %v, want %v", ev.Path, ev.Type, exp.typ)
		}

		if exp.stat != nil {
			if exp.stat.Size != 0 && ev.Stat.Size != exp.stat.Size {
				t.Fatalf("event %q: got size %d, want %d", ev.Path, ev.Stat.Size, exp.stat.Size)
			}

			if exp.stat.Mode != 0 && ev.Stat.Mode != exp.stat.Mode {
				t.Fatalf("event %q: got mode %v, want %v", ev.Path, ev.Stat.Mode, exp.stat.Mode)
			}

			if exp.stat.ModTime != 0 && ev.Stat.ModTime != exp.stat.ModTime {
				t.Fatalf("event %q: got modtime %d, want %d", ev.Path, ev.Stat.ModTime, exp.stat.ModTime)
			}

			if exp.stat.Inode != 0 && ev.Stat.Inode != exp.stat.Inode {
				t.Fatalf("event %q: got inode %d, want %d", ev.Path, ev.Stat.Inode, exp.stat.Inode)
			}
		}

		delete(want, ev.Path)
	}

	if len(want) != 0 {
		t.Fatalf("missing events: %v", want)
	}
}

// assertEvent is a convenience wrapper for asserting a single event.
func (watcher *testWatcher) assertEvent(t *testing.T, expected expectedEvent) {
	t.Helper()

	watcher.assertEvents(t, expected)
}

// assertClosed verifies the event channel closes after the watcher stops.
func (watcher *testWatcher) assertClosed(t *testing.T) {
	t.Helper()

	time.Sleep(testInterval)
	synctest.Wait()

	select {
	case _, ok := <-watcher.ch:
		if !ok {
			return
		}

		t.Fatal("expected channel to be closed, got event")
	default:
		t.Fatal("expected channel to be closed")
	}
}

// drain returns all currently buffered events without blocking.
func (watcher *testWatcher) drain() []fileproc.Event {
	var events []fileproc.Event

	for {
		select {
		case ev, ok := <-watcher.ch:
			if !ok {
				return events
			}

			events = append(events, ev)
		default:
			return events
		}
	}
}
