package fileproc_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"fileproc"
)

func Test_WithDefaults_Sets_Values_When_Defaults_Are_Missing(t *testing.T) {
	t.Parallel()

	opts := fileproc.WithDefaults(fileproc.Options{})
	if opts.ReadSize != fileproc.DefaultReadSize {
		t.Fatalf("ReadSize: got=%d want=%d", opts.ReadSize, fileproc.DefaultReadSize)
	}

	if opts.SmallFileThreshold != fileproc.DefaultSmallFileThreshold {
		t.Fatalf("SmallFileThreshold: got=%d want=%d", opts.SmallFileThreshold, fileproc.DefaultSmallFileThreshold)
	}

	opts2 := fileproc.WithDefaults(fileproc.Options{ReadSize: 123, SmallFileThreshold: 456})
	if opts2.ReadSize != 123 {
		t.Fatalf("ReadSize preserved: got=%d want=123", opts2.ReadSize)
	}

	if opts2.SmallFileThreshold != 456 {
		t.Fatalf("SmallFileThreshold preserved: got=%d want=456", opts2.SmallFileThreshold)
	}
}

func Test_DefaultWorkerCounts_Are_Clamped_When_Exceeding_Expected_Ranges(t *testing.T) {
	t.Parallel()

	w := fileproc.DefaultWorkers()
	if w < 4 {
		t.Fatalf("defaultWorkers too small: %d", w)
	}

	if w > fileproc.MaxWorkers {
		t.Fatalf("defaultWorkers exceeds maxWorkers: %d > %d", w, fileproc.MaxWorkers)
	}

	tw := fileproc.DefaultTreeWorkers()
	if tw < 4 || tw > 16 {
		t.Fatalf("defaultTreeWorkers out of range: %d", tw)
	}
}

func Test_Process_Clamps_Workers_When_Exceeding_MaxWorkers_In_Recursive_Mode(t *testing.T) {
	t.Parallel()

	// This test intentionally blocks callbacks to measure max in-flight concurrency.
	// It also serves as a regression test against spawning extreme numbers of workers.
	root := t.TempDir()

	// Create enough subdirectories that if clamping is broken, we'd exceed MaxWorkers
	// concurrent callbacks.
	numDirs := fileproc.MaxWorkers + 50
	for i := range numDirs {
		dir := filepath.Join(root, fmt.Sprintf("d%03d", i))

		err := os.MkdirAll(dir, 0o750)
		if err != nil {
			t.Fatalf("mkdir: %v", err)
		}

		err := os.WriteFile(filepath.Join(dir, "f.txt"), []byte("x"), 0o600)
		if err != nil {
			t.Fatalf("write: %v", err)
		}
	}

	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)

	release := make(chan struct{})

	var (
		inflight atomic.Int64
		maxSeen  atomic.Int64
		ok       struct{}
	)

	updateMax := func(v int64) {
		for {
			cur := maxSeen.Load()
			if v <= cur {
				return
			}

			if maxSeen.CompareAndSwap(cur, v) {
				return
			}
		}
	}

	opts := fileproc.Options{
		Recursive: true,
		Workers:   fileproc.MaxWorkers + 50,
		// Keep directories "small" so we don't mix in within-dir pipelining.
		SmallFileThreshold: 1_000_000,
	}

	done := make(chan struct{})

	go func() {
		_, _ = fileproc.Process(ctx, root, func(_ []byte, _ []byte) (*struct{}, error) {
			v := inflight.Add(1)
			updateMax(v)
			<-release
			inflight.Add(-1)

			return &ok, nil
		}, opts)

		close(done)
	}()

	// Wait until we hit the clamp level.
	deadline := time.Now().Add(10 * time.Second)
	for inflight.Load() < int64(fileproc.MaxWorkers) && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}

	// Let the processor finish.
	close(release)

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Process did not return (possible deadlock)")
	}

	if got := maxSeen.Load(); got > int64(fileproc.MaxWorkers) {
		t.Fatalf("expected max concurrent callbacks <= MaxWorkers (%d), got %d", fileproc.MaxWorkers, got)
	}

	if got := maxSeen.Load(); got < int64(fileproc.MaxWorkers) {
		// If this flakes on extremely low-core machines, it likely indicates that the
		// directory set is too small or scheduling is constrained.
		t.Fatalf("expected to observe at least MaxWorkers (%d) concurrent callbacks, got %d", fileproc.MaxWorkers, got)
	}
}
