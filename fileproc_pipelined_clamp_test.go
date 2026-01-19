package fileproc_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"fileproc"
)

func Test_Process_Clamps_Workers_When_Exceeding_MaxWorkers_In_Pipelined_Mode(t *testing.T) {
	t.Parallel()

	// We request far more workers than MaxWorkers (256) and verify we never
	// see more than MaxWorkers concurrent callbacks.
	//
	// With 200-char filenames, each 32KB getdents batch holds ~145 files.
	// 2000 files ≈ 14 batches. That's enough to observe concurrent workers
	// without needing to saturate all 256.

	root := t.TempDir()

	const (
		numFiles       = 2000
		requestWorkers = 1000 // way more than MaxWorkers (256)
	)

	pad := strings.Repeat("x", testNamePad)

	for i := range numFiles {
		name := fmt.Sprintf("f-%04d-%s.txt", i, pad)
		p := filepath.Join(root, name)

		err := os.WriteFile(p, []byte("x"), 0o600)
		if err != nil {
			t.Fatalf("write file %d: %v", i, err)
		}
	}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

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
		Workers:            requestWorkers,
		SmallFileThreshold: 1, // force pipelined mode
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

	// Wait a bit for workers to start and block.
	deadline := time.Now().Add(5 * time.Second)
	for inflight.Load() < 10 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}

	// Let it finish.
	close(release)

	select {
	case <-done:
		// ok
	case <-time.After(10 * time.Second):
		t.Fatal("Process did not return (possible deadlock)")
	}

	observed := maxSeen.Load()

	// Key assertion: we requested 1000 workers but should never exceed 256.
	if observed > int64(fileproc.MaxWorkers) {
		t.Fatalf("clamping failed: requested %d workers, observed %d concurrent (max should be %d)",
			requestWorkers, observed, fileproc.MaxWorkers)
	}

	// Sanity: we should see at least a few concurrent callbacks.
	if observed < 2 {
		t.Fatalf("expected multiple concurrent callbacks, got %d", observed)
	}
}
