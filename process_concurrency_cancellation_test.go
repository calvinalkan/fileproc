package fileproc_test

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"github.com/calvinalkan/fileproc"
)

func Test_Concurrency_Multiple_Workers_Have_Independent_Arenas_When_Parallel(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	// Create enough files to ensure multiple workers are used
	writeFlatFiles(t, root, 100, func(i int) string {
		return filepath.Join("sub", "file"+string(rune('0'+i%10))+string(rune('0'+i/10))+".txt")
	}, func(int) []byte { return []byte("content") })

	opts := []fileproc.Option{
		fileproc.WithRecursive(),
		fileproc.WithFileWorkers(4),
	}

	type holder struct {
		data []byte
	}

	results, errs := fileproc.Process(t.Context(), root, func(f *fileproc.File, w *fileproc.FileWorker) (*holder, error) {
		data, err := f.ReadAll()
		if err != nil {
			return nil, fmt.Errorf("test: %w", err)
		}

		return &holder{data: retainOwnedCopy(w, data)}, nil
	}, opts...)

	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}

	if len(results) != 100 {
		t.Fatalf("expected 100 results, got %d", len(results))
	}

	// Verify all data is correct
	for _, r := range results {
		if string(r.data) != "content" {
			t.Fatalf("data mismatch: got %q", r.data)
		}
	}
}

func Test_Concurrency_Multiple_Workers_Have_Independent_WorkerBuf_When_Parallel(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeFlatFiles(t, root, 50, func(i int) string {
		return "file" + string(rune('0'+i%10)) + string(rune('0'+i/10)) + ".txt"
	}, func(int) []byte { return []byte("content") })

	opts := []fileproc.Option{
		fileproc.WithFileWorkers(4),
	}

	type scratchResult struct {
		data []byte
	}

	results, errs := fileproc.Process(t.Context(), root, func(f *fileproc.File, scratch *fileproc.FileWorker) (*scratchResult, error) {
		// Write file path into scratch buffer
		buf := scratch.AllocateScratch(256)
		buf = append(buf, f.AbsPath()...)

		// Copy to result (scratch only valid during callback)
		return &scratchResult{data: append([]byte(nil), buf...)}, nil
	}, opts...)

	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}

	if len(results) != 50 {
		t.Fatalf("expected 50 results, got %d", len(results))
	}

	// Verify each result has correct path (scratch wasn't corrupted by other workers)
	seen := make(map[string]bool)

	for _, r := range results {
		path := string(r.data)
		if seen[path] {
			t.Fatalf("duplicate path: %s", path)
		}

		seen[path] = true
	}
}

func Test_Process_Invokes_Callbacks_Concurrently_When_Using_Multiple_Workers(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeFlatFiles(t, root, 8, func(i int) string {
		return fmt.Sprintf("c-%02d.txt", i)
	}, nil)

	var (
		inFlight    atomic.Int64
		maxInFlight atomic.Int64
		started     atomic.Int64
	)

	results, errs := fileproc.Process(t.Context(), root, func(_ *fileproc.File, _ *fileproc.FileWorker) (*struct{}, error) {
		cur := inFlight.Add(1)
		defer inFlight.Add(-1)

		for {
			prev := maxInFlight.Load()
			if cur <= prev || maxInFlight.CompareAndSwap(prev, cur) {
				break
			}
		}

		started.Add(1)

		const spinLimit = 1_000_000
		for i := 0; i < spinLimit && started.Load() < 2; i++ {
			runtime.Gosched()
		}

		return &struct{}{}, nil
	}, fileproc.WithFileWorkers(4), fileproc.WithChunkSize(1))

	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}

	if len(results) != 8 {
		t.Fatalf("expected 8 results, got %d", len(results))
	}

	if maxInFlight.Load() < 2 {
		t.Fatalf("expected concurrent callbacks, max in-flight=%d", maxInFlight.Load())
	}
}

func Test_Process_Does_Not_Invoke_Callbacks_Concurrently_When_Using_Single_Worker(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeFlatFiles(t, root, 8, func(i int) string {
		return fmt.Sprintf("s-%02d.txt", i)
	}, nil)

	var (
		inFlight    atomic.Int64
		maxInFlight atomic.Int64
	)

	results, errs := fileproc.Process(t.Context(), root, func(_ *fileproc.File, _ *fileproc.FileWorker) (*struct{}, error) {
		cur := inFlight.Add(1)
		defer inFlight.Add(-1)

		for {
			prev := maxInFlight.Load()
			if cur <= prev || maxInFlight.CompareAndSwap(prev, cur) {
				break
			}
		}

		spinGosched(100_000)

		return &struct{}{}, nil
	}, fileproc.WithFileWorkers(1), fileproc.WithChunkSize(1))

	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}

	if len(results) != 8 {
		t.Fatalf("expected 8 results, got %d", len(results))
	}

	if maxInFlight.Load() != 1 {
		t.Fatalf("expected no concurrent callbacks, max in-flight=%d", maxInFlight.Load())
	}
}

// ============================================================================
// Error handling tests
// ============================================================================

func Test_Process_Does_Not_Share_DataBuffers_When_Using_Concurrent_Workers(t *testing.T) {
	t.Parallel()

	t.Run("TreeWorkers", func(t *testing.T) {
		t.Parallel()

		root := t.TempDir()

		// Create many directories so all workers can get a job.
		const (
			workers = 4
			dirs    = 64
		)
		writeNestedFiles(t, root, dirs, 1, func(dir, _ int) string {
			return filepath.Join(fmt.Sprintf("d%03d", dir), "f.txt")
		}, nil)

		ptrCh := make(chan uintptr, workers*4)
		release := make(chan struct{})

		var ok struct{}

		opts := []fileproc.Option{
			fileproc.WithRecursive(),
			fileproc.WithFileWorkers(workers),
		}

		done := make(chan struct{})

		var errs []error

		go func() {
			_, errs = fileproc.Process(t.Context(), root, func(_ *fileproc.File, w *fileproc.FileWorker) (*struct{}, error) {
				buf := w.AllocateScratch(1)

				buf = buf[:cap(buf)]
				if len(buf) == 0 {
					return nil, errors.New("unexpected empty buffer")
				}

				ptr := uintptr(unsafe.Pointer(&buf[0]))

				select {
				case ptrCh <- ptr:
				default:
				}

				<-release

				return &ok, nil
			}, opts...)

			close(done)
		}()

		ptrs := collectPtrs(t, ptrCh, workers, 2*time.Second)

		close(release)

		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Fatal("Process did not return (possible deadlock)")
		}

		if len(errs) != 0 {
			t.Fatalf("unexpected errors: %v", errs)
		}

		if len(ptrs) != workers {
			t.Fatalf("expected %d distinct worker buffers across concurrent workers, got %d", workers, len(ptrs))
		}
	})

	t.Run("FileWorkers", func(t *testing.T) {
		t.Parallel()

		root := t.TempDir()

		const workers = 4

		pad := strings.Repeat("x", testNamePad)

		writeFlatFiles(t, root, testNumFilesBig, func(i int) string {
			return fmt.Sprintf("f-%04d-%s.txt", i, pad)
		}, nil)

		ptrCh := make(chan uintptr, workers*8)
		release := make(chan struct{})

		var ok struct{}

		opts := []fileproc.Option{
			fileproc.WithFileWorkers(workers),
		}

		done := make(chan struct{})

		var (
			results []*struct{}
			errs    []error
		)

		go func() {
			results, errs = fileproc.Process(t.Context(), root, func(_ *fileproc.File, w *fileproc.FileWorker) (*struct{}, error) {
				buf := w.AllocateScratch(1)

				buf = buf[:cap(buf)]
				if len(buf) == 0 {
					return nil, errors.New("unexpected empty buffer")
				}

				ptr := uintptr(unsafe.Pointer(&buf[0]))

				select {
				case ptrCh <- ptr:
				default:
				}

				<-release

				return &ok, nil
			}, opts...)

			close(done)
		}()

		ptrs := collectPtrs(t, ptrCh, workers, 2*time.Second)

		close(release)

		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("Process did not return (possible deadlock)")
		}

		if len(errs) != 0 {
			t.Fatalf("unexpected errors: %v", errs)
		}

		if len(results) == 0 {
			t.Fatal("expected some results")
		}

		if len(ptrs) != workers {
			t.Fatalf("expected %d distinct data buffers across concurrent workers, got %d", workers, len(ptrs))
		}
	})
}

func Test_Process_Stops_Early_When_Context_Canceled(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeFile(t, root, "one.txt", []byte("one"))
	writeFile(t, root, "two.txt", []byte("two"))
	writeFile(t, root, "three.txt", []byte("three"))

	ctx, cancel := context.WithCancelCause(t.Context())
	t.Cleanup(func() { cancel(nil) })

	stopErr := errors.New("stop")
	count := 0

	results, errs := fileproc.Process(ctx, root, func(_ *fileproc.File, _ *fileproc.FileWorker) (*struct{}, error) {
		count++
		if count == 1 {
			cancel(stopErr)
		}

		return &struct{}{}, nil
	}, fileproc.WithFileWorkers(1))

	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}

	if context.Cause(ctx) == nil || !errors.Is(context.Cause(ctx), stopErr) {
		t.Fatalf("expected cancellation cause to be %v, got %v", stopErr, context.Cause(ctx))
	}

	if len(results) >= 3 {
		t.Fatalf("expected early stop, got %d results", len(results))
	}
}

func Test_Process_Does_Not_Hang_When_Cancelled_With_Blocked_Callback(t *testing.T) {
	t.Parallel()

	root := t.TempDir()

	pad := strings.Repeat("x", testNamePad)

	writeFlatFiles(t, root, testNumFilesMed, func(i int) string {
		return fmt.Sprintf("f-%04d-%s.txt", i, pad)
	}, nil)

	ctx, cancel := context.WithCancelCause(t.Context())
	t.Cleanup(func() { cancel(nil) })

	stopErr := errors.New("stop")

	started := make(chan struct{}, 1)
	release := make(chan struct{})

	var ok struct{}

	done := make(chan struct{})

	var (
		results []*struct{}
		errs    []error
	)

	opts := []fileproc.Option{
		fileproc.WithFileWorkers(1),
	}

	go func() {
		results, errs = fileproc.Process(ctx, root, func(_ *fileproc.File, _ *fileproc.FileWorker) (*struct{}, error) {
			select {
			case started <- struct{}{}:
			default:
			}

			<-release

			return &ok, nil
		}, opts...)

		close(done)
	}()

	// Wait for at least one callback to start (worker is now blocked).
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		close(release)
		t.Fatal("timeout waiting for callback to start")
	}

	spinGosched(100_000)

	cancel(stopErr)
	close(release)

	select {
	case <-done:
		// ok
	case <-time.After(3 * time.Second):
		t.Fatal("Process did not return after cancellation (possible deadlock)")
	}

	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}

	if !errors.Is(context.Cause(ctx), stopErr) {
		t.Fatalf("unexpected cancellation cause: %v", context.Cause(ctx))
	}

	if len(results) == 0 {
		t.Fatal("expected at least 1 result before cancellation")
	}

	if len(results) >= testNumFilesMed {
		t.Fatalf("expected early stop, got %d results", len(results))
	}
}

func Test_Process_Does_Not_Hang_When_Cancelled_In_Recursive_Concurrent_Mode(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeNestedFiles(t, root, testNumDirs, 1, func(dir, _ int) string {
		return filepath.Join(fmt.Sprintf("d%03d", dir), "f.txt")
	}, nil)

	ctx, cancel := context.WithCancelCause(t.Context())
	t.Cleanup(func() { cancel(nil) })

	stopErr := errors.New("stop")

	var started atomic.Int64

	release := make(chan struct{})

	var ok struct{}

	done := make(chan struct{})

	var (
		results []*struct{}
		errs    []error
	)

	workers := 4
	opts := []fileproc.Option{
		fileproc.WithRecursive(),
		fileproc.WithFileWorkers(workers),
	}

	go func() {
		results, errs = fileproc.Process(ctx, root, func(_ *fileproc.File, _ *fileproc.FileWorker) (*struct{}, error) {
			started.Add(1)
			<-release

			return &ok, nil
		}, opts...)

		close(done)
	}()

	// Wait for all workers to be blocked in the callback.
	if !waitForCount(2*time.Second, int64(workers), started.Load) {
		close(release)
		t.Fatalf("expected at least %d concurrent callbacks to start; got %d", workers, started.Load())
	}

	cancel(stopErr)
	close(release)

	select {
	case <-done:
		// ok
	case <-time.After(3 * time.Second):
		t.Fatal("Process did not return after recursive cancellation (possible deadlock)")
	}

	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}

	if !errors.Is(context.Cause(ctx), stopErr) {
		t.Fatalf("unexpected cancellation cause: %v", context.Cause(ctx))
	}

	if len(results) == 0 {
		t.Fatal("expected at least 1 result before cancellation")
	}

	if len(results) >= testNumDirs {
		t.Fatalf("expected early stop, got %d results", len(results))
	}
}

func Test_Process_Does_Not_Hang_When_Cancelled_In_Recursive_Large_Directory(t *testing.T) {
	t.Parallel()

	root := t.TempDir()

	pad := strings.Repeat("x", testNamePad)

	writeFlatFiles(t, root, testNumFilesMed, func(i int) string {
		return filepath.Join("big", fmt.Sprintf("f-%04d-%s.txt", i, pad))
	}, nil)

	ctx, cancel := context.WithCancelCause(t.Context())
	t.Cleanup(func() { cancel(nil) })

	stopErr := errors.New("stop")

	started := make(chan struct{}, 1)
	release := make(chan struct{})

	var ok struct{}

	done := make(chan struct{})

	var (
		results []*struct{}
		errs    []error
	)

	opts := []fileproc.Option{
		fileproc.WithRecursive(),
		fileproc.WithFileWorkers(2),
	}

	go func() {
		results, errs = fileproc.Process(ctx, root, func(_ *fileproc.File, _ *fileproc.FileWorker) (*struct{}, error) {
			select {
			case started <- struct{}{}:
			default:
			}

			<-release

			return &ok, nil
		}, opts...)

		close(done)
	}()

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		close(release)
		t.Fatal("timeout waiting for callback to start")
	}

	spinGosched(100_000)

	cancel(stopErr)
	close(release)

	select {
	case <-done:
		// ok
	case <-time.After(3 * time.Second):
		t.Fatal("Process did not return after cancellation (possible deadlock in recursive large-dir processing)")
	}

	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}

	if !errors.Is(context.Cause(ctx), stopErr) {
		t.Fatalf("unexpected cancellation cause: %v", context.Cause(ctx))
	}

	if len(results) == 0 {
		t.Fatal("expected at least 1 result before cancellation")
	}

	if len(results) >= testNumFilesMed {
		t.Fatalf("expected early stop, got %d results", len(results))
	}
}

func Test_Process_Does_Not_Hang_When_Cancelled_With_Pending_Dirs(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeNestedFiles(t, root, testNumDirs, 1, func(dir, _ int) string {
		return filepath.Join(fmt.Sprintf("d%03d", dir), "f.txt")
	}, func(int, int) []byte { return []byte("x") })

	ctx, cancel := context.WithCancelCause(t.Context())
	t.Cleanup(func() { cancel(nil) })

	stopErr := errors.New("stop")
	boom := errors.New("boom")

	var fired atomic.Int64

	opts := []fileproc.Option{
		fileproc.WithRecursive(),
		fileproc.WithFileWorkers(1),
		fileproc.WithScanWorkers(1),
		fileproc.WithChunkSize(1),
		fileproc.WithOnError(func(err error, _, _ int) bool {
			if errors.Is(err, boom) || errors.Is(err, stopErr) {
				cancel(stopErr)
			}

			return true
		}),
	}

	done := make(chan struct{})

	go func() {
		_, _ = fileproc.Process(ctx, root, func(_ *fileproc.File, _ *fileproc.FileWorker) (*struct{}, error) {
			if fired.CompareAndSwap(0, 1) {
				return nil, boom
			}

			return &struct{}{}, nil
		}, opts...)

		close(done)
	}()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Process did not return after cancellation (possible deadlock with pending dirs)")
	}

	if !errors.Is(context.Cause(ctx), stopErr) {
		t.Fatalf("unexpected cancellation cause: %v", context.Cause(ctx))
	}
}

func collectPtrs(t *testing.T, ch <-chan uintptr, want int, timeout time.Duration) map[uintptr]struct{} {
	t.Helper()

	ptrs := make(map[uintptr]struct{}, want)
	deadline := time.Now().Add(timeout)

	for len(ptrs) < want && time.Now().Before(deadline) {
		select {
		case p := <-ch:
			ptrs[p] = struct{}{}
		case <-time.After(5 * time.Millisecond):
		}
	}

	return ptrs
}

func spinGosched(iterations int) {
	for range iterations {
		runtime.Gosched()
	}
}

func waitForCount(timeout time.Duration, want int64, get func() int64) bool {
	deadline := time.Now().Add(timeout)

	for get() < want && time.Now().Before(deadline) {
		runtime.Gosched()
	}

	return get() >= want
}
