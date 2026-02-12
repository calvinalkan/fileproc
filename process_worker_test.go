package fileproc_test

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
	"unsafe"

	"github.com/calvinalkan/fileproc"
)

func Test_FileWorker_IDs_Are_Unique_Within_Run_When_Workers_Execute_Concurrently(t *testing.T) {
	t.Parallel()

	const (
		workers = 4
		files   = 256
	)

	root := t.TempDir()
	writeFlatFiles(t, root, files, func(i int) string {
		return fmt.Sprintf("f-%04d.txt", i)
	}, nil)

	var (
		mu            sync.Mutex
		seenIDs       = make(map[int]struct{}, workers)
		activeByID    = make(map[int]int, workers)
		workerPtrByID = make(map[int]string, workers)
	)

	allStarted := make(chan struct{})
	startDeadline := time.After(2 * time.Second)

	_, errs := fileproc.Process(
		t.Context(),
		root,
		func(_ *fileproc.File, w *fileproc.FileWorker) (*struct{}, error) {
			id := w.ID()
			if id < 0 || id >= workers {
				return nil, fmt.Errorf("worker id out of range: %d", id)
			}

			ptr := fmt.Sprintf("%p", w)

			mu.Lock()

			activeByID[id]++
			if activeByID[id] > 1 {
				mu.Unlock()

				return nil, fmt.Errorf("worker id %d used concurrently", id)
			}

			if prevPtr, ok := workerPtrByID[id]; ok && prevPtr != ptr {
				mu.Unlock()

				return nil, fmt.Errorf("worker id %d changed instance: %s -> %s", id, prevPtr, ptr)
			}

			workerPtrByID[id] = ptr

			if _, ok := seenIDs[id]; !ok {
				seenIDs[id] = struct{}{}
				if len(seenIDs) == workers {
					close(allStarted)
				}
			}

			mu.Unlock()

			select {
			case <-allStarted:
			case <-startDeadline:
				return nil, errors.New("timed out waiting for all worker IDs")
			}

			mu.Lock()

			activeByID[id]--

			mu.Unlock()

			return &struct{}{}, nil
		},
		fileproc.WithFileWorkers(workers),
		fileproc.WithChunkSize(1),
	)

	if len(errs) != 0 {
		t.Fatalf("expected no errors, got %v", errs)
	}

	mu.Lock()
	defer mu.Unlock()

	if len(seenIDs) != workers {
		t.Fatalf("expected %d unique worker IDs, got %d", workers, len(seenIDs))
	}
}

func Test_FileWorker_ID_Is_Reused_When_Process_Is_Called_In_Separate_Runs(t *testing.T) {
	t.Parallel()

	run := func() (int, int, []error) {
		root := t.TempDir()
		writeFile(t, root, "file.txt", []byte("x"))

		calls := 0
		id := -1

		_, errs := fileproc.Process(t.Context(), root, func(_ *fileproc.File, w *fileproc.FileWorker) (*struct{}, error) {
			calls++
			id = w.ID()

			return &struct{}{}, nil
		}, fileproc.WithFileWorkers(1))

		return id, calls, errs
	}

	firstID, firstCalls, firstErrs := run()
	if len(firstErrs) != 0 {
		t.Fatalf("first run returned errors: %v", firstErrs)
	}

	secondID, secondCalls, secondErrs := run()
	if len(secondErrs) != 0 {
		t.Fatalf("second run returned errors: %v", secondErrs)
	}

	if firstCalls == 0 || secondCalls == 0 {
		t.Fatalf("expected callbacks in both runs, got first=%d second=%d", firstCalls, secondCalls)
	}

	if firstID != 0 || secondID != 0 {
		t.Fatalf("expected worker id 0 in both runs, got first=%d second=%d", firstID, secondID)
	}
}

func Test_FileWorker_RetainLease_Returns_Empty_Buffer_When_Size_Is_NonPositive(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeFile(t, root, "f.txt", []byte("x"))

	_, errs := fileproc.Process(t.Context(), root, func(_ *fileproc.File, w *fileproc.FileWorker) (*struct{}, error) {
		negative := w.RetainLease(-64)
		if len(negative.Buf) != 0 {
			t.Fatalf("expected len=0 for negative request, got %d", len(negative.Buf))
		}

		err := negative.Release()
		if err != nil {
			t.Fatalf("negative lease release: %v", err)
		}

		zero := w.RetainLease(0)
		if len(zero.Buf) != 0 {
			t.Fatalf("expected len=0 for zero request, got %d", len(zero.Buf))
		}

		err = zero.Release()
		if err != nil {
			t.Fatalf("zero lease release: %v", err)
		}

		return &struct{}{}, nil
	}, fileproc.WithFileWorkers(1))

	if len(errs) != 0 {
		t.Fatalf("expected no errors, got %v", errs)
	}
}

func Test_FileWorker_RetainLease_Uses_Distinct_Buffers_When_Multiple_Leases_Are_Active(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeFile(t, root, "f.txt", []byte("x"))

	_, errs := fileproc.Process(t.Context(), root, func(_ *fileproc.File, w *fileproc.FileWorker) (*struct{}, error) {
		first := w.RetainLease(32)
		second := w.RetainLease(32)

		// Both leases are still active, so memory must not alias.
		if sliceDataPtr(first.Buf) == sliceDataPtr(second.Buf) {
			t.Fatal("active leases unexpectedly share the same backing buffer")
		}

		err := first.Release()
		if err != nil {
			t.Fatalf("first release: %v", err)
		}

		err = second.Release()
		if err != nil {
			t.Fatalf("second release: %v", err)
		}

		return &struct{}{}, nil
	}, fileproc.WithFileWorkers(1))

	if len(errs) != 0 {
		t.Fatalf("expected no errors, got %v", errs)
	}
}

func Test_FileWorker_RetainLease_Reuses_Buffer_When_Previous_Lease_Is_Released(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeFile(t, root, "f.txt", []byte("x"))

	_, errs := fileproc.Process(t.Context(), root, func(_ *fileproc.File, w *fileproc.FileWorker) (*struct{}, error) {
		first := w.RetainLease(64)

		firstPtr := sliceDataPtr(first.Buf)
		if firstPtr == 0 {
			t.Fatal("expected non-empty retained lease buffer")
		}

		err := first.Release()
		if err != nil {
			t.Fatalf("first release: %v", err)
		}

		// Releasing returns memory to the worker-local pool for immediate reuse.
		second := w.RetainLease(16)

		secondPtr := sliceDataPtr(second.Buf)
		if secondPtr != firstPtr {
			t.Fatalf("expected buffer reuse after release: first=%#x second=%#x", firstPtr, secondPtr)
		}

		err = second.Release()
		if err != nil {
			t.Fatalf("second release: %v", err)
		}

		return &struct{}{}, nil
	}, fileproc.WithFileWorkers(1))

	if len(errs) != 0 {
		t.Fatalf("expected no errors, got %v", errs)
	}
}

func Test_FileWorker_Lease_Release_Is_Safe_When_Called_From_Other_Goroutine(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeFile(t, root, "f.txt", []byte("x"))

	_, errs := fileproc.Process(t.Context(), root, func(_ *fileproc.File, w *fileproc.FileWorker) (*struct{}, error) {
		lease := w.RetainLease(48)
		leasePtr := sliceDataPtr(lease.Buf)

		releaseDone := make(chan error, 1)

		go func(l fileproc.Lease) {
			releaseDone <- l.Release()
		}(lease)

		err := <-releaseDone
		if err != nil {
			t.Fatalf("cross-goroutine release: %v", err)
		}

		reused := w.RetainLease(48)
		if sliceDataPtr(reused.Buf) != leasePtr {
			t.Fatalf("expected released buffer reuse, got old=%#x new=%#x", leasePtr, sliceDataPtr(reused.Buf))
		}

		err = reused.Release()
		if err != nil {
			t.Fatalf("reused lease release: %v", err)
		}

		return &struct{}{}, nil
	}, fileproc.WithFileWorkers(1))

	if len(errs) != 0 {
		t.Fatalf("expected no errors, got %v", errs)
	}
}

func Test_FileWorker_Lease_Release_Is_Idempotent_When_Called_Concurrently(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeFile(t, root, "f.txt", []byte("x"))

	_, errs := fileproc.Process(t.Context(), root, func(_ *fileproc.File, w *fileproc.FileWorker) (*struct{}, error) {
		lease := w.RetainLease(24)
		leasePtr := sliceDataPtr(lease.Buf)

		const releasers = 32

		errsCh := make(chan error, releasers)

		var wg sync.WaitGroup
		wg.Add(releasers)

		for range releasers {
			go func(l fileproc.Lease) {
				defer wg.Done()

				errsCh <- l.Release()
			}(lease)
		}

		wg.Wait()
		close(errsCh)

		for releaseErr := range errsCh {
			if releaseErr != nil {
				t.Fatalf("idempotent release failed: %v", releaseErr)
			}
		}

		reused := w.RetainLease(24)
		if sliceDataPtr(reused.Buf) != leasePtr {
			t.Fatalf("expected released buffer reuse, got old=%#x new=%#x", leasePtr, sliceDataPtr(reused.Buf))
		}

		err := reused.Release()
		if err != nil {
			t.Fatalf("reused lease release: %v", err)
		}

		return &struct{}{}, nil
	}, fileproc.WithFileWorkers(1))

	if len(errs) != 0 {
		t.Fatalf("expected no errors, got %v", errs)
	}
}

func Test_FileWorker_Lease_Remains_Valid_When_Used_In_Later_Callback_Before_Release(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeFiles(t, root, map[string][]byte{
		"a.txt": []byte("a"),
		"b.txt": []byte("b"),
	})

	var (
		callbacks int
		held      fileproc.Lease
	)

	_, errs := fileproc.Process(t.Context(), root, func(_ *fileproc.File, w *fileproc.FileWorker) (*struct{}, error) {
		callbacks++

		if callbacks == 1 {
			held = w.RetainLease(7)
			copy(held.Buf, "persist")

			return &struct{}{}, nil
		}

		// Lease must stay valid after the callback where it was created.
		if string(held.Buf) != "persist" {
			t.Fatalf("unexpected held lease payload: %q", string(held.Buf))
		}

		err := held.Release()
		if err != nil {
			t.Fatalf("held lease release: %v", err)
		}

		return &struct{}{}, nil
	}, fileproc.WithFileWorkers(1), fileproc.WithChunkSize(1))

	if len(errs) != 0 {
		t.Fatalf("expected no errors, got %v", errs)
	}

	if callbacks != 2 {
		t.Fatalf("expected 2 callbacks, got %d", callbacks)
	}
}

func Test_FileWorker_Lease_Is_AutoCleaned_When_Process_Returns(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeFile(t, root, "f.txt", []byte("x"))

	var leaked fileproc.Lease

	_, errs := fileproc.Process(t.Context(), root, func(_ *fileproc.File, w *fileproc.FileWorker) (*struct{}, error) {
		leaked = w.RetainLease(8)
		copy(leaked.Buf, "cleanup")

		return &struct{}{}, nil
	}, fileproc.WithFileWorkers(1))

	if len(errs) != 0 {
		t.Fatalf("expected no errors, got %v", errs)
	}

	// Process return must make outstanding leases releasable without crashes.
	err := leaked.Release()
	if err != nil {
		t.Fatalf("release after Process return: %v", err)
	}

	err = leaked.Release()
	if err != nil {
		t.Fatalf("second release after Process return: %v", err)
	}
}

func Test_Lease_Release_Returns_Error_When_Lease_Is_ZeroValue(t *testing.T) {
	t.Parallel()

	var lease fileproc.Lease

	err := lease.Release()
	if err == nil {
		t.Fatal("expected zero-value lease release to fail")
	}
}

func Benchmark_FileWorker_RetainLease_Release_When_Buffer_Is_Reused(b *testing.B) {
	b.ReportAllocs()

	runLeaseBenchmark(b, func(worker *fileproc.FileWorker) {
		const size = 4 * 1024

		b.ResetTimer()

		for i := range b.N {
			lease := worker.RetainLease(size)
			lease.Buf[0] = byte(i)

			err := lease.Release()
			if err != nil {
				b.Fatalf("release: %v", err)
			}
		}

		b.StopTimer()
	})
}

func Benchmark_FileWorker_RetainLease_Release_When_Buffer_Grows(b *testing.B) {
	b.ReportAllocs()

	runLeaseBenchmark(b, func(worker *fileproc.FileWorker) {
		b.ResetTimer()

		for i := range b.N {
			size := 64
			if i%2 == 1 {
				size = 64 * 1024
			}

			lease := worker.RetainLease(size)
			lease.Buf[0] = byte(i)

			err := lease.Release()
			if err != nil {
				b.Fatalf("release: %v", err)
			}
		}

		b.StopTimer()
	})
}

func Benchmark_FileWorker_Lease_Release_Remains_Idempotent_When_Called_Twice(b *testing.B) {
	b.ReportAllocs()

	runLeaseBenchmark(b, func(worker *fileproc.FileWorker) {
		const size = 1024

		b.ResetTimer()

		for i := range b.N {
			lease := worker.RetainLease(size)
			lease.Buf[0] = byte(i)

			err := lease.Release()
			if err != nil {
				b.Fatalf("first release: %v", err)
			}

			err = lease.Release()
			if err != nil {
				b.Fatalf("second release: %v", err)
			}
		}

		b.StopTimer()
	})
}

func runLeaseBenchmark(b *testing.B, benchFn func(*fileproc.FileWorker)) {
	b.Helper()

	root := prepareLeaseBenchmarkRoot(b)
	callbacks := 0

	_, errs := fileproc.Process(
		b.Context(),
		root,
		func(_ *fileproc.File, worker *fileproc.FileWorker) (*struct{}, error) {
			callbacks++
			if callbacks > 1 {
				return nil, errors.New("unexpected extra callback in benchmark")
			}

			benchFn(worker)

			return &struct{}{}, nil
		},
		fileproc.WithFileWorkers(1),
	)
	if len(errs) != 0 {
		b.Fatalf("expected no errors, got %v", errs)
	}

	if callbacks != 1 {
		b.Fatalf("expected exactly one callback, got %d", callbacks)
	}
}

func prepareLeaseBenchmarkRoot(b *testing.B) string {
	b.Helper()

	root := b.TempDir()
	path := filepath.Join(root, "bench.txt")

	err := os.WriteFile(path, []byte("x"), 0o600)
	if err != nil {
		b.Fatalf("write %s: %v", path, err)
	}

	return root
}

func sliceDataPtr(b []byte) uintptr {
	if len(b) == 0 {
		return 0
	}

	return uintptr(unsafe.Pointer(unsafe.SliceData(b)))
}
