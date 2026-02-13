package fileproc_test

import (
	"context"
	"errors"
	"fmt"
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

func Test_FileWorker_AllocateOwned_Returns_Empty_Buffer_When_Size_Is_NonPositive(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeFile(t, root, "f.txt", []byte("x"))

	_, errs := fileproc.Process(t.Context(), root, func(_ *fileproc.File, w *fileproc.FileWorker) (*struct{}, error) {
		negative := w.AllocateOwned(-64)
		if len(negative.Buf) != 0 {
			t.Fatalf("expected len=0 for negative request, got %d", len(negative.Buf))
		}

		if negative.Buf == nil {
			t.Fatal("expected non-nil buffer for negative request")
		}

		negative.Free()

		zero := w.AllocateOwned(0)
		if len(zero.Buf) != 0 {
			t.Fatalf("expected len=0 for zero request, got %d", len(zero.Buf))
		}

		if zero.Buf == nil {
			t.Fatal("expected non-nil buffer for zero request")
		}

		zero.Free()

		return &struct{}{}, nil
	}, fileproc.WithFileWorkers(1))

	if len(errs) != 0 {
		t.Fatalf("expected no errors, got %v", errs)
	}
}

func Test_FileWorker_AllocateOwned_Uses_Distinct_Buffers_When_Multiple_Owned_Are_Active(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeFile(t, root, "f.txt", []byte("x"))

	_, errs := fileproc.Process(t.Context(), root, func(_ *fileproc.File, w *fileproc.FileWorker) (*struct{}, error) {
		first := w.AllocateOwned(32)
		second := w.AllocateOwned(32)

		// Both owned buffers are still active, so memory must not alias.
		if sliceDataPtr(first.Buf) == sliceDataPtr(second.Buf) {
			t.Fatal("active owned buffers unexpectedly share the same backing buffer")
		}

		first.Free()

		second.Free()

		return &struct{}{}, nil
	}, fileproc.WithFileWorkers(1))

	if len(errs) != 0 {
		t.Fatalf("expected no errors, got %v", errs)
	}
}

func Test_FileWorker_AllocateOwned_Reuses_Buffer_When_Previous_Owned_Is_Freed(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeFile(t, root, "f.txt", []byte("x"))

	_, errs := fileproc.Process(t.Context(), root, func(_ *fileproc.File, w *fileproc.FileWorker) (*struct{}, error) {
		first := w.AllocateOwned(64)

		firstPtr := sliceDataPtr(first.Buf)
		if firstPtr == 0 {
			t.Fatal("expected non-empty retained owned buffer")
		}

		first.Free()

		// Releasing returns memory to the worker-local pool for immediate reuse.
		second := w.AllocateOwned(16)

		secondPtr := sliceDataPtr(second.Buf)
		if secondPtr != firstPtr {
			t.Fatalf("expected buffer reuse after release: first=%#x second=%#x", firstPtr, secondPtr)
		}

		second.Free()

		return &struct{}{}, nil
	}, fileproc.WithFileWorkers(1))

	if len(errs) != 0 {
		t.Fatalf("expected no errors, got %v", errs)
	}
}

func Test_FileWorker_AllocateOwned_Returns_Writable_Buffer_When_Size_Is_Positive(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeFile(t, root, "f.txt", []byte("x"))

	sizes := []int{1, 64, 4096}

	_, errs := fileproc.Process(t.Context(), root, func(_ *fileproc.File, w *fileproc.FileWorker) (*struct{}, error) {
		for _, size := range sizes {
			owned := w.AllocateOwned(size)
			if len(owned.Buf) != size {
				t.Fatalf("expected len=%d, got %d", size, len(owned.Buf))
			}

			owned.Buf[0] = 0x41
			owned.Buf[len(owned.Buf)-1] = 0x7a

			owned.Free()
		}

		return &struct{}{}, nil
	}, fileproc.WithFileWorkers(1))

	if len(errs) != 0 {
		t.Fatalf("expected no errors, got %v", errs)
	}
}

func Test_FileWorker_AllocateOwned_Grows_Buffer_When_Next_Request_Is_Larger(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeFile(t, root, "f.txt", []byte("x"))

	_, errs := fileproc.Process(t.Context(), root, func(_ *fileproc.File, w *fileproc.FileWorker) (*struct{}, error) {
		small := w.AllocateOwned(64)
		small.Free()

		large := w.AllocateOwned(64 * 1024)

		largePtr := sliceDataPtr(large.Buf)
		if len(large.Buf) != 64*1024 {
			t.Fatalf("expected large len=%d, got %d", 64*1024, len(large.Buf))
		}

		large.Buf[0] = 1

		large.Buf[len(large.Buf)-1] = 2
		large.Free()

		// After growth, later smaller owned buffers should reuse the grown buffer.
		reused := w.AllocateOwned(128)
		if sliceDataPtr(reused.Buf) != largePtr {
			t.Fatalf("expected grown buffer reuse, got old=%#x new=%#x", largePtr, sliceDataPtr(reused.Buf))
		}

		reused.Free()

		return &struct{}{}, nil
	}, fileproc.WithFileWorkers(1))

	if len(errs) != 0 {
		t.Fatalf("expected no errors, got %v", errs)
	}
}

func Test_FileWorker_Owned_Free_Is_Idempotent_When_Called_Twice_Sequentially(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeFile(t, root, "f.txt", []byte("x"))

	_, errs := fileproc.Process(t.Context(), root, func(_ *fileproc.File, w *fileproc.FileWorker) (*struct{}, error) {
		owned := w.AllocateOwned(512)
		ownedPtr := sliceDataPtr(owned.Buf)

		owned.Free()

		owned.Free()

		reused := w.AllocateOwned(256)
		if sliceDataPtr(reused.Buf) != ownedPtr {
			t.Fatalf("expected released buffer reuse, got old=%#x new=%#x", ownedPtr, sliceDataPtr(reused.Buf))
		}

		reused.Free()

		return &struct{}{}, nil
	}, fileproc.WithFileWorkers(1))

	if len(errs) != 0 {
		t.Fatalf("expected no errors, got %v", errs)
	}
}

func Test_FileWorker_Owned_Free_Is_Safe_When_Called_From_Other_Goroutine(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeFile(t, root, "f.txt", []byte("x"))

	_, errs := fileproc.Process(t.Context(), root, func(_ *fileproc.File, w *fileproc.FileWorker) (*struct{}, error) {
		owned := w.AllocateOwned(48)
		ownedPtr := sliceDataPtr(owned.Buf)

		releaseDone := make(chan struct{}, 1)

		go func(l fileproc.Owned) {
			l.Free()

			releaseDone <- struct{}{}
		}(owned)

		<-releaseDone

		reused := w.AllocateOwned(48)
		if sliceDataPtr(reused.Buf) != ownedPtr {
			t.Fatalf("expected released buffer reuse, got old=%#x new=%#x", ownedPtr, sliceDataPtr(reused.Buf))
		}

		reused.Free()

		return &struct{}{}, nil
	}, fileproc.WithFileWorkers(1))

	if len(errs) != 0 {
		t.Fatalf("expected no errors, got %v", errs)
	}
}

func Test_FileWorker_Owned_Free_Is_Idempotent_When_Called_Concurrently(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeFile(t, root, "f.txt", []byte("x"))

	_, errs := fileproc.Process(t.Context(), root, func(_ *fileproc.File, w *fileproc.FileWorker) (*struct{}, error) {
		owned := w.AllocateOwned(24)
		ownedPtr := sliceDataPtr(owned.Buf)

		const releasers = 32

		var wg sync.WaitGroup
		wg.Add(releasers)

		for range releasers {
			go func(l fileproc.Owned) {
				defer wg.Done()

				l.Free()
			}(owned)
		}

		wg.Wait()

		reused := w.AllocateOwned(24)
		if sliceDataPtr(reused.Buf) != ownedPtr {
			t.Fatalf("expected released buffer reuse, got old=%#x new=%#x", ownedPtr, sliceDataPtr(reused.Buf))
		}

		reused.Free()

		return &struct{}{}, nil
	}, fileproc.WithFileWorkers(1))

	if len(errs) != 0 {
		t.Fatalf("expected no errors, got %v", errs)
	}
}

func Test_FileWorker_Owned_Remains_Valid_When_Used_In_Later_Callback_Before_Free(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeFiles(t, root, map[string][]byte{
		"a.txt": []byte("a"),
		"b.txt": []byte("b"),
	})

	var (
		callbacks int
		held      fileproc.Owned
	)

	_, errs := fileproc.Process(t.Context(), root, func(_ *fileproc.File, w *fileproc.FileWorker) (*struct{}, error) {
		callbacks++

		if callbacks == 1 {
			held = w.AllocateOwned(7)
			copy(held.Buf, "persist")

			return &struct{}{}, nil
		}

		// Owned must stay valid after the callback where it was created.
		if string(held.Buf) != "persist" {
			t.Fatalf("unexpected held owned payload: %q", string(held.Buf))
		}

		held.Free()

		return &struct{}{}, nil
	}, fileproc.WithFileWorkers(1), fileproc.WithChunkSize(1))

	if len(errs) != 0 {
		t.Fatalf("expected no errors, got %v", errs)
	}

	if callbacks != 2 {
		t.Fatalf("expected 2 callbacks, got %d", callbacks)
	}
}

func Test_FileWorker_Owned_Is_AutoCleaned_When_Process_Returns(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeFile(t, root, "f.txt", []byte("x"))

	var leaked fileproc.Owned

	_, errs := fileproc.Process(t.Context(), root, func(_ *fileproc.File, w *fileproc.FileWorker) (*struct{}, error) {
		leaked = w.AllocateOwned(8)
		copy(leaked.Buf, "cleanup")

		return &struct{}{}, nil
	}, fileproc.WithFileWorkers(1))

	if len(errs) != 0 {
		t.Fatalf("expected no errors, got %v", errs)
	}

	// Process return must make outstanding owned buffers releasable without crashes.
	leaked.Free()
	leaked.Free()
}

func Test_Owned_Free_Is_NoOp_When_Owned_Is_ZeroValue(t *testing.T) {
	t.Parallel()

	var owned fileproc.Owned

	owned.Free()
	owned.Free()
}

func Test_FileWorker_Owned_Are_Isolated_When_Multiple_Workers_Allocate_Concurrently(t *testing.T) {
	t.Parallel()

	const (
		workers   = 4
		ownedSize = 128
	)

	root := t.TempDir()
	writeFlatFiles(t, root, testNumFilesBig, func(i int) string {
		return fmt.Sprintf("f-%04d.txt", i)
	}, nil)

	var (
		mu               sync.Mutex
		firstPtrByWorker = make(map[string]uintptr, workers)
		markerByWorker   = make(map[string]byte, workers)
		completedWorkers = make(map[string]struct{}, workers)
	)

	allRetained := make(chan struct{})

	waitCtx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()

	_, errs := fileproc.Process(
		t.Context(),
		root,
		func(_ *fileproc.File, w *fileproc.FileWorker) (*struct{}, error) {
			workerPtr := fmt.Sprintf("%p", w)

			mu.Lock()

			if _, done := completedWorkers[workerPtr]; done {
				mu.Unlock()

				return &struct{}{}, nil
			}

			owned := w.AllocateOwned(ownedSize)

			marker := byte(len(firstPtrByWorker) + 1)
			for i := range owned.Buf {
				owned.Buf[i] = marker
			}

			firstPtr := sliceDataPtr(owned.Buf)
			firstPtrByWorker[workerPtr] = firstPtr
			markerByWorker[workerPtr] = marker

			if len(firstPtrByWorker) == workers {
				close(allRetained)
			}

			mu.Unlock()

			select {
			case <-allRetained:
			case <-waitCtx.Done():
				return nil, errors.New("timed out waiting for concurrent owned retention")
			}

			mu.Lock()

			for otherWorker, otherPtr := range firstPtrByWorker {
				if otherWorker != workerPtr && otherPtr == firstPtr {
					mu.Unlock()

					return nil, fmt.Errorf("workers share active owned buffer: %s and %s", workerPtr, otherWorker)
				}
			}

			expectedMarker := markerByWorker[workerPtr]

			mu.Unlock()

			for i := range ownedSize {
				if owned.Buf[i] != expectedMarker {
					return nil, fmt.Errorf("owned data corrupted at %d: got=%d want=%d", i, owned.Buf[i], expectedMarker)
				}
			}

			owned.Free()

			mu.Lock()

			completedWorkers[workerPtr] = struct{}{}

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

	if len(firstPtrByWorker) != workers {
		t.Fatalf("expected %d workers to retain owned buffers, got %d", workers, len(firstPtrByWorker))
	}

	if len(completedWorkers) != workers {
		t.Fatalf("expected %d workers to complete owned validation, got %d", workers, len(completedWorkers))
	}
}

func Test_FileWorker_Owned_Reuses_Worker_Buffer_When_Workers_Free_Concurrently(t *testing.T) {
	t.Parallel()

	const (
		workers   = 4
		ownedSize = 320
	)

	root := t.TempDir()
	writeFlatFiles(t, root, testNumFilesBig, func(i int) string {
		return fmt.Sprintf("f-%04d.txt", i)
	}, nil)

	var (
		mu               sync.Mutex
		firstPtrByWorker = make(map[string]uintptr, workers)
		completedWorkers = make(map[string]struct{}, workers)
	)

	allRetained := make(chan struct{})

	waitCtx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()

	_, errs := fileproc.Process(
		t.Context(),
		root,
		func(_ *fileproc.File, w *fileproc.FileWorker) (*struct{}, error) {
			workerPtr := fmt.Sprintf("%p", w)

			mu.Lock()

			if _, done := completedWorkers[workerPtr]; done {
				mu.Unlock()

				return &struct{}{}, nil
			}

			mu.Unlock()

			first := w.AllocateOwned(ownedSize)
			firstPtr := sliceDataPtr(first.Buf)

			mu.Lock()

			firstPtrByWorker[workerPtr] = firstPtr
			if len(firstPtrByWorker) == workers {
				close(allRetained)
			}

			mu.Unlock()

			select {
			case <-allRetained:
			case <-waitCtx.Done():
				return nil, errors.New("timed out waiting for concurrent owned retention")
			}

			releaseDone := make(chan struct{}, 1)

			go func(l fileproc.Owned) {
				l.Free()

				releaseDone <- struct{}{}
			}(first)

			<-releaseDone

			second := w.AllocateOwned(ownedSize)

			secondPtr := sliceDataPtr(second.Buf)
			if secondPtr != firstPtr {
				return nil, fmt.Errorf("worker did not reuse its owned buffer: first=%#x second=%#x", firstPtr, secondPtr)
			}

			second.Free()

			mu.Lock()

			completedWorkers[workerPtr] = struct{}{}

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

	if len(firstPtrByWorker) != workers {
		t.Fatalf("expected %d workers to retain owned buffers, got %d", workers, len(firstPtrByWorker))
	}

	if len(completedWorkers) != workers {
		t.Fatalf("expected %d workers to complete reuse validation, got %d", workers, len(completedWorkers))
	}
}

// ============================================================================
// Worker.AllocateScratch tests
// ============================================================================

func Test_Worker_AllocateScratch_Returns_Zero_Len_With_Cap_When_Called(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeFile(t, root, "test.txt", []byte("content"))

	opts := []fileproc.Option{fileproc.WithFileWorkers(1)}

	var bufLen, bufCap int

	results, errs := fileproc.Process(t.Context(), root, func(_ *fileproc.File, scratch *fileproc.FileWorker) (*struct{}, error) {
		buf := scratch.AllocateScratch(4096)
		bufLen = len(buf)
		bufCap = cap(buf)

		return &struct{}{}, nil
	}, opts...)

	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}

	if bufLen != 0 {
		t.Fatalf("expected len=0, got %d", bufLen)
	}

	if bufCap < 4096 {
		t.Fatalf("expected cap>=4096, got %d", bufCap)
	}
}

func Test_Worker_AllocateScratch_Returns_Empty_Buffer_When_Size_Is_NonPositive(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeFile(t, root, "test.txt", []byte("content"))

	results, errs := fileproc.Process(t.Context(), root, func(_ *fileproc.File, w *fileproc.FileWorker) (*struct{}, error) {
		negative := w.AllocateScratch(-32)
		if len(negative) != 0 {
			t.Fatalf("expected len=0 for negative request, got %d", len(negative))
		}

		zero := w.AllocateScratch(0)
		if len(zero) != 0 {
			t.Fatalf("expected len=0 for zero request, got %d", len(zero))
		}

		return &struct{}{}, nil
	}, fileproc.WithFileWorkers(1))

	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
}

func Test_Worker_AllocateScratch_Grows_Capacity_When_Larger_Request(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeFile(t, root, "a.txt", []byte("a"))
	writeFile(t, root, "b.txt", []byte("b"))

	opts := []fileproc.Option{
		fileproc.WithFileWorkers(1),
	}

	var caps []int

	results, errs := fileproc.Process(t.Context(), root, func(_ *fileproc.File, scratch *fileproc.FileWorker) (*struct{}, error) {
		// First file requests 1024, second requests 8192
		size := 1024
		if len(caps) > 0 {
			size = 8192
		}

		buf := scratch.AllocateScratch(size)
		caps = append(caps, cap(buf))

		return &struct{}{}, nil
	}, opts...)

	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}

	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}

	if len(caps) != 2 {
		t.Fatalf("expected 2 capacity readings, got %d", len(caps))
	}

	if caps[0] < 1024 {
		t.Fatalf("first cap should be >= 1024, got %d", caps[0])
	}

	if caps[1] < 8192 {
		t.Fatalf("second cap should be >= 8192, got %d", caps[1])
	}
}

func Test_Worker_AllocateScratch_Reuses_Buffer_When_Same_Worker(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeFile(t, root, "a.txt", []byte("a"))
	writeFile(t, root, "b.txt", []byte("b"))

	opts := []fileproc.Option{
		fileproc.WithFileWorkers(1),
	}

	var ptrs []uintptr

	results, errs := fileproc.Process(t.Context(), root, func(_ *fileproc.File, scratch *fileproc.FileWorker) (*struct{}, error) {
		buf := scratch.AllocateScratch(1024)
		// Expand to get actual backing array pointer
		buf = buf[:cap(buf)]
		if len(buf) > 0 {
			ptrs = append(ptrs, uintptr(unsafe.Pointer(&buf[0])))
		}

		return &struct{}{}, nil
	}, opts...)

	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}

	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}

	if len(ptrs) != 2 {
		t.Fatalf("expected 2 pointers, got %d", len(ptrs))
	}

	// Same backing array should be reused
	if ptrs[0] != ptrs[1] {
		t.Fatal("worker buffer should be reused across files in same worker")
	}
}

func Test_Worker_AllocateScratch_Preserves_Previous_Content_When_Called_Multiple_Times_In_Callback(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeFile(t, root, "a.txt", []byte("x"))

	_, errs := fileproc.Process(t.Context(), root, func(_ *fileproc.File, w *fileproc.FileWorker) (*struct{}, error) {
		first := w.AllocateScratch(16)
		first = append(first, "alpha"...)

		second := w.AllocateScratch(16)
		second = append(second, "bravo"...)

		third := w.AllocateScratch(16)
		third = append(third, "charlie"...)

		if string(first) != "alpha" {
			t.Fatalf("first scratch content was overwritten: got %q", string(first))
		}

		if string(second) != "bravo" {
			t.Fatalf("second scratch content was overwritten: got %q", string(second))
		}

		if string(third) != "charlie" {
			t.Fatalf("unexpected third scratch content: got %q", string(third))
		}

		return &struct{}{}, nil
	}, fileproc.WithFileWorkers(1))

	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}
}

func Test_Worker_AllocateScratch_Does_Not_Clobber_ReadAll_Or_AbsPath_When_File_Methods_Run_First(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeFile(t, root, "x.txt", []byte("payload"))

	_, errs := fileproc.Process(t.Context(), root, func(f *fileproc.File, w *fileproc.FileWorker) (*struct{}, error) {
		pathBefore := string(f.AbsPath())

		data, err := f.ReadAll()
		if err != nil {
			return nil, fmt.Errorf("read all: %w", err)
		}

		scratch := w.AllocateScratch(16)
		scratch = append(scratch, "scratch"...)

		// Internal slots (path/read) must remain independent from public scratch.
		if string(data) != "payload" {
			t.Fatalf("read data was clobbered after AllocateScratch: got %q", string(data))
		}

		if string(scratch) != "scratch" {
			t.Fatalf("scratch data mismatch: got %q", string(scratch))
		}

		if string(f.AbsPath()) != pathBefore {
			t.Fatalf("abs path was clobbered after AllocateScratch: got %q want %q", string(f.AbsPath()), pathBefore)
		}

		return &struct{}{}, nil
	}, fileproc.WithFileWorkers(1))

	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}
}

func Test_Worker_AllocateScratch_Does_Not_Clobber_ReadAll_Or_AbsPath_When_Scratch_Runs_First(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeFile(t, root, "x.txt", []byte("payload"))

	_, errs := fileproc.Process(t.Context(), root, func(f *fileproc.File, w *fileproc.FileWorker) (*struct{}, error) {
		scratch := w.AllocateScratch(16)
		scratch = append(scratch, "scratch"...)

		path := string(f.AbsPath())

		data, err := f.ReadAll()
		if err != nil {
			return nil, fmt.Errorf("read all: %w", err)
		}

		// Public scratch must remain independent from internal path/read slots.
		if string(scratch) != "scratch" {
			t.Fatalf("scratch was clobbered by file methods: got %q", string(scratch))
		}

		if string(data) != "payload" {
			t.Fatalf("read data mismatch: got %q", string(data))
		}

		if path == "" {
			t.Fatal("expected non-empty abs path")
		}

		if filepath.Base(path) != "x.txt" {
			t.Fatalf("unexpected abs path base: got %q", filepath.Base(path))
		}

		return &struct{}{}, nil
	}, fileproc.WithFileWorkers(1))

	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}
}

func Test_Worker_AllocateScratch_Are_Isolated_When_Multiple_Workers_Allocate_Concurrently(t *testing.T) {
	t.Parallel()

	const (
		workers     = 4
		scratchSize = 4 << 6
	)

	root := t.TempDir()
	writeFlatFiles(t, root, testNumFilesBig, func(i int) string {
		return fmt.Sprintf("f-%04d.txt", i)
	}, nil)

	var (
		mu               sync.Mutex
		firstPtrByWorker = make(map[string]uintptr, workers)
		markerByWorker   = make(map[string]byte, workers)
		completedWorkers = make(map[string]struct{}, workers)
	)

	allRetained := make(chan struct{})

	waitCtx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()

	_, errs := fileproc.Process(
		t.Context(),
		root,
		func(_ *fileproc.File, w *fileproc.FileWorker) (*struct{}, error) {
			workerPtr := fmt.Sprintf("%p", w)

			mu.Lock()

			if _, done := completedWorkers[workerPtr]; done {
				mu.Unlock()

				return &struct{}{}, nil
			}

			marker, ok := markerByWorker[workerPtr]
			if !ok {
				marker = byte(len(firstPtrByWorker) + 1)
				markerByWorker[workerPtr] = marker
			}

			mu.Unlock()

			scratch := w.AllocateScratch(scratchSize)
			scratch = scratch[:scratchSize]

			for i := range scratchSize {
				scratch[i] = marker
			}

			firstPtr := sliceDataPtr(scratch)

			mu.Lock()

			firstPtrByWorker[workerPtr] = firstPtr

			if len(firstPtrByWorker) == workers {
				close(allRetained)
			}

			mu.Unlock()

			select {
			case <-allRetained:
			case <-waitCtx.Done():
				return nil, errors.New("timed out waiting for concurrent scratch retention")
			}

			mu.Lock()

			for otherWorker, otherPtr := range firstPtrByWorker {
				if otherWorker != workerPtr && otherPtr == firstPtr {
					mu.Unlock()

					return nil, fmt.Errorf("workers share active scratch buffer: %s and %s", workerPtr, otherWorker)
				}
			}

			expectedMarker := markerByWorker[workerPtr]

			mu.Unlock()

			for i := range scratchSize {
				if scratch[i] != expectedMarker {
					return nil, fmt.Errorf("scratch data corrupted at %d: got=%d want=%d", i, scratch[i], expectedMarker)
				}
			}

			mu.Lock()

			completedWorkers[workerPtr] = struct{}{}

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

	if len(firstPtrByWorker) != workers {
		t.Fatalf("expected %d workers to retain scratch buffers, got %d", workers, len(firstPtrByWorker))
	}

	if len(completedWorkers) != workers {
		t.Fatalf("expected %d workers to complete scratch validation, got %d", workers, len(completedWorkers))
	}
}

// ============================================================================
// Owned copy tests
// ============================================================================

func Test_Worker_AllocateOwnedCopy_Returns_Stable_Slice_When_Called(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeFile(t, root, "test.txt", []byte("content"))

	opts := []fileproc.Option{fileproc.WithFileWorkers(1)}

	type holder struct {
		retained []byte
	}

	results, errs := fileproc.Process(t.Context(), root, func(f *fileproc.File, w *fileproc.FileWorker) (*holder, error) {
		data, err := f.ReadAll()
		if err != nil {
			return nil, fmt.Errorf("read all: %w", err)
		}

		return &holder{retained: retainOwnedCopy(w, data)}, nil
	}, opts...)

	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}

	if string(results[0].retained) != "content" {
		t.Fatalf("expected 'content', got %q", results[0].retained)
	}
}

func Test_Worker_AllocateOwnedCopy_Returns_Empty_Slice_When_Input_Empty(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeFile(t, root, "empty.txt", nil)

	opts := []fileproc.Option{fileproc.WithFileWorkers(1)}

	var retained []byte

	results, errs := fileproc.Process(t.Context(), root, func(f *fileproc.File, w *fileproc.FileWorker) (*struct{}, error) {
		data, err := f.ReadAll()
		if err != nil {
			return nil, fmt.Errorf("read all: %w", err)
		}

		retained = retainOwnedCopy(w, data)

		return &struct{}{}, nil
	}, opts...)

	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}

	if retained == nil {
		t.Fatal("expected non-nil slice for empty input")
	}

	if len(retained) != 0 {
		t.Fatalf("expected empty slice, got len=%d", len(retained))
	}
}

func Test_Worker_AllocateOwnedCopy_Preserves_Multiple_Slices_When_Called_Repeatedly(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeFile(t, root, "test.txt", []byte("hello world"))

	opts := []fileproc.Option{fileproc.WithFileWorkers(1)}

	type holder struct {
		first  []byte
		second []byte
	}

	results, errs := fileproc.Process(t.Context(), root, func(f *fileproc.File, w *fileproc.FileWorker) (*holder, error) {
		data, err := f.ReadAll()
		if err != nil {
			return nil, fmt.Errorf("read all: %w", err)
		}

		// Retain two different subslices
		first := retainOwnedCopy(w, data[:5])  // "hello"
		second := retainOwnedCopy(w, data[6:]) // "world"

		return &holder{first: first, second: second}, nil
	}, opts...)

	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}

	if string(results[0].first) != "hello" {
		t.Fatalf("first: expected 'hello', got %q", results[0].first)
	}

	if string(results[0].second) != "world" {
		t.Fatalf("second: expected 'world', got %q", results[0].second)
	}
}

func Test_Worker_AllocateOwnedCopy_Is_Independent_From_Original_When_Modified(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeFile(t, root, "test.txt", []byte("original"))

	opts := []fileproc.Option{fileproc.WithFileWorkers(1)}

	type holder struct {
		retained []byte
	}

	results, errs := fileproc.Process(t.Context(), root, func(f *fileproc.File, w *fileproc.FileWorker) (*holder, error) {
		data, err := f.ReadAll()
		if err != nil {
			return nil, fmt.Errorf("read all: %w", err)
		}

		retained := retainOwnedCopy(w, data)

		// Modify original - should not affect retained copy
		data[0] = 'X'

		return &holder{retained: retained}, nil
	}, opts...)

	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}

	if string(results[0].retained) != "original" {
		t.Fatalf("expected 'original', got %q (was modified)", results[0].retained)
	}
}

// ============================================================================
// Arena lifetime tests
// ============================================================================

func Test_Arena_Multiple_Files_Dont_Interfere_When_Using_Single_Worker(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeFile(t, root, "a.txt", []byte("alpha"))
	writeFile(t, root, "b.txt", []byte("bravo"))
	writeFile(t, root, "c.txt", []byte("charlie"))

	opts := []fileproc.Option{
		fileproc.WithFileWorkers(1),
	}

	type fileData struct {
		path string
		data []byte
	}

	results, errs := fileproc.Process(t.Context(), root, func(f *fileproc.File, w *fileproc.FileWorker) (*fileData, error) {
		data, err := f.ReadAll()
		if err != nil {
			return nil, fmt.Errorf("test: %w", err)
		}

		return &fileData{path: string(f.AbsPath()), data: retainOwnedCopy(w, data)}, nil
	}, opts...)

	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}

	if len(results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(results))
	}

	expected := map[string]string{
		filepath.Join(root, "a.txt"): "alpha",
		filepath.Join(root, "b.txt"): "bravo",
		filepath.Join(root, "c.txt"): "charlie",
	}

	for _, r := range results {
		want, ok := expected[r.path]
		if !ok {
			t.Fatalf("unexpected path: %s", r.path)
		}

		if string(r.data) != want {
			t.Fatalf("data mismatch for %s: got %q, want %q", r.path, r.data, want)
		}
	}
}

func Test_Arena_Subslices_Remain_Valid_When_Process_Returns(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	content := []byte("prefix:middle:suffix")
	writeFile(t, root, "test.txt", content)

	opts := []fileproc.Option{fileproc.WithFileWorkers(1)}

	type parts struct {
		prefix []byte
		middle []byte
		suffix []byte
	}

	results, errs := fileproc.Process(t.Context(), root, func(f *fileproc.File, w *fileproc.FileWorker) (*parts, error) {
		data, err := f.ReadAll()
		if err != nil {
			return nil, fmt.Errorf("test: %w", err)
		}
		// Create subslices
		return &parts{
			prefix: retainOwnedCopy(w, data[0:6]),   // "prefix"
			middle: retainOwnedCopy(w, data[7:13]),  // "middle"
			suffix: retainOwnedCopy(w, data[14:20]), // "suffix"
		}, nil
	}, opts...)

	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}

	// Subslices should still be valid
	if string(results[0].prefix) != "prefix" {
		t.Fatalf("prefix: got %q", results[0].prefix)
	}

	if string(results[0].middle) != "middle" {
		t.Fatalf("middle: got %q", results[0].middle)
	}

	if string(results[0].suffix) != "suffix" {
		t.Fatalf("suffix: got %q", results[0].suffix)
	}
}

func sliceDataPtr(b []byte) uintptr {
	if len(b) == 0 {
		return 0
	}

	return uintptr(unsafe.Pointer(unsafe.SliceData(b)))
}
