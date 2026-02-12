package fileproc_test

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

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
