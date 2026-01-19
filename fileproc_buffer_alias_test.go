package fileproc_test

import (
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
	"unsafe"

	"fileproc"
)

func Test_Process_Does_Not_Share_DataBuffers_When_Using_Concurrent_Tree_Workers(t *testing.T) {
	t.Parallel()

	root := t.TempDir()

	// Create many directories so all workers can get a job.
	const (
		workers = 4
		dirs    = 64
	)

	for i := range dirs {
		p := filepath.Join(fmt.Sprintf("d%03d", i), "f.txt")
		writeFile(t, root, p, []byte("x"))
	}

	ptrCh := make(chan uintptr, workers*4)
	release := make(chan struct{})

	var ok struct{}

	opts := fileproc.Options{
		Recursive:          true,
		Workers:            workers,
		SmallFileThreshold: 1_000_000,
	}

	done := make(chan struct{})

	var errs []error

	go func() {
		_, errs = fileproc.Process(t.Context(), root, func(_ []byte, data []byte) (*struct{}, error) {
			if len(data) == 0 {
				return nil, errors.New("unexpected empty data")
			}

			ptr := uintptr(unsafe.Pointer(&data[0]))

			select {
			case ptrCh <- ptr:
			default:
			}

			<-release

			return &ok, nil
		}, opts)

		close(done)
	}()

	// Wait until we have observed one buffer pointer per worker.
	ptrs := make(map[uintptr]struct{})
	deadline := time.Now().Add(2 * time.Second)

	for len(ptrs) < workers && time.Now().Before(deadline) {
		select {
		case p := <-ptrCh:
			ptrs[p] = struct{}{}
		case <-time.After(5 * time.Millisecond):
		}
	}

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
		t.Fatalf("expected %d distinct data buffers across concurrent workers, got %d", workers, len(ptrs))
	}
}

func Test_ProcessReader_Does_Not_Share_ReplayReader_When_Using_Concurrent_Tree_Workers(t *testing.T) {
	t.Parallel()

	root := t.TempDir()

	const (
		workers = 4
		dirs    = 64
	)

	for i := range dirs {
		p := filepath.Join(fmt.Sprintf("d%03d", i), "f.txt")
		writeFile(t, root, p, []byte("x"))
	}

	ptrCh := make(chan uintptr, workers*4)
	release := make(chan struct{})

	var ok struct{}

	opts := fileproc.Options{
		Recursive:          true,
		Workers:            workers,
		SmallFileThreshold: 1_000_000,
	}

	done := make(chan struct{})

	var errs []error

	go func() {
		_, errs = fileproc.ProcessReader(t.Context(), root, func(_ []byte, r io.Reader) (*struct{}, error) {
			// The concrete type is an unexported *replayReader. We can't type-assert,
			// but we can still observe pointer identity.
			v := reflect.ValueOf(r)
			if v.Kind() != reflect.Ptr {
				return nil, fmt.Errorf("expected pointer reader, got %v", v.Kind())
			}

			ptr := v.Pointer()

			select {
			case ptrCh <- ptr:
			default:
			}

			<-release

			return &ok, nil
		}, opts)

		close(done)
	}()

	ptrs := make(map[uintptr]struct{})
	deadline := time.Now().Add(2 * time.Second)

	for len(ptrs) < workers && time.Now().Before(deadline) {
		select {
		case p := <-ptrCh:
			ptrs[p] = struct{}{}
		case <-time.After(5 * time.Millisecond):
		}
	}

	close(release)

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("ProcessReader did not return (possible deadlock)")
	}

	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}

	if len(ptrs) != workers {
		t.Fatalf("expected %d distinct replayReader instances across concurrent workers, got %d", workers, len(ptrs))
	}
}

func Test_Process_Does_Not_Share_DataBuffers_When_Using_Pipelined_Workers(t *testing.T) {
	t.Parallel()

	root := t.TempDir()

	const workers = 4

	pad := strings.Repeat("x", testNamePad)

	for i := range testNumFilesBig {
		name := fmt.Sprintf("f-%04d-%s.txt", i, pad)
		writeFile(t, root, name, []byte("x"))
	}

	ptrCh := make(chan uintptr, workers*8)
	release := make(chan struct{})

	var ok struct{}

	opts := fileproc.Options{
		Workers:            workers,
		SmallFileThreshold: 1,
	}

	done := make(chan struct{})

	var (
		results []fileproc.Result[struct{}]
		errs    []error
	)

	go func() {
		results, errs = fileproc.Process(t.Context(), root, func(_ []byte, data []byte) (*struct{}, error) {
			if len(data) == 0 {
				return nil, errors.New("unexpected empty data")
			}

			ptr := uintptr(unsafe.Pointer(&data[0]))

			select {
			case ptrCh <- ptr:
			default:
			}

			<-release

			return &ok, nil
		}, opts)

		close(done)
	}()

	ptrs := make(map[uintptr]struct{})
	deadline := time.Now().Add(2 * time.Second)

	for len(ptrs) < workers && time.Now().Before(deadline) {
		select {
		case p := <-ptrCh:
			ptrs[p] = struct{}{}
		case <-time.After(5 * time.Millisecond):
		}
	}

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
		t.Fatalf("expected %d distinct data buffers across concurrent pipeline workers, got %d", workers, len(ptrs))
	}
}
