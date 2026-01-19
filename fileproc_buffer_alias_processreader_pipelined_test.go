package fileproc_test

import (
	"fmt"
	"io"
	"reflect"
	"strings"
	"testing"
	"time"

	"fileproc"
)

func Test_ProcessReader_Does_Not_Share_ReplayReader_When_Using_Pipelined_Workers(t *testing.T) {
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
		SmallFileThreshold: 1, // force pipelined mode
	}

	done := make(chan struct{})

	var errs []error

	go func() {
		_, errs = fileproc.ProcessReader(t.Context(), root, func(_ []byte, r io.Reader) (*struct{}, error) {
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
		// ok
	case <-time.After(5 * time.Second):
		t.Fatal("ProcessReader did not return (possible deadlock)")
	}

	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}

	if len(ptrs) != workers {
		t.Fatalf("expected %d distinct replayReader instances across concurrent pipeline workers, got %d", workers, len(ptrs))
	}
}
