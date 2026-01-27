package fileproc_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/calvinalkan/fileproc"
)

func Test_Process_Does_Not_Hang_When_Cancelled_In_Tree_LargeDir_Pipelined_Mode(t *testing.T) {
	t.Parallel()

	root := t.TempDir()

	pad := strings.Repeat("x", testNamePad)

	for i := range testNumFilesMed {
		name := filepath.Join("big", fmt.Sprintf("f-%04d-%s.txt", i, pad))
		writeFile(t, root, name, []byte("x"))
	}

	ctx, cancel := context.WithCancelCause(t.Context())
	t.Cleanup(func() { cancel(nil) })

	stopErr := errors.New("stop")

	started := make(chan struct{}, 1)
	release := make(chan struct{})

	var ok struct{}

	done := make(chan struct{})

	var (
		results []fileproc.Result[struct{}]
		errs    []error
	)

	opts := fileproc.Options{
		Recursive:          true,
		Workers:            2,
		SmallFileThreshold: 1, // force within-tree pipelining for "big"
	}

	go func() {
		results, errs = fileproc.Process(ctx, root, func(_ []byte, _ []byte) (*struct{}, error) {
			select {
			case started <- struct{}{}:
			default:
			}

			<-release

			return &ok, nil
		}, opts)

		close(done)
	}()

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		close(release)
		t.Fatal("timeout waiting for callback to start")
	}

	// Give the within-tree pipeline producer time to fill the queue / potentially block.
	time.Sleep(50 * time.Millisecond)

	cancel(stopErr)
	close(release)

	select {
	case <-done:
		// ok
	case <-time.After(3 * time.Second):
		t.Fatal("Process did not return after cancellation (possible deadlock in tree large-dir pipeline)")
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

func Test_ProcessReader_Does_Not_Hang_When_Cancelled_In_Tree_LargeDir_Pipelined_Mode(t *testing.T) {
	t.Parallel()

	root := t.TempDir()

	pad := strings.Repeat("x", testNamePad)

	for i := range testNumFilesMed {
		name := filepath.Join("big", fmt.Sprintf("f-%04d-%s.txt", i, pad))
		writeFile(t, root, name, []byte("x"))
	}

	ctx, cancel := context.WithCancelCause(t.Context())
	t.Cleanup(func() { cancel(nil) })

	stopErr := errors.New("stop")

	started := make(chan struct{}, 1)
	release := make(chan struct{})

	var ok struct{}

	done := make(chan struct{})

	var (
		results []fileproc.Result[struct{}]
		errs    []error
	)

	opts := fileproc.Options{
		Recursive:          true,
		Workers:            2,
		SmallFileThreshold: 1,
	}

	go func() {
		results, errs = fileproc.ProcessReader(ctx, root, func(_ []byte, _ io.Reader) (*struct{}, error) {
			select {
			case started <- struct{}{}:
			default:
			}

			<-release

			return &ok, nil
		}, opts)

		close(done)
	}()

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		close(release)
		t.Fatal("timeout waiting for callback to start")
	}

	time.Sleep(50 * time.Millisecond)

	cancel(stopErr)
	close(release)

	select {
	case <-done:
		// ok
	case <-time.After(3 * time.Second):
		t.Fatal("ProcessReader did not return after cancellation (possible deadlock in tree large-dir pipeline)")
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
