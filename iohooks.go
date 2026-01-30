//go:build fileproc_testhooks

package fileproc

import "sync/atomic"

// This file provides test-only I/O hooks for the internal backend contract.
//
// Build tag:
//   - Enabled only when tests are run with: go test -tags fileproc_testhooks ./...
//   - Normal builds use iohooks_stub.go, which forwards directly to the backend
//     implementation with zero hook overhead.
//
// How it is called:
//   - Process -> processDir / processTree -> readDirBatch(...)
//   - The wrapper below intercepts readDirBatch and (optionally) routes it to
//     a test hook. This allows deterministic injection of partial reads and
//     non-EOF errors without relying on filesystem quirks.
//
// Scope and safety:
//   - The hook is global to the test binary. Tests that install it MUST NOT be
//     run in parallel with other hook users.
//   - An atomic pointer is used to avoid data races with non-hooked tests.

// readDirBatchHook enables deterministic error-injection in tests without
// adding explicit dependencies to the hot path.
type readDirBatchHookFn func(readdirHandle, []byte, string, *nameBatch, func([]byte)) error

var readDirBatchHook atomic.Pointer[readDirBatchHookFn]

// setReadDirBatchHook installs a hook and returns a restore function.
//
// Usage:
//
//	restore := setReadDirBatchHook(func(...) error { ... })
//	defer restore()
//
// Passing nil removes any previously-installed hook.
func setReadDirBatchHook(hook readDirBatchHookFn) func() {
	if hook == nil {
		readDirBatchHook.Store(nil)

		return func() {}
	}

	ptr := new(readDirBatchHookFn)
	*ptr = hook
	readDirBatchHook.Store(ptr)

	return func() {
		readDirBatchHook.Store(nil)
	}
}

// readDirBatch wraps the backend implementation and optionally diverts to the
// test hook. The call signature matches the backend contract exactly.
func readDirBatch(rh readdirHandle, buf []byte, suffix string, batch *nameBatch, reportSubdir func([]byte)) error {
	if hook := readDirBatchHook.Load(); hook != nil {
		return (*hook)(rh, buf, suffix, batch, reportSubdir)
	}

	return readDirBatchImpl(rh, buf, suffix, batch, reportSubdir)
}

// Compile-time guard: wrapper signature must match the backend contract.
var _ func(readdirHandle, []byte, string, *nameBatch, func([]byte)) error = readDirBatch
