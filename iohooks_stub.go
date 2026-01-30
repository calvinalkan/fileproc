//go:build !fileproc_testhooks

package fileproc

func readDirBatch(rh readdirHandle, buf []byte, suffix string, batch *nameBatch, reportSubdir func([]byte)) error {
	return readDirBatchImpl(rh, buf, suffix, batch, reportSubdir)
}

// Compile-time guard: wrapper signature must match the backend contract.
var _ func(readdirHandle, []byte, string, *nameBatch, func([]byte)) error = readDirBatch
