//go:build !fileproc_testhooks

package fileproc

func readDirBatch(rh readdirHandle, dirPath nulTermPath, buf []byte, suffix string, batch *pathArena, reportSubdir func(nulTermName)) error {
	return readDirBatchImpl(rh, dirPath, buf, suffix, batch, reportSubdir)
}

// Compile-time guard: wrapper signature must match the backend contract.
var _ func(readdirHandle, nulTermPath, []byte, string, *pathArena, func(nulTermName)) error = readDirBatch
