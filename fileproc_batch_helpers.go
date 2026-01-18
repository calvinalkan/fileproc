package fileproc

// byteSeq is a small constraint used for shared helper functions that work on
// both string directory entry names (non-Linux/Windows ReadDir) and []byte
// directory entry names (Linux getdents64 parsing).
//
// Keeping these helpers in one place avoids drift across platform-specific
// files.
type byteSeq interface {
	~string | ~[]byte
}

// hasSuffix reports whether name ends with suffix. Empty suffix matches all.
//
// Implemented once for both string and []byte names.
func hasSuffix[S byteSeq](name S, suffix string) bool {
	if suffix == "" {
		return true
	}

	if len(name) < len(suffix) {
		return false
	}

	start := len(name) - len(suffix)
	for i := range len(suffix) {
		if name[start+i] != suffix[i] {
			return false
		}
	}

	return true
}

// appendBytes adds a filename to the batch, appending a NUL terminator.
//
// name must NOT include a NUL terminator (it is added here).
func (b *nameBatch) appendBytes(name []byte) {
	start := len(b.storage)
	b.storage = append(b.storage, name...) // copy filename bytes into arena
	b.storage = append(b.storage, 0)       // append NUL terminator for syscalls
	b.names = append(b.names, b.storage[start:len(b.storage)])
}
