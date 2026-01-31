package fileproc

import "os"

// ============================================================================
// Path helpers
// ============================================================================

// appendPathBytesPrefix builds a path from prefix and name (which includes NUL terminator).
// Returns a slice WITHOUT NUL terminator (for display/string conversion).
func appendPathBytesPrefix(buf []byte, prefix []byte, name []byte) []byte {
	buf = buf[:0]
	buf = appendPathPrefix(buf, prefix)

	buf = append(buf, name[:nameLen(name)]...)

	return buf
}

// appendPathPrefix appends prefix and a separator (if needed) to buf.
// Caller controls buf capacity and initial length.
func appendPathPrefix(buf []byte, prefix []byte) []byte {
	if len(prefix) == 0 {
		return buf
	}

	buf = append(buf, prefix...)

	last := prefix[len(prefix)-1]
	if last != os.PathSeparator && last != '/' {
		buf = append(buf, os.PathSeparator)
	}

	return buf
}

// pathWithNul converts a string path to []byte with NUL terminator.
// Used for syscalls that require NUL-terminated paths.
func pathWithNul(s string) []byte {
	b := make([]byte, 0, len(s)+1)
	b = append(b, s...)
	b = append(b, 0)

	return b
}

// pathStr converts a NUL-terminated path back to string (strips NUL).
// Used for error messages.
func pathStr(p []byte) string {
	if len(p) > 0 && p[len(p)-1] == 0 {
		return string(p[:len(p)-1])
	}

	return string(p)
}

// joinPathWithNul joins a base path (NUL-terminated) with a name and returns
// a new NUL-terminated path. base must be NUL-terminated.
func joinPathWithNul(base, name []byte) []byte {
	// base includes NUL at end, name does not include NUL.
	baseLen := len(base)
	if baseLen > 0 && base[baseLen-1] == 0 {
		baseLen--
	}

	sep := byte(os.PathSeparator)

	// Note: We must not blindly append a separator. For example, when base is the
	// filesystem root ("/" on Unix, "C:\\" on Windows), it already ends with a
	// separator.
	result := make([]byte, 0, baseLen+1+len(name)+1)

	result = append(result, base[:baseLen]...)
	if baseLen > 0 {
		last := base[baseLen-1]
		if last != sep && last != '/' {
			result = append(result, sep)
		}
	}

	result = append(result, name...)
	result = append(result, 0)

	return result
}

// nameLen returns the length of the filename EXCLUDING the NUL terminator.
// Use this when calculating buffer sizes or path lengths.
func nameLen(name []byte) int {
	if len(name) == 0 {
		return 0
	}

	return len(name) - 1
}
