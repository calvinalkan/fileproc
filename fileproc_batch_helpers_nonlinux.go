//go:build !linux

package fileproc

// appendString copies a filename into the batch, appending a NUL terminator.
//
// name must NOT include a NUL terminator (it is added here).
func (b *nameBatch) appendString(name string) {
	start := len(b.storage)
	b.storage = append(b.storage, name...)
	b.storage = append(b.storage, 0)
	b.names = append(b.names, b.storage[start:len(b.storage)])
}
