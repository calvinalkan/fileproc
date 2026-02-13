package fileproc

import "sync"

// FileWorker provides low-level memory helpers to avoid per-callback
// allocations and reduce GC pressure.
//
// Memory modes:
//   - [FileWorker.AllocateScratch]: callback-local scratch buffer. Borrowed,
//     reused, and valid only during the current callback.
//   - [FileWorker.AllocateOwned]: caller-owned writable buffer. It can be kept
//     across callbacks and freed early via [Owned.Free].
//
// Use [FileWorker.ID] to index caller-owned per-worker state without locks.
// IDs are unique within a single [Process] invocation and stable for that
// worker until the invocation returns.
type FileWorker struct {
	// id is stable for this worker within one Process invocation.
	id int

	// Reusable scratch slots for this worker.
	// Slots 0/1 are reserved for File internals; public AllocateScratch uses
	// slots starting at scratchSlotFirstPublic.
	perCallbackScratchSlots [][]byte

	// Current callback slot index into perCallbackScratchSlots.
	// It is reset to scratchSlotFirstPublic before each callback.
	nextScratchSlotIdx int

	// ownedBufferPool tracks callback-independent buffers returned by AllocateOwned.
	ownedBufferPool ownedBufferPool

	// currentFile is reused to avoid per-file File allocations.
	currentFile File
}

const (
	// scratchSlotAbsPath is reserved for [File.AbsPath]/[File.RelPath] internals.
	scratchSlotAbsPath = 0
	// scratchSlotFileBytes is reserved for [File.ReadAll] internals.
	scratchSlotFileBytes = 1
	// scratchSlotFirstPublic is the first slot used by public AllocateScratch.
	scratchSlotFirstPublic = 2
)

// ID returns this callback worker's identity.
//
// The ID is unique only within a single [Process] invocation and remains
// stable for this worker until [Process] returns. IDs are zero-based within
// that invocation (0..workers-1). IDs may be reused across separate [Process]
// calls.
//
// Use it to index caller-owned per-worker state without synchronization.
func (w *FileWorker) ID() int {
	return w.id
}

// AllocateScratch returns a callback-local scratch buffer with len=0 and
// cap>=size.
//
// The returned slice is valid only until the callback returns.
//
// Multiple calls within the same callback are safe: each call uses the next
// scratch slot, so earlier results remain valid for that callback.
//
// Scratch slots are reused across callbacks on the same worker. A slot may
// still grow (and reallocate) if a later callback requests a larger size for
// that slot.
//
// size <= 0 is treated as size == 0.
//
// Typical append usage:
//
//	buf := w.AllocateScratch(4096)
//	buf = append(buf, data...) // no alloc if fits
//
// Fixed-size read usage:
//
//	buf := w.AllocateScratch(4096)
//	buf = buf[:cap(buf)]
//	n, _ := io.ReadFull(r, buf)
func (w *FileWorker) AllocateScratch(size int) []byte {
	// AllocateScratch is multi-use per callback: each call consumes the next
	// slot so earlier returned slices remain valid until callback end.
	//
	// Timeline for public AllocateScratch calls (same worker):
	//   - callback A: 3 calls -> slots 2,3,4 used (create/grow as needed)
	//   - callback B: 2 calls -> slots 2,3 reused (and can still grow)
	//   - callback C: 4 calls -> slots 2,3,4 reused; slot 5 created/grown
	slot := w.nextScratchSlotIdx
	w.nextScratchSlotIdx++

	return w.allocateScratchAt(slot, size)
}

// AllocateOwned returns a writable owned buffer with len=size.
//
// The returned memory is independent from callback-local scratch and can be
// retained across callbacks and after [Process] returns. Use [Owned.Free] for
// early reuse.
//
// size <= 0 returns a non-nil owned buffer with len=0.
func (w *FileWorker) AllocateOwned(size int) Owned {
	return w.ownedBufferPool.allocate(size)
}

// allocateScratchAt returns the requested scratch slot with len=0 and cap>=size.
//
// The zero length is intentional: append-style callers can build into the buffer
// without carrying stale bytes from previous callback uses. Callers that need a
// fixed-size writable window must reslice first (e.g. buf = buf[:size]).
//
// Internal code uses fixed slots; public AllocateScratch uses dynamic slots.
func (w *FileWorker) allocateScratchAt(slot, size int) []byte {
	if size < 0 {
		size = 0
	}

	// This callback requested more scratch buffers than previously seen on this
	// worker; append a new reusable slot.
	if slot >= len(w.perCallbackScratchSlots) {
		growBy := slot - len(w.perCallbackScratchSlots) + 1
		w.perCallbackScratchSlots = append(w.perCallbackScratchSlots, make([][]byte, growBy)...)
	}

	buffer := w.perCallbackScratchSlots[slot]
	if cap(buffer) < size {
		// This slot is too small, grow it. It then stays at least as large as
		// the largest request ever seen for this slot on this worker.
		buffer = make([]byte, 0, size)
		w.perCallbackScratchSlots[slot] = buffer
	}

	return buffer[:0]
}

// Owned is a writable caller-owned buffer allocated by [FileWorker.AllocateOwned].
//
// Buf remains valid until [Owned.Free]. The zero value is valid; Free is a
// no-op.
//
// Owned may be copied by value. Copies share the same underlying Buf and lease
// handle.
//
// If you keep Buf after Free, behavior is undefined from the API perspective
// because storage may be reused.
type Owned struct {
	// Buf has len equal to requested size and is writable.
	Buf []byte

	pool    *ownedBufferPool
	leaseID uint64
}

// Free returns this owned buffer to the worker reuse pool.
//
// Free is safe to call from any goroutine (including multiple goroutines
// holding copies of the same Owned) and is idempotent.
// Calling Free after [Process] returns is also a no-op.
func (o Owned) Free() {
	if o.pool == nil {
		return
	}

	o.pool.release(o.leaseID)
}

// ownedBufferPool tracks in-use owned buffers and reusable released buffers.
type ownedBufferPool struct {
	// Protects nextLeaseID, inUse, available, and closed.
	// Needed because [Owned.Free] may run from goroutines outside the callback
	// that created the lease, while allocate/shutdown run on worker lifecycle.
	mu sync.Mutex

	nextLeaseID uint64
	inUse       map[uint64][]byte
	available   [][]byte
	closed      bool
}

func (o *ownedBufferPool) allocate(size int) Owned {
	if size < 0 {
		size = 0
	}

	// Serialize pool state updates (inUse/available/nextLeaseID/closed).
	o.mu.Lock()
	defer o.mu.Unlock()

	if o.closed {
		// After shutdown, allocations become inert; callers get an empty buffer.
		return Owned{Buf: []byte{}}
	}

	if o.inUse == nil {
		o.inUse = make(map[uint64][]byte, 8)
	}

	// Monotonic lease IDs let release ignore stale/double frees safely.
	nextLeaseID := o.nextLeaseID + 1
	o.nextLeaseID = nextLeaseID

	// Reuse released storage when possible; allocate only when needed.
	var buffer []byte

	// Scan available buffers from the end (LIFO) to favor hot, recently freed
	// allocations and keep removal O(1).
	for i := len(o.available) - 1; i >= 0; i-- {
		if cap(o.available[i]) < size {
			continue
		}

		// Re-slice reused buffer to requested len while keeping backing storage.
		buffer = o.available[i][:size]
		last := len(o.available) - 1
		// Remove chosen entry in O(1):
		// 1) move last entry into i
		// 2) clear old last slot to drop backing-array reference
		// 3) truncate slice by one
		o.available[i] = o.available[last]
		o.available[last] = nil
		o.available = o.available[:last]

		break
	}

	// No reusable buffer matched: keep zero-size allocations non-nil, allocate
	// fresh backing storage otherwise.
	if buffer == nil {
		if size == 0 {
			buffer = []byte{}
		} else {
			buffer = make([]byte, 0, size)[:size]
		}
	}

	o.inUse[nextLeaseID] = buffer

	return Owned{
		Buf:     buffer,
		pool:    o,
		leaseID: nextLeaseID,
	}
}

func (o *ownedBufferPool) release(leaseID uint64) {
	// Owned.Free can be called from any goroutine, so release must synchronize
	// with concurrent allocate/shutdown and map/pool mutations.
	o.mu.Lock()
	defer o.mu.Unlock()

	// After shutdown, leases are dropped and no further pooling occurs.
	if o.closed {
		return
	}

	// Unknown lease ID means already released (or never allocated); no-op, mimic go's delete().
	buffer, ok := o.inUse[leaseID]
	if !ok {
		return
	}

	// Move from in-use set back to reusable pool. Keep capacity, reset len.
	delete(o.inUse, leaseID)
	o.available = append(o.available, buffer[:0])
}

func (o *ownedBufferPool) shutdown() {
	o.mu.Lock()
	defer o.mu.Unlock()

	// Mark pool unusable and drop pool-owned references.
	// If callers still hold copied Owned values / Owned.Buf slices, those
	// external references keep backing arrays alive until the caller drops them.
	o.closed = true
	clear(o.inUse)
	o.inUse = nil
	o.available = nil
}
