package daemon

import "sync"

// Ring is a fixed size byte ring buffer used to keep the tail of a session's
// terminal output so a client can re-attach or read recent logs.
type Ring struct {
	mu   sync.Mutex
	buf  []byte
	size int
	full bool
	head int // next write position
}

// NewRing creates a ring buffer holding at most size bytes.
func NewRing(size int) *Ring {
	if size <= 0 {
		size = 256 << 10
	}
	return &Ring{buf: make([]byte, size), size: size}
}

// Write appends p, dropping the oldest bytes when full.
func (r *Ring) Write(p []byte) {
	if len(p) == 0 {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(p) >= r.size {
		copy(r.buf, p[len(p)-r.size:])
		r.head = 0
		r.full = true
		return
	}
	n := copy(r.buf[r.head:], p)
	if n < len(p) {
		copy(r.buf, p[n:])
	}
	r.head = (r.head + len(p)) % r.size
	if r.head < len(p) {
		r.full = true
	}
	if !r.full && r.head > 0 {
		// still filling, nothing dropped
	}
}

// Len reports the number of buffered bytes.
func (r *Ring) Len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.full {
		return r.size
	}
	return r.head
}

// Snapshot returns a copy of the buffered bytes in chronological order.
func (r *Ring) Snapshot() []byte {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.full {
		out := make([]byte, r.head)
		copy(out, r.buf[:r.head])
		return out
	}
	out := make([]byte, r.size)
	n := copy(out, r.buf[r.head:])
	copy(out[n:], r.buf[:r.head])
	return out
}

// Tail returns at most n of the most recent bytes.
func (r *Ring) Tail(n int) []byte {
	snap := r.Snapshot()
	if n <= 0 || n >= len(snap) {
		return snap
	}
	return snap[len(snap)-n:]
}
