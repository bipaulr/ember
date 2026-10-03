package store

// List is a double-ended queue backed by a growable ring buffer, so pushes and
// pops at either end are O(1) and indexed access is O(1).
type List struct {
	buf  [][]byte
	head int
	n    int
}

// NewList returns an empty list.
func NewList() *List { return &List{} }

// Len returns the number of elements.
func (l *List) Len() int { return l.n }

func (l *List) grow() {
	nc := len(l.buf) * 2
	if nc < 8 {
		nc = 8
	}
	nb := make([][]byte, nc)
	for i := 0; i < l.n; i++ {
		nb[i] = l.buf[(l.head+i)%len(l.buf)]
	}
	l.buf = nb
	l.head = 0
}

// PushBack appends v.
func (l *List) PushBack(v []byte) {
	if l.n == len(l.buf) {
		l.grow()
	}
	l.buf[(l.head+l.n)%len(l.buf)] = v
	l.n++
}

// PushFront prepends v.
func (l *List) PushFront(v []byte) {
	if l.n == len(l.buf) {
		l.grow()
	}
	l.head = (l.head - 1 + len(l.buf)) % len(l.buf)
	l.buf[l.head] = v
	l.n++
}

// PopFront removes and returns the first element. It panics on an empty list.
func (l *List) PopFront() []byte {
	v := l.buf[l.head]
	l.buf[l.head] = nil
	l.head = (l.head + 1) % len(l.buf)
	l.n--
	return v
}

// PopBack removes and returns the last element. It panics on an empty list.
func (l *List) PopBack() []byte {
	i := (l.head + l.n - 1) % len(l.buf)
	v := l.buf[i]
	l.buf[i] = nil
	l.n--
	return v
}

// At returns element i, where 0 <= i < Len().
func (l *List) At(i int) []byte { return l.buf[(l.head+i)%len(l.buf)] }
