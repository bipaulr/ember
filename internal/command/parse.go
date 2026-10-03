package command

import "math"

// parseInt parses a canonical base-10 int64: an optional '-', no '+', no
// leading zeros, no spaces. This is the same strictness as Redis string2ll.
func parseInt(b []byte) (int64, bool) {
	n := len(b)
	if n == 0 || n > 20 {
		return 0, false
	}
	i, neg := 0, false
	if b[0] == '-' {
		neg = true
		i = 1
		if n == 1 {
			return 0, false
		}
	}
	if b[i] == '0' {
		return 0, n == 1
	}
	var u uint64
	for ; i < n; i++ {
		d := b[i] - '0'
		if d > 9 {
			return 0, false
		}
		if u > (math.MaxUint64-uint64(d))/10 {
			return 0, false
		}
		u = u*10 + uint64(d)
	}
	if neg {
		if u > 1<<63 {
			return 0, false
		}
		return int64(-u), true
	}
	if u > math.MaxInt64 {
		return 0, false
	}
	return int64(u), true
}
