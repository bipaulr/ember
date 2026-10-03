package command

// globMatch reports whether s matches the Redis glob pattern: * ? [abc] [^a]
// [a-z] and backslash escapes. It runs in O(len(pattern)*len(s)) worst case and
// never recurses, so hostile patterns cannot blow the stack.
func globMatch(pattern, s string) bool {
	p, i := 0, 0
	starP, starI := -1, 0
	for i < len(s) {
		if p < len(pattern) {
			switch pattern[p] {
			case '*':
				for p < len(pattern) && pattern[p] == '*' {
					p++
				}
				if p == len(pattern) {
					return true
				}
				starP, starI = p, i
				continue
			case '?':
				p++
				i++
				continue
			case '[':
				if np, ok := matchClass(pattern, p, s[i]); ok {
					p = np
					i++
					continue
				}
			case '\\':
				if p+1 < len(pattern) {
					if pattern[p+1] == s[i] {
						p += 2
						i++
						continue
					}
				} else if s[i] == '\\' {
					p++
					i++
					continue
				}
			default:
				if pattern[p] == s[i] {
					p++
					i++
					continue
				}
			}
		}
		if starP >= 0 {
			starI++
			i = starI
			p = starP
			continue
		}
		return false
	}
	for p < len(pattern) && pattern[p] == '*' {
		p++
	}
	return p == len(pattern)
}

// matchClass matches c against the bracket class starting at pattern[p]. On a
// match it returns the index just after the closing bracket.
func matchClass(pattern string, p int, c byte) (int, bool) {
	p++ // skip '['
	negate := false
	if p < len(pattern) && pattern[p] == '^' {
		negate = true
		p++
	}
	matched := false
	for p < len(pattern) && pattern[p] != ']' {
		switch {
		case pattern[p] == '\\' && p+1 < len(pattern):
			p++
			if pattern[p] == c {
				matched = true
			}
			p++
		case p+2 < len(pattern) && pattern[p+1] == '-' && pattern[p+2] != ']':
			lo, hi := pattern[p], pattern[p+2]
			if lo > hi {
				lo, hi = hi, lo
			}
			if c >= lo && c <= hi {
				matched = true
			}
			p += 3
		default:
			if pattern[p] == c {
				matched = true
			}
			p++
		}
	}
	if p < len(pattern) { // consume ']'
		p++
	}
	if negate {
		matched = !matched
	}
	return p, matched
}
