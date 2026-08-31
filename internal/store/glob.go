package store

// escapeChar is the backslash that escapes the next byte in a glob pattern.
const escapeChar = 0x5c

// Match reports whether s matches a Redis glob-style pattern.
//
// Supported syntax, matching the real KEYS/SCAN implementation:
//
//   - any sequence of bytes, including none
//     ?        exactly one byte
//     [abc]    any one of the listed bytes
//     [^abc]   any byte not listed
//     [a-z]    any byte in the range
//     \x       the literal byte x
//
// Matching is byte-oriented rather than rune-oriented because Redis keys are
// arbitrary binary strings, not necessarily valid UTF-8.
//
// The implementation is the standard iterative glob matcher: it remembers the
// most recent '*' and, on a mismatch, backtracks to let that star consume one
// more byte. That keeps it linear in the common case and avoids the exponential
// blowup a naive recursive matcher hits on patterns like "a*a*a*a*b".
func Match(pattern, s string) bool {
	// Fast path: "*" matches everything and is by far the most common pattern.
	if pattern == "*" {
		return true
	}

	var (
		sIdx, pIdx int
		starPIdx   = -1
		starSIdx   int
		haveStar   bool
	)
	for sIdx < len(s) {
		if pIdx < len(pattern) && pattern[pIdx] == '*' {
			starPIdx, starSIdx, haveStar = pIdx, sIdx, true
			pIdx++
			continue
		}
		if pIdx < len(pattern) {
			if ok, next := matchOne(pattern, pIdx, s[sIdx]); ok {
				pIdx = next
				sIdx++
				continue
			}
		}
		if !haveStar {
			return false
		}
		// Backtrack: let the remembered star swallow one more byte of s.
		starSIdx++
		pIdx = starPIdx + 1
		sIdx = starSIdx
	}
	// s is exhausted; the rest of the pattern must be stars.
	for pIdx < len(pattern) && pattern[pIdx] == '*' {
		pIdx++
	}
	return pIdx == len(pattern)
}

// matchOne reports whether the single pattern token beginning at pIdx matches
// the byte c, and returns the index just past that token.
func matchOne(pattern string, pIdx int, c byte) (bool, int) {
	switch pattern[pIdx] {
	case '?':
		return true, pIdx + 1

	case '[':
		return matchClass(pattern, pIdx, c)

	case escapeChar:
		// A trailing backslash is treated as a literal backslash.
		if pIdx+1 < len(pattern) {
			return pattern[pIdx+1] == c, pIdx + 2
		}
		return c == escapeChar, pIdx + 1

	default:
		return pattern[pIdx] == c, pIdx + 1
	}
}

// matchClass evaluates a bracket expression starting at the '[' in pattern.
//
// An unterminated class (no closing ']') is treated as a literal '[', which is
// what Redis does rather than reporting an error.
func matchClass(pattern string, pIdx int, c byte) (bool, int) {
	i := pIdx + 1
	negate := false
	if i < len(pattern) && (pattern[i] == '^') {
		negate = true
		i++
	}

	matched := false
	first := true
	for i < len(pattern) {
		if pattern[i] == ']' && !first {
			i++ // consume ']'
			if negate {
				matched = !matched
			}
			return matched, i
		}
		first = false

		switch {
		case pattern[i] == escapeChar && i+1 < len(pattern):
			i++
			if pattern[i] == c {
				matched = true
			}
			i++

		// A range needs a closing byte after '-' that is not the class
		// terminator, so "[a-]" treats '-' as a literal.
		case i+2 < len(pattern) && pattern[i+1] == '-' && pattern[i+2] != ']':
			lo, hi := pattern[i], pattern[i+2]
			if lo > hi {
				lo, hi = hi, lo
			}
			if c >= lo && c <= hi {
				matched = true
			}
			i += 3

		default:
			if pattern[i] == c {
				matched = true
			}
			i++
		}
	}

	// Unterminated class: fall back to matching a literal '['.
	return c == '[', pIdx + 1
}
