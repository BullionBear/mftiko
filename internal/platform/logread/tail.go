//go:build unix

package logread

import "io"

// scanBlock is how much of a file tail reads at a time while walking backward.
const scanBlock = 8192

// findTail returns the file index (oldest is 0) and offset where the last
// lines of the concatenation begin.
//
// A line ends at '\n'. The final line counts even when the stream does not end
// with '\n', matching docker's --tail (E14-D012). The search therefore looks
// back across N newline boundaries after a trailing newline, which is N+1
// newline bytes when the file ends with one, and continues into .1, .2, …
// when the live file does not hold enough (E14-03). lines <= 0 is the end of
// the newest file: no history.
func findTail(ents []fileEnt, lines int) (int, int64, error) {
	last := len(ents) - 1
	if lines <= 0 {
		return last, ents[last].size, nil
	}
	need := lines
	atEnd := true
	for i := last; i >= 0; i-- {
		off, found, err := scanBack(ents[i].path, ents[i].size, &need, &atEnd)
		if err != nil {
			return 0, 0, err
		}
		if found {
			return i, off, nil
		}
	}
	return 0, 0, nil
}

// scanBack walks path from size toward 0. need counts newlines still required.
// atEnd is true only before the last byte of the whole concatenation has been
// seen; a newline there belongs to the last line and is not a separator for an
// extra empty line.
func scanBack(path string, size int64, need *int, atEnd *bool) (int64, bool, error) {
	if size == 0 {
		return 0, false, nil
	}
	f, err := openNoFollow(path)
	if err != nil {
		return 0, false, err
	}
	defer f.Close()
	buf := make([]byte, scanBlock)
	pos := size
	for pos > 0 {
		n := int64(len(buf))
		if pos < n {
			n = pos
		}
		pos -= n
		if _, err := io.ReadFull(io.NewSectionReader(f, pos, n), buf[:n]); err != nil {
			return 0, false, err
		}
		for j := int(n) - 1; j >= 0; j-- {
			if *atEnd {
				*atEnd = false
				if buf[j] == '\n' {
					continue
				}
			}
			if buf[j] == '\n' {
				*need--
				if *need == 0 {
					return pos + int64(j) + 1, true, nil
				}
			}
		}
	}
	return 0, false, nil
}
