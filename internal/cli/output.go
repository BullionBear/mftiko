package cli

import (
	"fmt"
	"io"
	"strings"
	"time"
	"unicode/utf8"

	apiv1 "github.com/BullionBear/mftiko/contract/gen/api/v1"
)

// WriteTable writes a header and rows. Columns are separated by two spaces and
// padded to the widest cell. The last column is not padded, and each line has
// no trailing space.
func WriteTable(w io.Writer, header []string, rows [][]string) error {
	n := len(header)
	width := make([]int, n)
	for i, h := range header {
		width[i] = utf8.RuneCountInString(h)
	}
	cell := func(row []string, i int) string {
		if i < len(row) {
			return row[i]
		}
		return ""
	}
	for _, row := range rows {
		for i := 0; i < n; i++ {
			if c := utf8.RuneCountInString(cell(row, i)); c > width[i] {
				width[i] = c
			}
		}
	}
	write := func(row []string) error {
		var b strings.Builder
		for i := 0; i < n; i++ {
			if i > 0 {
				b.WriteString("  ")
			}
			text := cell(row, i)
			b.WriteString(text)
			if i < n-1 {
				if pad := width[i] - utf8.RuneCountInString(text); pad > 0 {
					b.WriteString(strings.Repeat(" ", pad))
				}
			}
		}
		line := strings.TrimRight(b.String(), " ")
		_, err := fmt.Fprintln(w, line)
		return err
	}
	if err := write(header); err != nil {
		return err
	}
	for _, row := range rows {
		if err := write(row); err != nil {
			return err
		}
	}
	return nil
}

// meTable is the default whoami and login table (E11-D008). expires_at is the
// local zone, RFC 3339 with a numeric offset. A null expires_at (bearer) is blank.
func meTable(me apiv1.Me) ([]string, [][]string) {
	exp := ""
	if me.ExpiresAt != nil {
		exp = me.ExpiresAt.In(time.Local).Format(time.RFC3339)
	}
	return []string{"NAME", "EXPIRES_AT"}, [][]string{{me.Name, exp}}
}
