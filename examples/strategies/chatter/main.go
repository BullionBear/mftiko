// Command chatter writes numbered lines to stdout and stderr, for exercising session logs
// (mftiko session logs, export-logs, the console's Logs panel; E14-08). It places no orders and
// binds nothing.
//
// Params are the strategy's own JSON. The platform does not interpret them.
//
//	{"lines":5000,"rate":0,"bytes":100,"stderr_every":10}
//
// From OnStart, chatter writes stdout lines "chatter <seq> <seq> <padding>", seq from 1, each bytes
// long with its newline. The number is there twice so that a line cut at a rotation and joined to the
// cut end of another is not read as a third one. Every stderr_every-th line also goes to stderr. rate is lines per second; 0 writes
// as fast as it can. lines is how many to write; 0 is no limit. After the last line it keeps running,
// silent, until the session is ended.
package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/BullionBear/mftiko/sdk"
)

// chatterParams is chatter's own configuration. Field names are not a platform contract.
type chatterParams struct {
	Lines       int `json:"lines"`
	Rate        int `json:"rate"`
	Bytes       int `json:"bytes"`
	StderrEvery int `json:"stderr_every"`
}

const (
	defaultBytes = 100
	minBytes     = len("chatter 18446744073709551615 18446744073709551615 \n")
	maxBytes     = 4000 // one write below PIPE_BUF, so a line is never split by another writer
)

type chatter struct {
	p         chatterParams
	out, errw io.Writer
	stop      chan struct{}
	done      sync.WaitGroup
	stopOnce  sync.Once
}

func (c *chatter) OnInit(ctx sdk.Context) error {
	if raw := ctx.Params(); len(raw) > 0 {
		if err := json.Unmarshal(raw, &c.p); err != nil {
			return err
		}
	}
	if c.p.Bytes == 0 {
		c.p.Bytes = defaultBytes
	}
	if c.p.Lines < 0 || c.p.Rate < 0 || c.p.StderrEvery < 0 {
		return fmt.Errorf("lines, rate and stderr_every must not be negative")
	}
	if c.p.Bytes < minBytes || c.p.Bytes > maxBytes {
		return fmt.Errorf("bytes must be between %d and %d", minBytes, maxBytes)
	}
	if c.out == nil {
		c.out = os.Stdout
	}
	if c.errw == nil {
		c.errw = os.Stderr
	}
	c.stop = make(chan struct{})
	return nil
}

func (c *chatter) OnStart(sdk.Context) error {
	c.done.Add(1)
	go c.run()
	return nil
}

func (c *chatter) OnStop(sdk.Context) {
	if c.stop == nil {
		return
	}
	c.stopOnce.Do(func() { close(c.stop) })
	c.done.Wait()
}

// tickEvery is the shortest wait between batches. A ticker much faster than this falls behind, so a
// high rate writes several lines per tick instead.
const tickEvery = 10 * time.Millisecond

func (c *chatter) run() {
	defer c.done.Done()
	var tick <-chan time.Time
	batch := 1
	if c.p.Rate > 0 {
		every := time.Second / time.Duration(c.p.Rate)
		if every < tickEvery {
			every = tickEvery
			batch = c.p.Rate * int(tickEvery) / int(time.Second)
		}
		t := time.NewTicker(every)
		defer t.Stop()
		tick = t.C
	}
	for seq := uint64(1); c.p.Lines == 0 || seq <= uint64(c.p.Lines); seq++ {
		if (seq-1)%uint64(batch) == 0 && !c.wait(tick) {
			return
		}
		b := line(seq, c.p.Bytes)
		_, _ = c.out.Write(b)
		if c.p.StderrEvery > 0 && seq%uint64(c.p.StderrEvery) == 0 {
			_, _ = c.errw.Write(b)
		}
	}
}

// wait blocks until the next tick and reports false once OnStop has been called. Without a rate
// there is no tick: it only checks for the stop.
func (c *chatter) wait(tick <-chan time.Time) bool {
	if tick == nil {
		select {
		case <-c.stop:
			return false
		default:
			return true
		}
	}
	select {
	case <-c.stop:
		return false
	case <-tick:
		return true
	}
}

// line is "chatter <seq> <seq> " padded with dots to n bytes, newline included.
func line(seq uint64, n int) []byte {
	num := strconv.FormatUint(seq, 10)
	head := "chatter " + num + " " + num + " "
	return []byte(head + strings.Repeat(".", n-len(head)-1) + "\n")
}

var _ sdk.Strategy = (*chatter)(nil)

func main() { sdk.Run(&chatter{}) }
