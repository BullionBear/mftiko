package sdk

import (
	"errors"
	"path/filepath"
	"runtime/debug"
	"strings"
	"time"

	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	stsctlv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/stsctl/v1"
	"github.com/BullionBear/mftiko/contract/symbol"
	"github.com/BullionBear/mftiko/sdk/eventlog"
	"github.com/BullionBear/mftiko/sdk/internal/dispatch"
	"github.com/BullionBear/mftiko/sdk/internal/evlog"
	"github.com/BullionBear/mftiko/sdk/internal/symclient"
)

// The plane writes the frames that do not pass the dispatcher: init, start, bindings, resolve, and
// an OnStop the dispatcher does not run (DECISIONS E20-D003, PROTOCOL §12). The dispatcher, tdpath
// and mdpath write the rest. With the log off p.rec is nil and none of this runs (E20-D007).

const (
	// sdkModule is the module the SDK is part of. The file header carries its version and commit.
	sdkModule = "github.com/BullionBear/mftiko"
	// natsField is InitPayload.nats. The credentials are not written to the log.
	natsField protowire.Number = 9
	// bindingsField is Command.bindings.
	bindingsField protowire.Number = 1
)

var sdkVersion, sdkCommit = sdkParts()

func sdkParts() (version, commit string) {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return "", ""
	}
	return sdkRelease(info)
}

// sdkRelease is the version and commit of the SDK module the binary was built with. A binary built
// in this repository is the module itself. A strategy in its own module lists the SDK among its
// dependencies, and a pseudo-version carries the commit.
func sdkRelease(info *debug.BuildInfo) (version, commit string) {
	if info == nil {
		return "", ""
	}
	if info.Main.Path == sdkModule {
		return versionCommit(info)
	}
	for _, m := range info.Deps {
		if m == nil || m.Path != sdkModule {
			continue
		}
		if m.Replace != nil {
			m = m.Replace
		}
		return m.Version, pseudoCommit(m.Version)
	}
	return "", ""
}

// pseudoCommit is the commit a pseudo-version ends with, such as v0.0.0-20261031090000-0a1b2c3d4e5f.
// A tagged version has none.
func pseudoCommit(v string) string {
	v, _ = strings.CutSuffix(v, "+incompatible")
	i := strings.LastIndexByte(v, '-')
	if i < 0 {
		return ""
	}
	rev := v[i+1:]
	stamp := v[:i]
	if j := strings.LastIndexAny(stamp, "-."); j >= 0 {
		stamp = stamp[j+1:]
	}
	if len(rev) != 12 || strings.Trim(rev, "0123456789abcdef") != "" ||
		len(stamp) != 14 || strings.Trim(stamp, "0123456789") != "" {
		return ""
	}
	return rev
}

// openRec starts the writer for this worker. Files go to VolumeDir()/eventlog; without a volume
// directory the writer counts every frame as dropped and says so once (E20-D006).
func (p *plane) openRec(init *stsctlv1.InitPayload) *evlog.Log {
	dir := ""
	if p.volumeDir != "" {
		dir = filepath.Join(p.volumeDir, eventlog.Dir)
	}
	return evlog.Open(evlog.Config{
		Dir: dir,
		Header: eventlog.Header{
			Incarnation: init.GetIncarnation(),
			SessionID:   init.GetSessionId(),
			SDKVersion:  sdkVersion,
			SDKCommit:   sdkCommit,
		},
		KeepBytes: int64(init.GetEventLog().GetKeepMb()) << 20,
		Now:       p.host.now,
		Log:       p.note,
	})
}

// now is the host clock. Context.Now is the step clock instead.
func (p *plane) now() time.Time {
	if p.host.now != nil {
		return p.host.now()
	}
	return time.Now()
}

// stepNow is the step clock: when the hook that is running started (E20-D004).
func (p *plane) stepNow() time.Time { return time.Unix(0, p.clock.Load()).UTC() }

// outside runs a hook the dispatcher does not run: OnInit, OnStart, and OnStop after a Start that
// failed. It sets the step clock to the hook's start. With the log on it writes the input frame
// first and a hook_end after. c4 does not recover a panic here, and the process ends with it: the
// hook_end with the panic is written and the ring flushed before the panic goes on (E20-D005).
func (p *plane) outside(name string, kind eventlog.Kind, recvNs int64, ref uint64, head, body []byte, fn func() error) error {
	start := p.now().UnixNano()
	p.clock.Store(start)
	p.hook.Store(name)
	defer p.hook.Store("")
	if p.rec == nil {
		return fn()
	}
	seq := p.rec.Append(kind, 0, recvNs, start, ref, head, body)
	returned := false
	defer func() {
		if returned {
			return
		}
		// recover is nil when fn called runtime.Goexit: that is not a hook ending.
		if r := recover(); r != nil {
			flags, text := dispatch.EndOf(r, nil)
			p.rec.Append(eventlog.KindHookEnd, flags, 0, p.now().UnixNano(), seq, text, nil)
			p.rec.Close()
			panic(r)
		}
	}()
	err := fn()
	returned = true
	flags, text := dispatch.EndOf(nil, err)
	p.rec.Append(eventlog.KindHookEnd, flags, 0, p.now().UnixNano(), seq, text, nil)
	return err
}

// resolve is Context.Resolve. With the log on each call is a resolve frame: the ticker, and the
// SymbolInfo or the error. A replay answers from those frames instead of SYM.
func (p *plane) resolve(ticker string) (symbol.Info, error) {
	var callNs int64
	if p.rec != nil {
		callNs = p.now().UnixNano()
	}
	info, err := symbol.Info{}, ErrStopping
	if p.sym != nil {
		info, err = p.sym.Resolve(p.resolveCtx(), ticker)
		err = asSDK(err)
	}
	if p.rec != nil {
		p.recordResolve(ticker, callNs, info, err)
	}
	return info, err
}

func (p *plane) recordResolve(ticker string, callNs int64, info symbol.Info, err error) {
	flags, text := resolveOutcome(err)
	rest := []byte(text)
	if err == nil {
		b, merr := proto.Marshal(symclient.InfoToProto(info))
		if merr != nil {
			flags, rest = eventlog.Failed, []byte(merr.Error())
		} else {
			rest = b
		}
	}
	p.rec.Append(eventlog.KindResolve, flags, p.now().UnixNano(), callNs, 0, eventlog.AppendResolve(nil, ticker, nil), rest)
}

// resolveOutcome is the flags of a resolve frame. NotInInit, NotFound and Stopping are the SDK's
// own answers; the rest classify the request to SYM as order_reply does.
func resolveOutcome(err error) (uint16, string) {
	switch {
	case err == nil:
		return eventlog.OK, ""
	case errors.Is(err, ErrNotInInit):
		return eventlog.NotInInit, err.Error()
	case errors.Is(err, symbol.ErrNotFound):
		return eventlog.NotFound, err.Error()
	case errors.Is(err, ErrStopping):
		return eventlog.Stopping, err.Error()
	}
	return evlog.Outcome(err)
}

// resolveErr is the error a replayed Resolve returns for a recorded outcome.
func resolveErr(flags uint16, text []byte) error {
	switch flags {
	case eventlog.NotInInit:
		return evlog.Wrap(string(text), ErrNotInInit)
	case eventlog.NotFound:
		return evlog.Wrap(string(text), symbol.ErrNotFound)
	case eventlog.Stopping:
		return evlog.Wrap(string(text), ErrStopping)
	}
	return evlog.Err(flags, text)
}

// recordBindings writes a Bindings snapshot that was applied. recvNs is when it reached the
// worker; raw is the stsctl.Bindings as received.
func (p *plane) recordBindings(recvNs int64, flags uint16, raw []byte) {
	if p.rec == nil {
		return
	}
	p.rec.Append(eventlog.KindBindings, flags, recvNs, p.now().UnixNano(), 0, raw, nil)
}

// relayed is one Command waiting for the command loop, and when it arrived when the log is on.
type relayed struct {
	at int64
	b  []byte
}

// withoutField is the message b with every occurrence of field num left out. Init uses it to keep
// the NATS credentials out of the log. b has already been unmarshaled, so it parses.
func withoutField(b []byte, num protowire.Number) []byte {
	out := make([]byte, 0, len(b))
	for len(b) > 0 {
		n, typ, l := protowire.ConsumeTag(b)
		if l < 0 {
			return out
		}
		m := protowire.ConsumeFieldValue(n, typ, b[l:])
		if m < 0 {
			return out
		}
		if n != num {
			out = append(out, b[:l+m]...)
		}
		b = b[l+m:]
	}
	return out
}

// fieldBytes is the bytes of the message field num of b, as they are in b.
func fieldBytes(b []byte, num protowire.Number) ([]byte, bool) {
	var out []byte
	found := false
	for len(b) > 0 {
		n, typ, l := protowire.ConsumeTag(b)
		if l < 0 {
			return nil, false
		}
		b = b[l:]
		if n == num && typ == protowire.BytesType {
			v, m := protowire.ConsumeBytes(b)
			if m < 0 {
				return nil, false
			}
			out, found = v, true
			b = b[m:]
			continue
		}
		m := protowire.ConsumeFieldValue(n, typ, b)
		if m < 0 {
			return nil, false
		}
		b = b[m:]
	}
	return out, found
}

// eventLogStat is Report.event_log: present only while the log is on (E20-D007).
func (p *plane) eventLogStat() *stsctlv1.EventLogStat {
	if p.rec == nil {
		return nil
	}
	records, dropped := p.rec.Stats()
	return &stsctlv1.EventLogStat{Records: records, Dropped: dropped}
}
