package sdk

import (
	"time"

	commonv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/common/v1"
	stsctlv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/stsctl/v1"
)

const reportInterval = 5 * time.Second

// reportLoop sends stsctl.Report from a goroutine the hooks do not block (PROTOCOL §9.2).
// The first report goes out as soon as the loop starts. A later one goes out when the
// applied binding version changes, and at least every five seconds even when it did not.
// Queue stats are read only for a report that is actually sent: Stats clears the max age.
func (p *plane) reportLoop() {
	defer close(p.repDone)
	every := p.host.reportEvery
	if every <= 0 {
		every = reportInterval
	}
	p.sendReport()
	timer := p.reportAfter(every)
	for {
		select {
		case <-p.root.Done():
			return
		case <-timer:
			p.sendReport()
			timer = p.reportAfter(every)
		case <-p.wake:
			if !p.appliedChanged() {
				continue
			}
			p.sendReport()
		}
	}
}

func (p *plane) reportAfter(d time.Duration) <-chan time.Time {
	if p.host.reportAfter != nil {
		return p.host.reportAfter(d)
	}
	return time.After(d)
}

func (p *plane) wakeReport() {
	if p.wake == nil {
		return
	}
	select {
	case p.wake <- struct{}{}:
	default:
	}
}

func (p *plane) appliedChanged() bool {
	if p.md == nil {
		return false
	}
	epoch, seq, ok := p.md.Applied()
	p.repMu.Lock()
	defer p.repMu.Unlock()
	return ok != p.lastOK || epoch != p.lastEpoch || seq != p.lastSeq
}

func (p *plane) sendReport() {
	if p.sess == nil || p.md == nil || p.disp == nil {
		return
	}
	epoch, seq, ok := p.md.Applied()
	p.repMu.Lock()
	p.lastEpoch, p.lastSeq, p.lastOK = epoch, seq, ok
	p.repMu.Unlock()
	td, md, blocked, slow := p.disp.Stats()
	rep := &stsctlv1.Report{
		Hook:          p.hookName(),
		HookBlockedMs: blocked,
		HookSlow:      slow,
		Queues: []*stsctlv1.QueueStat{
			{Name: "td", Depth: td.Depth, Dropped: td.Dropped, MaxAgeMs: td.MaxAgeMs},
			{Name: "md", Depth: md.Depth, Dropped: md.Dropped, MaxAgeMs: md.MaxAgeMs},
		},
	}
	if ok {
		rep.BindingsApplied = &commonv1.Version{Epoch: epoch, Seq: seq}
	}
	_ = p.sess.Report(rep)
}

func (p *plane) hookName() string {
	v := p.hook.Load()
	s, _ := v.(string)
	return s
}
