package conformance

import (
	"context"
	"fmt"
	"strings"

	workerv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/worker/v1"
)

// Config selects the worker and, for tests, which checks to run.
// Worker, when set, is used instead of Command. Only empty means every check.
type Config struct {
	Command Command
	Worker  Worker
	Only    []string
}

// Run starts one embedded server and runs the selected checks (E08-D028–E08-D039).
// A non-nil error means the tool itself could not run (exit 2). Check
// failures are reported inside Report; they are not an error.
func Run(ctx context.Context, cfg Config) (Report, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	w := cfg.Worker
	if w == nil {
		w = cfg.Command
	}
	argv, err := w.Args()
	if err != nil {
		return Report{}, err
	}
	if err := validateOnly(cfg.Only); err != nil {
		return Report{}, err
	}
	b, err := startBus()
	if err != nil {
		return Report{}, err
	}
	defer b.close()
	r := &runner{ctx: ctx, args: argv, bus: b}
	return r.run(selected(cfg.Only)), nil
}

func validateOnly(only []string) error {
	for _, id := range only {
		if !knownID(id) {
			return fmt.Errorf("conformance: unknown check %q", id)
		}
	}
	return nil
}

func selected(only []string) []checkSpec {
	if len(only) == 0 {
		return catalog()
	}
	want := map[string]bool{}
	for _, id := range only {
		want[id] = true
	}
	var out []checkSpec
	for _, s := range catalog() {
		if want[s.ID] {
			out = append(out, s)
		}
	}
	return out
}

type runner struct {
	ctx  context.Context
	args []string
	bus  *bus
}

func (r *runner) run(specs []checkSpec) Report {
	rep := Report{Checks: []Check{}}
	contracts, detail := r.probeContracts()
	for _, spec := range specs {
		if detail != "" {
			rep.Checks = append(rep.Checks, Check{ID: spec.ID, Status: statusFail, Detail: detail})
			continue
		}
		if miss := missingContracts(spec.Needs, contracts); len(miss) > 0 {
			rep.Checks = append(rep.Checks, Check{
				ID:     spec.ID,
				Status: statusSkip,
				Detail: strings.Join(miss, ", ") + " not in contracts",
			})
			continue
		}
		c, err := r.one(spec.ID)
		if err != nil {
			c = Check{Status: statusFail, Detail: err.Error()}
		}
		c.ID = spec.ID
		rep.Checks = append(rep.Checks, c)
	}
	return rep
}

func (r *runner) one(id string) (Check, error) {
	switch id {
	case CheckFlow:
		return r.checkFlow()
	case CheckBeat:
		return r.checkBeat()
	case CheckDeadline:
		return r.checkDeadline()
	case CheckShutdown:
		return r.checkShutdown()
	case CheckBufferPriority:
		return r.checkPriority()
	case CheckPrehook:
		return r.checkPrehook()
	case CheckTDOverflow:
		return r.checkOverflow()
	case CheckCloid:
		return r.checkCloid()
	case CheckSubject:
		return r.checkSubject()
	case CheckRecon:
		return r.checkRecon()
	case CheckCtx:
		return r.checkCtx()
	default:
		return Check{}, fmt.Errorf("conformance: unknown check %q", id)
	}
}

func (r *runner) spawn() (*proc, error) {
	if err := r.ctx.Err(); err != nil {
		return nil, err
	}
	return startProc(r.args, r.bus.workerEnv())
}

// probeContracts reads Hello.contracts from one short-lived process (E08-D038).
func (r *runner) probeContracts() ([]string, string) {
	p, err := r.spawn()
	if err != nil {
		return nil, err.Error()
	}
	defer p.stop()
	h, detail := waitHello(p)
	if h == nil {
		return nil, detail
	}
	return append([]string(nil), h.GetContracts()...), ""
}

func waitHello(p *proc) (*workerv1.Hello, string) {
	var hello *workerv1.Hello
	poll(opTimeout, func() bool {
		s := p.snap()
		if h := s.hello(); h != nil {
			hello = h
			return true
		}
		return s.exited
	})
	if hello != nil {
		return hello, ""
	}
	s := p.snap()
	if s.exited {
		return nil, fmt.Sprintf("worker exited %d before Hello", s.code)
	}
	return nil, "before Hello"
}

func missingContracts(needs, have []string) []string {
	got := map[string]bool{}
	for _, h := range have {
		got[h] = true
	}
	var miss []string
	for _, n := range needs {
		if !got[n] {
			miss = append(miss, n)
		}
	}
	return miss
}

func pass() Check { return Check{Status: statusPass} }

func fail(detail string) Check { return Check{Status: statusFail, Detail: detail} }
