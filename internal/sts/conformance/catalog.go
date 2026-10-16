package conformance

// Check ids. They are the cross-language contract in PROTOCOL §9.8 (D838).
// fakeworker repeats these strings; it cannot import this package.
const (
	CheckFlow           = "flow"
	CheckBeat           = "beat"
	CheckDeadline       = "deadline"
	CheckShutdown       = "shutdown"
	CheckBufferPriority = "buffer_priority"
	CheckPrehook        = "prehook"
	CheckTDOverflow     = "td_overflow"
	CheckCloid          = "cloid"
	CheckSubject        = "subject"
	CheckRecon          = "recon"
	CheckCtx            = "ctx"
)

// Contract majors a check reads from Hello.contracts (D838).
const (
	contractWorker = "worker.v1"
	contractSTS    = "stsctl.v1"
	contractMD     = "md.v1"
	contractTD     = "td.v1"
)

type checkSpec struct {
	ID    string
	Needs []string
}

// catalog is the suite order. PROTOCOL §9.8 lists the same ids.
func catalog() []checkSpec {
	return []checkSpec{
		{ID: CheckFlow, Needs: []string{contractWorker}},
		{ID: CheckBeat, Needs: []string{contractWorker}},
		{ID: CheckDeadline, Needs: []string{contractWorker}},
		{ID: CheckShutdown, Needs: []string{contractWorker}},
		{ID: CheckBufferPriority, Needs: []string{contractMD, contractTD}},
		{ID: CheckPrehook, Needs: []string{contractMD}},
		{ID: CheckTDOverflow, Needs: []string{contractTD}},
		{ID: CheckCloid, Needs: []string{contractTD}},
		{ID: CheckSubject, Needs: []string{contractSTS, contractMD, contractTD}},
		{ID: CheckRecon, Needs: []string{contractTD}},
		{ID: CheckCtx, Needs: []string{contractMD, contractTD}},
	}
}

// CheckIDs is the check ids in suite order.
func CheckIDs() []string {
	all := catalog()
	out := make([]string, len(all))
	for i, s := range all {
		out[i] = s.ID
	}
	return out
}

func knownID(id string) bool {
	for _, s := range catalog() {
		if s.ID == id {
			return true
		}
	}
	return false
}
