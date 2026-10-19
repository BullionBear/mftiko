package sdk

import "strings"

// Contract majors a session worker implements (PROTOCOL §9.2). helloContracts is
// that list. The same constants are concatenated into contractsMarker, which
// mftiko-pack reads back out of the strategy binary (D935). The C5 contract
// checked on Init, Start, and Live stays stsctl.v1.
const (
	contractWorker = "worker.v1"
	contractSTS    = "stsctl.v1"
	contractMD     = "md.v1"
	contractTD     = "td.v1"
)

var helloContracts = []string{contractWorker, contractSTS, contractMD, contractTD}

// contractsMarker is the contiguous byte sequence embedded in every binary that
// links this package (D935). The recognizable text is
// mftiko.contracts=worker.v1,stsctl.v1,md.v1,td.v1 and a newline ends it: the
// following bytes in the file must not be able to lengthen a name.
const contractsMarker = "mftiko.contracts=" + contractWorker + "," + contractSTS + "," + contractMD + "," + contractTD + "\n"

// embeddedContracts is a variable, not only a const, so the linker keeps the
// marker in the binary. init reads it.
var embeddedContracts = contractsMarker

func init() {
	got := strings.TrimSuffix(strings.TrimPrefix(embeddedContracts, "mftiko.contracts="), "\n")
	if got != strings.Join(helloContracts, ",") {
		panic("sdk: contracts marker drifted from helloContracts")
	}
}

// Contracts returns a copy of the contract majors this SDK embeds and Hello
// sends (D939). Changing the returned slice does not change the SDK's list.
func Contracts() []string {
	out := make([]string, len(helloContracts))
	copy(out, helloContracts)
	return out
}
