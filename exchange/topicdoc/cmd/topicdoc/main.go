// Command topicdoc rewrites the venue × topic table in docs/PROTOCOL.md (E04-D062).
package main

import (
	"fmt"
	"os"

	"github.com/BullionBear/mftiko/exchange/topicdoc"
)

func main() {
	if err := topicdoc.WriteProtocol(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
