// Command hello is the smallest strategy: it takes a few hooks and does nothing with them. Use it to try
// a deployment end to end.
package main

import "github.com/BullionBear/mftiko/sdk"

type hello struct{}

func (hello) OnInit(sdk.Context) error               { return nil }
func (hello) OnStart(sdk.Context) error              { return nil }
func (hello) OnStop(sdk.Context)                     {}
func (hello) OnBestQuote(sdk.Context, sdk.BestQuote) {}
func (hello) OnRecon(sdk.Context, sdk.ReconEvent)    {}
func (hello) OnOrder(sdk.Context, sdk.OrderEvent)    {}

// The hooks hello takes, checked at compile time.
var (
	_ sdk.Strategy         = hello{}
	_ sdk.BestQuoteHandler = hello{}
	_ sdk.ReconHandler     = hello{}
	_ sdk.OrderHandler     = hello{}
)

func main() { sdk.Run(hello{}) }
