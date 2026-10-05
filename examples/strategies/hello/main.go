// Command hello is the smallest strategy: it logs what it receives. Use it to try a deployment end to end.
package main

import "github.com/BullionBear/mftiko/sdk"

type hello struct{}

func (hello) OnInit(sdk.Context) error             { return nil }
func (hello) OnStart(sdk.Context) error            { return nil }
func (hello) OnStop(sdk.Context)                   {}
func (hello) OnFeed(sdk.Context, *sdk.FeedEvent)   {}
func (hello) OnRecon(sdk.Context, *sdk.ReconState) {}

func main() { sdk.Run(hello{}) }
