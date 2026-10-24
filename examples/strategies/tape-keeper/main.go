// Command tape-keeper holds its feeds open so the MD tapes stay warm. It places no orders and binds
// no account; the tapes live as long as this session does (DECISIONS E04-D004).
package main

import "github.com/BullionBear/mftiko/sdk"

type keeper struct{}

func (keeper) OnInit(sdk.Context) error  { return nil }
func (keeper) OnStart(sdk.Context) error { return nil }
func (keeper) OnStop(sdk.Context)        {}

func main() { sdk.Run(keeper{}) }
