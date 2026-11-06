//go:build !lean

package cmd

import (
	"context"

	cloudlog "go.k6.io/k6/v2/internal/log/cloud"
)

// setupCloudLogPusher registers the cloud log pusher on the logger and starts
// its listener. It runs only for full builds and only when cloud log push is enabled.
func (c *rootCommand) setupCloudLogPusher(stop <-chan struct{}) {
	if !c.enableCloudLogPush {
		return
	}
	p := cloudlog.New(c.globalState.FallbackLogger)
	c.globalState.CloudLogPusher = p
	c.globalState.Logger.AddHook(p)
	pctx, pcancel := context.WithCancel(context.Background())
	c.loggersWg.Go(func() { p.Listen(pctx) })
	c.loggersWg.Go(func() {
		<-stop
		pcancel()
	})
}
