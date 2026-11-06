//go:build !lean

package state

import cloudlog "go.k6.io/k6/v2/internal/log/cloud"

type cloudLogPusher = *cloudlog.Pusher
