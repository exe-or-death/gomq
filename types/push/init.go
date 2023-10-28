package push

import (
	"context"

	"github.com/workspace-9/gomq"
	"github.com/workspace-9/gomq/socketutil"
	"github.com/workspace-9/gomq/zmtp"
)

func init() {
	gomq.RegisterSocketType(
		"PUSH",
		func(ctx context.Context, mech zmtp.Mechanism, conf *gomq.Config, eventBus gomq.EventBus) (gomq.SocketDriver, error) {
			derived, cancel := context.WithCancel(ctx)
			return &Push{
				Context:           derived,
				Cancel:            cancel,
				Config:            conf,
				Mech:              mech,
				ConnectionDrivers: map[string]*socketutil.ConnectionDriver{},
				BindDrivers:       map[string]*socketutil.BindDriver{},
				ConnectionHandles: map[string]socketutil.WaitCloser[struct{}]{},
				EventBus:          eventBus,
				WritePoint:        make(chan []zmtp.Message),
			}, nil
		},
	)
}
