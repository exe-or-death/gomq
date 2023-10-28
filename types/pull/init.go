package pull

import (
	"context"

	"github.com/workspace-9/gomq"
	"github.com/workspace-9/gomq/socketutil"
	"github.com/workspace-9/gomq/zmtp"
)

func init() {
	gomq.RegisterSocketType(
		"PULL",
		func(ctx context.Context, mech zmtp.Mechanism, conf *gomq.Config, eventBus gomq.EventBus) (gomq.SocketDriver, error) {
			derived, cancel := context.WithCancel(ctx)
			return &Pull{
				Context:           derived,
				Cancel:            cancel,
				Config:            conf,
				Mech:              mech,
				ConnectionDrivers: map[string]*socketutil.ConnectionDriver{},
				ConnectionHandles: map[string]socketutil.WaitCloser[struct{}]{},
				BindDrivers:       map[string]*socketutil.BindDriver{},
				ReadPoint:         make(chan []zmtp.Message),
				EventBus:          eventBus,
			}, nil
		},
	)
}
