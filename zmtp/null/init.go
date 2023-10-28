package null

import (
	"github.com/workspace-9/gomq"
	"github.com/workspace-9/gomq/zmtp"
)

func init() {
	gomq.RegisterMechanism("NULL", func() zmtp.Mechanism {
		return Null{}
	})
}
