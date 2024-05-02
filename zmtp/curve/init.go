package curve

import (
	"github.com/workspace-9/gomq"
	"github.com/workspace-9/gomq/zmtp"
)

const MechName = "CURVE"

func init() {
	gomq.RegisterMechanism(MechName, func() zmtp.Mechanism {
		return &Curve{}
	})
}
