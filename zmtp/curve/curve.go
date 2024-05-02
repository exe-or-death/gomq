package curve

import (
	"fmt"
	"net"

	"github.com/workspace-9/gomq/zmtp"
)

type Curve struct {
	serv *CurveServer
	cli  *CurveClient
}

func (c *Curve) Name() string {
	return MechName
}

func (c *Curve) Server() bool {
	return c.serv != nil
}

func (c *Curve) ValidateGreeting(g *zmtp.Greeting) error {
	if g.Mechanism() != MechName {
		return fmt.Errorf("%w: expected %s", zmtp.ErrMechMismatch, MechName)
	}

	return nil
}

type bothClients struct{}

func (bothClients) Error() string {
	return "Cannot connect two curve clients"
}

var ErrBothClients bothClients

type bothServers struct{}

func (bothServers) Error() string {
	return "Cannot connect two curve servers"
}

var ErrBothServers bothServers

// Handshake performs a null mechanism handshake.
func (c *Curve) Handshake(conn net.Conn, meta zmtp.Metadata, verifier zmtp.MetadataVerifier) (
	zmtp.Socket,
	zmtp.Metadata,
	error,
) {
	if serv := c.serv; serv != nil {
		return serv.Handshake(conn, meta, verifier)
	}

	if c.cli == nil {
		c.cli = &CurveClient{}
	}

	return c.cli.Handshake(conn, meta, verifier)
}
