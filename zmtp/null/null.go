package null

import (
	"fmt"
	"github.com/workspace-9/gomq/zmtp"
	"net"
)

type Null struct{}

func (Null) Name() string {
	return "NULL"
}

func (Null) Server() bool {
	return false
}

// ValidateGreeting ensures the other side is not a server.
func (Null) ValidateGreeting(g *zmtp.Greeting) error {
	if g.Server() {
		return ErrCannotBeServer
	}

	return nil
}

type cannotBeServer struct{}

func (cannotBeServer) Error() string {
	return "Cannot be server"
}

var ErrCannotBeServer cannotBeServer

// Handshake performs a null mechanism handshake.
func (Null) Handshake(conn net.Conn, meta zmtp.Metadata) (
	zmtp.Socket,
	zmtp.Metadata,
	error,
) {
	var cmd zmtp.Command
	cmd.Name = "READY"
	cmd.Body = meta
	if _, err := cmd.WriteTo(conn); err != nil {
		return nil, nil, err
	}

	if _, err := cmd.ReadFrom(conn); err != nil {
		return nil, nil, err
	}

	if cmd.Name != "READY" {
		return nil, nil, fmt.Errorf("%w: received %s", ErrNotReady, cmd.Name)
	}

	return NullSocket{conn}, zmtp.Metadata(cmd.Body), nil
}

type notReady struct{}

func (notReady) Error() string {
	return "Failed receiving ready command"
}

var ErrNotReady notReady

func (n Null) SetOption(string, string) {}

type NullSocket struct {
	net.Conn
}

// Net returns the underlying net.Conn for the socket.
func (n NullSocket) Net() net.Conn {
	return n.Conn
}
