package tcp

import (
	"context"
	"net"
	"net/url"

	"github.com/workspace-9/gomq"
	"github.com/workspace-9/gomq/transport"
)

func init() {
	gomq.RegisterTransport("tcp", func() transport.Transport {
		return Transport{}
	})
}

// Transport implements transport.Transport
type Transport struct{}

// Name of the transport is tcp.
func (Transport) Name() string {
	return "tcp"
}

// Bind to a tcp address.
func (Transport) Bind(url *url.URL) (net.Listener, error) {
	tcpAddr, err := net.ResolveTCPAddr("tcp", url.Host)
	if err != nil {
		return nil, err
	}

	return net.ListenTCP("tcp", tcpAddr)
}

// Connect to a tcp address.
func (Transport) Connect(
	ctx context.Context,
	url *url.URL,
) (
	conn net.Conn,
	fatal bool,
	err error,
) {
	_, err = net.ResolveTCPAddr("tcp", url.Host)
	if err != nil {
		return nil, true, err
	}

	var d net.Dialer
	conn, err = d.DialContext(ctx, "tcp", url.Host)
	return conn, false, err
}
