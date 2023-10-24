package ipc

import (
	"context"
	"net"
	"net/url"

	"github.com/exe-or-death/gomq"
	"github.com/exe-or-death/gomq/transport"
)

func init() {
	gomq.RegisterTransport("ipc", func() transport.Transport {
		return IPCTransport{}
	})
}

type IPCTransport struct{}

func (IPCTransport) Name() string {
	return "ipc"
}

func (IPCTransport) Bind(url *url.URL) (net.Listener, error) {
	unixAddr, err := net.ResolveUnixAddr("unix", url.Host+url.Path)
	if err != nil {
		return nil, err
	}

	return net.ListenUnix("unix", unixAddr)
}

// Connect to a unix address.
func (IPCTransport) Connect(
	ctx context.Context,
	url *url.URL,
) (
	conn net.Conn,
	fatal bool,
	err error,
) {
	_, err = net.ResolveUnixAddr("unix", url.Host+url.Path)
	if err != nil {
		return nil, true, err
	}

	var d net.Dialer
	conn, err = d.DialContext(ctx, "unix", url.Host+url.Path)
	return conn, false, err
}
