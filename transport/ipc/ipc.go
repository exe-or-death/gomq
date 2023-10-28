package ipc

import (
	"context"
	"net"
	"net/url"
	"os"

	"github.com/workspace-9/gomq"
	"github.com/workspace-9/gomq/transport"
)

func init() {
	gomq.RegisterTransport("ipc", func() transport.Transport {
		return Transport{}
	})
}

type Transport struct{}

func (Transport) Name() string {
	return "ipc"
}

func (Transport) Bind(url *url.URL) (net.Listener, error) {
	os.Remove(url.Host + url.Path)
	unixAddr, err := net.ResolveUnixAddr("unix", url.Host+url.Path)
	if err != nil {
		return nil, err
	}

	return net.ListenUnix("unix", unixAddr)
}

// Connect to a unix address.
func (Transport) Connect(
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
