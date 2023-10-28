package socketutil

import (
	"context"
	"net/url"
	"time"

	"github.com/workspace-9/gomq"
	"github.com/workspace-9/gomq/transport"
	"github.com/workspace-9/gomq/zmtp"
)

type SocketHandler func(context.Context, zmtp.Socket) error

type MetadataProvider func() zmtp.Metadata

type MetadataHandler func(zmtp.Metadata) error

type ConnectionDriver struct {
	ctx                context.Context
	mechanism          zmtp.Mechanism
	socket             zmtp.Socket
	transport          transport.Transport
	url                *url.URL
	config             *gomq.Config
	eventBus           gomq.EventBus
	handler            SocketHandler
	meta               MetadataProvider
	metaHandler        MetadataHandler
	cancelFunc         context.CancelFunc
	done               chan struct{}
	lastConnectAttempt time.Time
}

type ConnectionDriverHandle struct {
	Driver *ConnectionDriver
	Queue  chan zmtp.CommandOrMessage
}

func (c *ConnectionDriver) Close() error {
	c.cancelFunc()
	var err error
	if c.socket != nil {
		err = c.socket.Close()
	}
	<-c.done
	return err
}

func (c *ConnectionDriver) TryConnect() (fatal bool, err error) {
	c.lastConnectAttempt = time.Now()
	ctx, cancel := context.WithTimeout(c.ctx, c.config.ConnectTimeout())
	conn, fatal, err := c.transport.Connect(ctx, c.url)
	cancel()
	if err != nil {
		c.eventBus.Post(gomq.Event{
			gomq.EventTypeConnectFailed,
			"",
			c.url.String(),
			err.Error(),
		})
		return fatal, err
	}
	c.eventBus.Post(gomq.Event{
		gomq.EventTypeConnected,
		transport.BuildURL(conn.LocalAddr(), c.transport),
		transport.BuildURL(conn.RemoteAddr(), c.transport),
		"",
	})

	greeting := zmtp.NewGreeting()
	greeting.SetVersionMajor(3)
	greeting.SetVersionMinor(1)
	greeting.SetMechanism(c.mechanism.Name())
	if _, err := greeting.WriteTo(conn); err != nil {
		c.eventBus.Post(gomq.Event{
			gomq.EventTypeFailedGreeting,
			transport.BuildURL(conn.LocalAddr(), c.transport),
			transport.BuildURL(conn.RemoteAddr(), c.transport),
			err.Error(),
		})
		return false, err
	}

	if _, err := greeting.ReadFrom(conn); err != nil {
		c.eventBus.Post(gomq.Event{
			gomq.EventTypeFailedGreeting,
			transport.BuildURL(conn.LocalAddr(), c.transport),
			transport.BuildURL(conn.RemoteAddr(), c.transport),
			err.Error(),
		})
		return false, err
	}

	if err := c.mechanism.ValidateGreeting(&greeting); err != nil {
		c.eventBus.Post(gomq.Event{
			gomq.EventTypeFailedGreeting,
			transport.BuildURL(conn.LocalAddr(), c.transport),
			transport.BuildURL(conn.RemoteAddr(), c.transport),
			err.Error(),
		})
		return false, err
	}

	sock, meta, err := c.mechanism.Handshake(conn, c.meta())
	if err != nil {
		c.eventBus.Post(gomq.Event{
			gomq.EventTypeFailedHandshake,
			transport.BuildURL(conn.LocalAddr(), c.transport),
			transport.BuildURL(conn.RemoteAddr(), c.transport),
			err.Error(),
		})
		return false, err
	}

	if err := c.metaHandler(meta); err != nil {
		c.eventBus.Post(gomq.Event{
			gomq.EventTypeFailedHandshake,
			transport.BuildURL(conn.LocalAddr(), c.transport),
			transport.BuildURL(conn.RemoteAddr(), c.transport),
			err.Error(),
		})
		return false, err
	}

	c.socket = sock
	c.eventBus.Post(gomq.Event{
		gomq.EventTypeReady,
		transport.BuildURL(conn.LocalAddr(), c.transport),
		transport.BuildURL(conn.RemoteAddr(), c.transport),
		"",
	})
	return false, nil
}

func (c *ConnectionDriver) Setup(
	ctx context.Context,
	mech zmtp.Mechanism,
	tp transport.Transport,
	url *url.URL,
	conf *gomq.Config,
	eventBus gomq.EventBus,
	handler SocketHandler,
	meta MetadataProvider,
	metaHandler MetadataHandler,
) {
	derived, cancel := context.WithCancel(ctx)
	c.ctx = derived
	c.cancelFunc = cancel
	c.mechanism = mech
	c.transport = tp
	c.url = url
	c.config = conf
	c.eventBus = eventBus
	c.handler = handler
	c.meta = meta
	c.metaHandler = metaHandler
	c.done = make(chan struct{})
}

func (c *ConnectionDriver) Run() {
	c.run()
	c.cancelFunc()
	close(c.done)
}

func (c *ConnectionDriver) run() error {
	for {
		if err := c.ctx.Err(); err != nil {
			return c.ctx.Err()
		}

		if c.socket == nil {
			if !c.lastConnectAttempt.IsZero() {
				time.Sleep(c.config.ReconnectTimeout() - time.Since(c.lastConnectAttempt))
			}
			_, err := c.TryConnect()
			if err != nil {
				time.Sleep(c.config.ReconnectTimeout() - time.Since(c.lastConnectAttempt))
				continue
			}
		}

		err := c.handler(c.ctx, c.socket)
		if err != nil {
			c.eventBus.Post(gomq.Event{
				gomq.EventTypeDisconnected,
				transport.BuildURL(c.socket.Net().LocalAddr(), c.transport),
				transport.BuildURL(c.socket.Net().RemoteAddr(), c.transport),
				err.Error(),
			})
			time.Sleep(c.config.ReconnectTimeout())
			c.socket = nil
		}
	}
}
