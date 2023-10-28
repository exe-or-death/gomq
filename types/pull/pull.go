package pull

import (
	"context"
	"fmt"
	"net/url"

	"github.com/workspace-9/gomq"
	"github.com/workspace-9/gomq/socketutil"
	"github.com/workspace-9/gomq/transport"
	"github.com/workspace-9/gomq/types"
	"github.com/workspace-9/gomq/zmtp"
)

// Pull implements the zmq pull socket.
type Pull struct {
	context.Context
	Cancel context.CancelFunc
	*gomq.Config
	Mech              zmtp.Mechanism
	ConnectionDrivers map[string]*socketutil.ConnectionDriver
	ConnectionHandles map[string]socketutil.WaitCloser[struct{}]
	BindDrivers       map[string]*socketutil.BindDriver
	ReadPoint         chan []zmtp.Message
	EventBus          gomq.EventBus
}

func (p *Pull) Name() string {
	return "PULL"
}

func (p *Pull) Connect(tp transport.Transport, url *url.URL) error {
	if _, ok := p.ConnectionHandles[url.String()]; ok {
		return fmt.Errorf("%w: %s", types.ErrAlreadyConnected, url)
	}

	var queue chan zmtp.Message
	driver := &socketutil.ConnectionDriver{}
	driver.Setup(
		p.Context,
		p.Mech,
		tp,
		url,
		p.Config,
		p.EventBus,
		func(ctx context.Context, s zmtp.Socket) error {
			return HandleSock(ctx, s, queue)
		},
		p.Meta,
		p.MetaHandler,
	)
	fatal, err := driver.TryConnect()
	if err != nil && fatal {
		return err
	}
	queue = make(chan zmtp.Message, p.Config.QueueLen())
	wc := socketutil.NewWaitCloser[struct{}](p.Context)
	go PushIntoReadPoint(&wc, queue, p.ReadPoint)
	p.ConnectionDrivers[url.String()] = driver
	p.ConnectionHandles[url.String()] = wc
	go driver.Run()
	return nil
}

func (p *Pull) Disconnect(url *url.URL) error {
	driver, ok := p.ConnectionDrivers[url.String()]
	if !ok {
		return fmt.Errorf("%w to %s", types.ErrNeverConnected, url)
	}

	delete(p.ConnectionDrivers, url.String())
	err := driver.Close()
	p.ConnectionHandles[url.String()].Close()
	delete(p.ConnectionHandles, url.String())
	return err
}

func (p *Pull) Bind(tp transport.Transport, url *url.URL) error {
	if _, ok := p.BindDrivers[url.String()]; ok {
		return fmt.Errorf("%w: %s", types.ErrAlreadyBound, url)
	}

	driver := &socketutil.BindDriver{}
	driver.Setup(
		p.Context,
		tp,
		p.Mech,
		url,
		func(ctx context.Context, s zmtp.Socket) error {
			queue := make(chan zmtp.Message, p.Config.QueueLen())
			wc := socketutil.NewWaitCloser[struct{}](p.Context)
			defer wc.Finish(struct{}{})
			go PushIntoReadPoint(&wc, queue, p.ReadPoint)
			return HandleSock(ctx, s, queue)
		},
		p.EventBus,
		p.Meta,
		p.MetaHandler,
	)
	if err := driver.TryBind(); err != nil {
		return err
	}
	p.BindDrivers[url.String()] = driver
	go driver.Run()
	return nil
}

func (p *Pull) Unbind(url *url.URL) error {
	driver, ok := p.BindDrivers[url.String()]
	if !ok {
		return fmt.Errorf("%w to %s", types.ErrNeverBound, url.String())
	}

	delete(p.BindDrivers, url.String())
	err := driver.Close()
	return err
}

func PushIntoReadPoint(wc *socketutil.WaitCloser[struct{}], pull <-chan zmtp.Message, readPoint chan []zmtp.Message) {
	defer wc.Finish(struct{}{})
	built := make([]zmtp.Message, 0)
	for {
		select {
		case part := <-pull:
			built = append(built, part)
			if !part.More {
				select {
				case readPoint <- built:
					built = make([]zmtp.Message, 0)
				case <-wc.Done():
					return
				}
			}
		case <-wc.Done():
			return
		}
	}
}

func HandleSock(ctx context.Context, sock zmtp.Socket, queue chan<- zmtp.Message) (err error) {
	for {
		next, err := sock.Read()
		if err != nil {
			return err
		}

		if !next.IsMessage {
			continue
		}

		select {
		case queue <- *next.Message:
		case <-ctx.Done():
		}
	}
}

func (p *Pull) Meta() zmtp.Metadata {
	meta := zmtp.Metadata{}
	meta.AddProperty("Socket-Type", "PULL")
	return meta
}

func (p *Pull) MetaHandler(meta zmtp.Metadata) error {
	var err error
	meta.Properties(func(name string, value string) {
		if name == "Socket-Type" && err == nil {
			if value != "PUSH" {
				err = fmt.Errorf("Expected push socket to connect, got %s", value)
			}
		}
	})

	return err
}

func (p *Pull) Send([]zmtp.Message) error {
	return types.ErrOperationNotPermitted
}

func (p *Pull) Recv() ([]zmtp.Message, error) {
	select {
	case msg := <-p.ReadPoint:
		return msg, nil
	case <-p.Context.Done():
		return nil, p.Context.Err()
	}
}

func (p *Pull) Close() error {
	p.Cancel()
	for _, conn := range p.ConnectionDrivers {
		conn.Close()
	}
	for _, bind := range p.BindDrivers {
		bind.Close()
	}
	return nil
}
