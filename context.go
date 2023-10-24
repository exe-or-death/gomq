package gomq

import (
	"context"
	"fmt"
	"sync"

	"github.com/exe-or-death/gomq/transport"
)

type Context struct {
	sync.RWMutex
	transports map[string]transport.Transport
	ctx        context.Context
}

func NewContext(ctx context.Context) *Context {
	return &Context{
		ctx:        ctx,
		transports: make(map[string]transport.Transport),
	}
}

func (c *Context) getTransport(name string) (transport.Transport, bool) {
	c.Lock()
	defer c.Unlock()

	if tp, ok := c.transports[name]; ok {
		return tp, ok
	}

	registeredTransports.RLock()
	defer registeredTransports.RUnlock()
	if fac, ok := registeredTransports.transports[name]; ok {
		tp := fac()
		c.transports[name] = tp
		return tp, true
	}

	return nil, false
}

func (c *Context) NewSocket(typ string, mech string) (*Socket, error) {
	sock := &Socket{}

	constructor, ok := FindSocketType(typ)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrTypeNotFound, typ)
	}

	mechConstructor, ok := FindMechanism(mech)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrMechanismNotFound, mech)
	}

	conf := &Config{}
	conf.Default()
	driver, err := constructor(c.ctx, mechConstructor(), conf, PrintBus{})
	if err != nil {
		return nil, err
	}

	sock.driver = driver
	sock.ctx = c
	return sock, nil
}

type typeNotFound struct{}

func (typeNotFound) Error() string {
	return "Type not found"
}

var ErrTypeNotFound typeNotFound

type mechanismNotFound struct{}

func (mechanismNotFound) Error() string {
	return "Mechanism not found"
}

var ErrMechanismNotFound mechanismNotFound
