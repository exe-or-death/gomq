package gomq

import (
	"fmt"
	"sync"

	"github.com/workspace-9/gomq/transport"
)

// TransportFactory creates a transport given a list of key value pairs.
type TransportFactory func() transport.Transport

// registeredTransports contains all registered transports.
var registeredTransports struct {
	transports map[string]TransportFactory
	sync.RWMutex
}

func RegisterTransport(name string, fac TransportFactory) error {
	registeredTransports.Lock()
	defer registeredTransports.Unlock()

	if registeredTransports.transports == nil {
		registeredTransports.transports = make(map[string]TransportFactory)
	}

	if _, ok := registeredTransports.transports[name]; ok {
		return fmt.Errorf("%w: %s", name)
	}

	registeredTransports.transports[name] = fac

	return nil
}

type transportExists struct{}

func (transportExists) Error() string {
	return "Transport already registered"
}

var ErrTransportExists transportExists
