package gomq

import (
  "context"
  "sync"

  "github.com/exe-or-death/gomq/zmtp"
  "github.com/exe-or-death/gomq/transport"
)

// SocketDriver represents a type of socket in a communitcation pattern.
type SocketDriver interface {
  // Name of the type.
  Name() string

  // Connect to the remote address using the given transport.
  Connect(tp transport.Transport, addr string) error

  // Bind to the given address using the given transport.
  Bind(tp transport.Transport, addr string) error
  
  // Send a message over the socket.
  Send([]zmtp.Message) error

  // Recv either a command or a message on the socket.
  Recv() ([]zmtp.Message, error)
}

// SocketConstructor constructs a socket.
type SocketConstructor func(
  ctx context.Context,
  mech zmtp.Mechanism, 
  conf *Config, 
  eventBus EventBus,
) (SocketDriver, error)

var registeredTypes struct {
  types map[string]SocketConstructor
  sync.RWMutex
}

func RegisterSocketType(
  name string, 
  constructor SocketConstructor,
) error {
  registeredTypes.Lock()
  defer registeredTypes.Unlock()

  if registeredTypes.types == nil {
    registeredTypes.types = make(map[string]SocketConstructor)
  }

  if _, ok := registeredTypes.types[name]; ok {
    return ErrTypeExists
  }

  registeredTypes.types[name] = constructor
  return nil
}

type typeExists struct{}

func (typeExists) Error() string {
  return "Type exists"
}

var ErrTypeExists typeExists

func FindSocketType(name string) (SocketConstructor, bool) {
  registeredTypes.RLock()
  defer registeredTypes.RUnlock()
  cons, ok := registeredTypes.types[name]
  return cons, ok
}
