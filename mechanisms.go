package gomq

import (
	"github.com/workspace-9/gomq/zmtp"
	"sync"
)

var registeredMechanisms struct {
	mechanisms map[string]func() zmtp.Mechanism
	sync.RWMutex
}

func RegisterMechanism(name string, mech func() zmtp.Mechanism) error {
	registeredMechanisms.Lock()
	defer registeredMechanisms.Unlock()

	if registeredMechanisms.mechanisms == nil {
		registeredMechanisms.mechanisms = make(map[string]func() zmtp.Mechanism)
	}

	if _, ok := registeredMechanisms.mechanisms[name]; ok {
		return ErrMechanismExists
	}

	registeredMechanisms.mechanisms[name] = mech
	return nil
}

type mechanismExists struct{}

func (mechanismExists) Error() string {
	return "Mechanism already registered"
}

var ErrMechanismExists mechanismExists

func FindMechanism(name string) (func() zmtp.Mechanism, bool) {
	registeredMechanisms.RLock()
	defer registeredMechanisms.RUnlock()
	mech, ok := registeredMechanisms.mechanisms[name]
	return mech, ok
}
