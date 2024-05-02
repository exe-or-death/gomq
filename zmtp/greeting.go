package zmtp

import (
	"fmt"
	"io"
)

// Greeting defines a zmtp greeting
type Greeting [64]byte

// NewGreeting returns a new zmtp greeting.
func NewGreeting() Greeting {
	g := Greeting{}
	g[0] = 0xFF
	g[9] = 0x7F
	return g
}

// VersionMajor returns the major version of this zmtp greeting.
func (g *Greeting) VersionMajor() uint8 {
	return g[10]
}

// SetVersionMajor sets the major version of this zmtp greeting.
func (g *Greeting) SetVersionMajor(major uint8) {
	g[10] = major
}

// VersionMinor returns the minor version of this zmtp greeting.
func (g *Greeting) VersionMinor() uint8 {
	return g[11]
}

// SetVersionMinor sets the minor version of this zmtp greeting.
func (g *Greeting) SetVersionMinor(minor uint8) {
	g[11] = minor
}

// Mechanism returns the mechanism for this Greeting.
func (g *Greeting) Mechanism() string {
	mech := g[12:32]
	for idx, b := range mech {
		if b == 0 {
			return string(mech[:idx])
		}
	}

	return ""
}

// SetMechanism sets the mechanism for this greeting.
func (g *Greeting) SetMechanism(mech string) {
	mechBytes := g[12:32]
	for idx := 0; idx < len(mech); idx++ {
		mechBytes[idx] = mech[idx]
	}

	for idx := len(mech); idx < len(mechBytes); idx++ {
		mechBytes[idx] = 0
	}
}

// Server returns whether the greeting specifies the sender as a server.
func (g *Greeting) Server() bool {
	return g[32] == 1
}

// SetServer sets the server flag in the greeting.
func (g *Greeting) SetServer(server bool) {
	if server {
		g[32] = 1
	} else {
		g[32] = 0
	}
}

// String implements Stringer for greeting.
func (g *Greeting) String() string {
	return fmt.Sprintf("zmtp.Greeting(v=%d.%d, mech=%s, srv=%v)", g.VersionMajor(), g.VersionMinor(), g.Mechanism(), g.Server())
}

// WriteTo writes the greeting to a writer.
func (g *Greeting) WriteTo(w io.Writer) (int64, error) {
	n, err := w.Write(g[:])
	return int64(n), err
}

// ReadFrom reads the greeting from a reader.
func (g *Greeting) ReadFrom(r io.Reader) (int64, error) {
	n, err := io.ReadFull(r, g[:])
	return int64(n), err
}

type mechMismatch struct{}

func (mechMismatch) Error() string {
	return "Mechanism mismatch"
}

var ErrMechMismatch mechMismatch
