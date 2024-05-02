package null

import (
	"github.com/workspace-9/gomq/zmtp"
)

func (n NullSocket) Read() (zmtp.CommandOrMessage, error) {
	var ret zmtp.CommandOrMessage
	_, err := ret.ReadFrom(n.Conn)
	return ret, err
}

func (n NullSocket) SendMessage(m zmtp.Message) error {
	_, err := m.WriteTo(n.Conn)
	return err
}

func (n NullSocket) SendCommand(cmd zmtp.Command) error {
	_, err := cmd.WriteTo(n.Conn)
	return err
}

type invalidFrame struct{}

func (invalidFrame) Error() string {
	return "Invalid frame"
}

var ErrInvalidFrame invalidFrame
