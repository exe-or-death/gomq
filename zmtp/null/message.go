package null

import (
	"github.com/workspace-9/gomq/zmtp"
	"io"
)

func (n NullSocket) Read() (zmtp.CommandOrMessage, error) {
	var frameHeader [1]byte
	_, err := io.ReadFull(n.Conn, frameHeader[:])
	if err != nil {
		return zmtp.CommandOrMessage{}, err
	}

	reader := io.MultiReader(ByteReader(frameHeader[0]), n.Conn)
	switch frameHeader[0] {
	case 0x00, 0x01, 0x02, 0x03:
		var m zmtp.Message
		_, err := m.ReadFrom(reader)
		return zmtp.CommandOrMessage{
			IsMessage: true,
			Message:   &m,
		}, err
	case 0x04, 0x06:
		var cmd zmtp.Command
		_, err := cmd.ReadFrom(reader)
		return zmtp.CommandOrMessage{
			IsMessage: false,
			Command:   &cmd,
		}, err
	}

	return zmtp.CommandOrMessage{}, ErrInvalidFrame
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
