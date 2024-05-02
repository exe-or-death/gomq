package zmtp

import (
	"fmt"
	"io"
)

type invalidFrameHeader struct{}

func (invalidFrameHeader) Error() string {
	return "Invalid frame header"
}

var ErrInvalidFrameHeader invalidFrameHeader

func (dat *CommandOrMessage) ReadFrom(r io.Reader) (n int64, err error) {
	var frameHeader [1]byte
	nRead, err := io.ReadFull(r, frameHeader[:])
	if err != nil {
		return int64(nRead), err
	}

	reader := io.MultiReader(ByteReader(frameHeader[0]), r)
	switch frameHeader[0] {
	case 0x00, 0x01, 0x02, 0x03:
		var m Message
		nRead, err := m.ReadFrom(reader)
		if err != nil {
			return int64(nRead), err
		}
		dat.IsMessage = true
		dat.Message = &m
		dat.Command = nil
		return int64(nRead), nil
	case 0x04, 0x06:
		var cmd Command
		nRead, err := cmd.ReadFrom(reader)
		if err != nil {
			return int64(nRead), err
		}
		dat.IsMessage = false
		dat.Command = &cmd
		dat.Message = nil
		return int64(nRead), nil
	}

	return 1, fmt.Errorf("%w: invalid byte %x", ErrInvalidFrameHeader, frameHeader[0])
}

type ByteReader byte

func (b ByteReader) Read(p []byte) (n int, err error) {
	if len(p) == 0 {
		return 0, nil
	}

	p[0] = byte(b)
	return 1, io.EOF
}
