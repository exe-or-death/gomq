package gomq

import (
	"net/url"

	"github.com/workspace-9/gomq/zmtp"
)

type Socket struct {
	driver SocketDriver
	ctx    *Context
}

func (s Socket) Connect(addr string) error {
	url, err := url.Parse(addr)
	if err != nil {
		return err
	}

	tp, ok := s.ctx.getTransport(url.Scheme)
	if !ok {
		return ErrTransportNotFound
	}

	if err := s.driver.Connect(tp, url); err != nil {
		return err
	}

	return nil
}

type transportNotFound struct{}

func (transportNotFound) Error() string {
	return "Transport not found"
}

var ErrTransportNotFound transportNotFound

func (s Socket) Bind(addr string) error {
	url, err := url.Parse(addr)
	if err != nil {
		return err
	}

	tp, ok := s.ctx.getTransport(url.Scheme)
	if !ok {
		return ErrTransportNotFound
	}

	if err := s.driver.Bind(tp, url); err != nil {
		return err
	}

	return nil
}

func (s Socket) Send(data [][]byte) error {
	messages := make([]zmtp.Message, len(data))
	for idx, datum := range data {
		messages[idx] = zmtp.Message{
			More: idx != len(data)-1, Body: datum,
		}
	}

	return s.driver.Send(messages)
}

func (s Socket) Recv() ([][]byte, error) {
	messages, err := s.driver.Recv()
	if err != nil {
		return nil, err
	}

	data := make([][]byte, len(messages))
	for idx, message := range messages {
		data[idx] = message.Body
	}

	return data, nil
}

func (s Socket) Close() error {
	return s.driver.Close()
}

func (s Socket) Disconnect(addr string) error {
	url, err := url.Parse(addr)
	if err != nil {
		return err
	}
	return s.driver.Disconnect(url)
}

func (s Socket) Unbind(addr string) error {
	url, err := url.Parse(addr)
	if err != nil {
		return err
	}
	return s.driver.Unbind(url)
}
