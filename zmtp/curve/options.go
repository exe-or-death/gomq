package curve

import (
	"fmt"

	"github.com/workspace-9/gomq/zmtp"
	"golang.org/x/crypto/curve25519"
)

func (c *Curve) SetOption(option string, val any) error {
	switch option {
	case zmtp.OptionServer:
		serv, ok := val.(bool)
		if !ok {
			return fmt.Errorf("Value for option %s must be bool, got %T", option, val)
		}

		if serv {
			c.SetupServer()
		} else {
			c.SetupClient()
		}
	case zmtp.OptionPubKey:
		var byteData []byte
		byteData, ok := val.([]byte)
		if !ok {
			strData, ok := val.(string)
			if !ok {
				return fmt.Errorf("Value for option %s must be string or []byte, got %T", option, val)
			}
			byteData = []byte(strData)
		}

		if len(byteData) != 32 {
			return fmt.Errorf("Key must be 32 bytes, got %d", len(byteData))
		}

		if c.serv != nil {
			copy(c.serv.pubKey[:], byteData)
		} else {
			if c.cli == nil {
				c.cli = &CurveClient{}
			}
			copy(c.cli.pubKey[:], byteData)
		}
	case zmtp.OptionSecKey:
		var byteData []byte
		byteData, ok := val.([]byte)
		if !ok {
			strData, ok := val.(string)
			if !ok {
				return fmt.Errorf("Value for option %s must be string or []byte, got %T", option, val)
			}
			byteData = []byte(strData)
		}

		if len(byteData) != 32 {
			return fmt.Errorf("Key must be 32 bytes, got %d", len(byteData))
		}

		var pubKey, secKey *[32]byte
		if c.serv != nil {
			secKey = &c.serv.privKey
			pubKey = &c.serv.pubKey
		} else {
			if c.cli == nil {
				c.cli = &CurveClient{}
			}
			pubKey = &c.cli.pubKey
			secKey = &c.cli.privKey
		}

		copy(secKey[:], byteData)
		if isEmpty(pubKey) {
			curve25519.ScalarBaseMult(pubKey, secKey)
		}
	case zmtp.OptionSrvKey:
		var byteData []byte
		byteData, ok := val.([]byte)
		if !ok {
			strData, ok := val.(string)
			if !ok {
				return fmt.Errorf("Value for option %s must be string or []byte, got %T", option, val)
			}
			byteData = []byte(strData)
		}

		if len(byteData) != 32 {
			return fmt.Errorf("Key must be 32 bytes, got %d", len(byteData))
		}

		if c.serv != nil {
			return fmt.Errorf("Cannot set server key on curve server (set OptionSecKey to set the private key for the server)")
		}

		if c.cli == nil {
			c.cli = &CurveClient{}
		}
		copy(c.cli.serverPubKey[:], byteData)
	}

	return nil
}

func (c *Curve) SetupServer() {
	c.serv = &CurveServer{}
	c.cli = nil
}

func (c *Curve) SetupClient() {
	c.serv = nil
	c.cli = &CurveClient{}
}
