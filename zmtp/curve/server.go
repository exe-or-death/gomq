package curve

import (
	"encoding/binary"
	"fmt"
	"net"
	"time"

	"github.com/workspace-9/gomq/zmtp"
	"golang.org/x/crypto/curve25519"
	"golang.org/x/crypto/nacl/box"
	"golang.org/x/crypto/nacl/secretbox"
)

type CurveServer struct {
	pubKey, privKey [32]byte
}

func (c *CurveServer) Handshake(conn net.Conn, meta zmtp.Metadata, verifier zmtp.MetadataVerifier) (
	zmtp.Socket,
	zmtp.Metadata,
	error,
) {
	var nonce Nonce
	var servTransPubKey, servTransSecKey, cookieKey [32]byte
	clientTransPubKey, err := c.doHello(&nonce, conn)
	if err != nil {
		return nil, nil, fmt.Errorf("Client hello failed: %w", err)
	}

	err = c.doWelcome(&nonce, conn, &clientTransPubKey, &cookieKey, &servTransPubKey, &servTransSecKey)
	if err != nil {
		return nil, nil, fmt.Errorf("Failed sending welcome: %w", err)
	}

	clientMeta, err := c.doInitiate(&nonce, conn, &cookieKey, &clientTransPubKey, &servTransSecKey)
	if err != nil {
		return nil, nil, fmt.Errorf("Client initiate failed: %w", err)
	}

	if err := verifier.VerifyMetadata(clientMeta); err != nil {
		return nil, nil, err
	}

	if err := c.doReady(conn, meta, &clientTransPubKey, &servTransSecKey); err != nil {
		return nil, nil, err
	}

	ret := &CurveSocket{nonceIdx: 2, peerNonceIdx: 2, isServ: true, Conn: conn}
	box.Precompute(&ret.sharedKey, &clientTransPubKey, &servTransSecKey)
	return ret, clientMeta, nil
}

func (c *CurveServer) doHello(nonce *Nonce, conn net.Conn) (clientTransPubKey [32]byte, err error) {
	var cmd zmtp.Command
	if _, err = cmd.ReadFrom(conn); err != nil {
		return
	}

	if cmd.Name != "HELLO" {
		err = fmt.Errorf("Invalid handshake: expected hello, got %s", cmd.Name)
		return
	}

	if len(cmd.Body) != 194 {
		err = fmt.Errorf("Invalid hello: expected length to be 194 bytes, got %d", len(cmd.Body))
		return
	}

	if cmd.Body[0] != 1 || cmd.Body[1] != 0 {
		err = fmt.Errorf("Expected CURVEZMQ version 1.0, got %d.%d", cmd.Body[0], cmd.Body[1])
		return
	}

	copy(clientTransPubKey[:], cmd.Body[74:106])
	cliNonceIdx := binary.BigEndian.Uint64(cmd.Body[106:114])
	nonce.Short("CurveZMQHELLO---", cliNonceIdx)
	if cliNonceIdx != 1 {
		err = fmt.Errorf("Expected client nonce to be 1, got %d", cliNonceIdx)
		return
	}
	var out [64]byte
	_, ok := box.Open(out[0:0], cmd.Body[114:], nonce.N(), &clientTransPubKey, &c.privKey)
	if !ok {
		err = fmt.Errorf("Invalid signature in hello command")
		return
	}

	for idx, byte := range out {
		if byte != 0 {
			err = fmt.Errorf("Expected signature to contain only 0's, byte %d has value %x", idx, byte)
			return
		}
	}
	return
}

func (c *CurveServer) doWelcome(
	nonce *Nonce,
	conn net.Conn,
	clientTransPubKey, cookieKey, servTransPubKey, servTransSecKey *[32]byte,
) (
	err error,
) {
	GenerateKeys(servTransPubKey, servTransSecKey)

	// at this point, we welcome
	welcome := zmtp.Command{Name: "WELCOME"}
	welcomeBody := make([]byte, 160)
	welcome.Body = welcomeBody

	var cookie [64]byte
	copy(cookie[:], clientTransPubKey[:])
	copy(cookie[32:], servTransSecKey[:])
	PopulateSecKey(cookieKey)
	if err != nil {
		panic(fmt.Sprintf("Failed creaing cookie key: %s", err.Error()))
	}
	nonce.Long("COOKIE--")
	cookieData := make([]byte, 96)
	secretbox.Seal(cookieData[16:16], cookie[:], nonce.N(), cookieKey)
	copy(cookieData[:16], nonce[8:])

	welcomeBox := make([]byte, 128)
	copy(welcomeBox, servTransPubKey[:])
	copy(welcomeBox[32:], cookieData)
	nonce.Long("WELCOME-")
	copy(welcomeBody, nonce[8:])
	box.Seal(welcomeBody[16:16], welcomeBox, nonce.N(), clientTransPubKey, &c.privKey)
	_, err = welcome.WriteTo(conn)
	return
}

func (c *CurveServer) doInitiate(
	nonce *Nonce,
	conn net.Conn,
	cookieKey, clientTransPubKey, serverTransSecKey *[32]byte,
) (
	clientMeta zmtp.Metadata,
	err error,
) {
	conn.SetDeadline(time.Now().Add(time.Second * 60))
	var init zmtp.Command
	if _, err = init.ReadFrom(conn); err != nil {
		err = fmt.Errorf("Failed reading initiate command: %w", err)
		return
	}
	conn.SetDeadline(time.Time{})

	if init.Name != "INITIATE" {
		err = fmt.Errorf("Expected initiate command, got %s", init.Name)
		return
	}

	if len(init.Body) < 248 {
		err = fmt.Errorf("Expected initiate command to be at least 248 bytes long, was %d", len(init.Body))
		return
	}

	nonce.FromLong("COOKIE--", init.Body[:16])
	clientCookieBox := init.Body[16:96]
	clientCookieData := make([]byte, 0, 64)
	clientCookieData, ok := secretbox.Open(clientCookieData, clientCookieBox, nonce.N(), cookieKey)
	if !ok {
		err = fmt.Errorf("Client sent invalid cookie")
		return
	}

	var serverTransPubKey [32]byte
	copy(serverTransSecKey[:], clientCookieData[32:])
	curve25519.ScalarBaseMult(&serverTransPubKey, serverTransSecKey)

	// second point to check client short nonce
	cliNonceIdx := binary.BigEndian.Uint64(init.Body[96:104])
	if cliNonceIdx != 2 {
		err = fmt.Errorf("Expected client nonce to be 2, got %d", cliNonceIdx)
		return
	}
	nonce.Short("CurveZMQINITIATE", cliNonceIdx)
	initBox := make([]byte, 0, len(init.Body)-120)
	initBox, ok = box.Open(initBox, init.Body[104:], nonce.N(), clientTransPubKey, serverTransSecKey)
	if !ok {
		err = fmt.Errorf("Failed opening initiate box")
		return
	}

	var clientPermPublicKey [32]byte
	copy(clientPermPublicKey[:], initBox[:32])
	vouch := initBox[32:128]
	clientMeta = zmtp.Metadata(initBox[128:])
	nonce.FromLong("VOUCH---", vouch[:16])
	vouchData := make([]byte, 0, 64)
	vouchData, ok = box.Open(vouchData, vouch[16:], nonce.N(), &clientPermPublicKey, serverTransSecKey)
	if !ok {
		err = fmt.Errorf("Failed opening vouch box")
	}
	return
}

func (c *CurveServer) doReady(
	conn net.Conn,
	meta zmtp.Metadata,
	clientTransPubKey *[32]byte,
	serverTransSecKey *[32]byte,
) error {
	var nonce [24]byte
	readyCmd := zmtp.Command{Name: "READY"}
	copy(nonce[:16], "CurveZMQREADY---")
	binary.BigEndian.AppendUint64(nonce[16:16], 1)
	readyBody := make([]byte, len(meta)+16+8)
	box.Seal(readyBody[8:8], meta, &nonce, clientTransPubKey, serverTransSecKey)
	binary.BigEndian.AppendUint64(readyBody[0:0], 1)
	readyCmd.Body = readyBody
	if _, err := readyCmd.WriteTo(conn); err != nil {
		return err
	}

	return nil
}
