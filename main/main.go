package main

import (
	"context"
	"log"
	"time"

	"github.com/pebbe/zmq4"
	"github.com/workspace-9/gomq"
	_ "github.com/workspace-9/gomq/transport/tcp"
	_ "github.com/workspace-9/gomq/types/pull"
	_ "github.com/workspace-9/gomq/types/push"
	"github.com/workspace-9/gomq/zmtp"
	_ "github.com/workspace-9/gomq/zmtp/curve"
)

func main() {
	srvPub, srvPriv, _ := zmq4.NewCurveKeypair()
	cliPub, cliPriv, _ := zmq4.NewCurveKeypair()
	go runPullSock(srvPub, cliPub, cliPriv)
	runPebbePushSock(srvPub, srvPriv)
}

func runPullSock(
	srvPub, cliPub, cliPriv string,
) {
	srvPub, cliPriv = zmq4.Z85decode(srvPub), zmq4.Z85decode(cliPriv)
	ctx := gomq.NewContext(context.Background())
	sock, err := ctx.NewSocket("PULL", "CURVE")
	if err != nil {
		log.Fatalf("Failed creating socket: %s", err.Error())
	}

	if err := sock.SetOption(zmtp.OptionServer, false); err != nil {
		log.Fatalf("Failed setting socket role as client: %s", err.Error())
	}

	if err := sock.SetOption(zmtp.OptionSecKey, cliPriv); err != nil {
		log.Fatalf("Failed creating socket: %s", err.Error())
	}

	if err := sock.SetOption(zmtp.OptionSrvKey, srvPub); err != nil {
		log.Fatalf("Failed setting server key: %s", err.Error())
	}

	if err := sock.Connect("tcp://127.0.0.1:8089"); err != nil {
		log.Fatalf("Failed connecting to local endpoint: %s", err.Error())
	}

	for idx := 0; idx < 2; idx++ {
		msg, err := sock.Recv()
		if err != nil {
			log.Printf("Failed receiving: %s", err.Error())
		}

		for _, part := range msg {
			log.Println("RX:", string(part))
		}
	}
}

func runPebbePullSock(
	srvKey, pubKey, privKey string,
) {
	sock, err := zmq4.NewSocket(zmq4.PUSH)
	if err != nil {
		log.Fatalf("Failed creating socket: %s", err.Error())
	}
	monSock, err := zmq4.NewSocket(zmq4.PAIR)
	if err != nil {
		panic(err)
	}
	monSock.Connect("inproc://mon")
	go func() {
		for {
			a, b, c, err := monSock.RecvEvent(0)
			log.Println(a, b, c, err)
		}
	}()
	sock.Monitor("inproc://mon", zmq4.EVENT_ALL)

	sock.ClientAuthCurve(srvKey, pubKey, privKey)

	if err := sock.Connect("tcp://127.0.0.1:8089"); err != nil {
		log.Fatalf("Failed connecting to local endpoint: %s", err.Error())
	}
	log.Println("running server")

	for idx := 0; idx < 2; idx++ {
		sock.SendMessage("HI", "omg")
		sock.SendMessage("woaw")
	}
}

func runPushSock(
	srvPub, srvPriv string,
) {
	srvPub, srvPriv = zmq4.Z85decode(srvPub), zmq4.Z85decode(srvPriv)

	ctx := gomq.NewContext(context.Background())
	sock, err := ctx.NewSocket("PUSH", "CURVE")
	if err != nil {
		log.Fatalf("Failed creating socket: %s", err.Error())
	}

	if err := sock.SetServer(true); err != nil {
		log.Fatalf("Failed setting server: %s", err.Error())
	}

	if err := sock.SetOption(zmtp.OptionPubKey, srvPub); err != nil {
		log.Fatalf("Failed setting pubkey: %s", err.Error())
	}

	if err := sock.SetOption(zmtp.OptionSecKey, srvPriv); err != nil {
		log.Fatalf("Failed setting pubkey: %s", err.Error())
	}

	if err := sock.Bind("tcp://127.0.0.1:8089"); err != nil {
		log.Fatalf("Failed connecting to local endpoint: %s", err.Error())
	}

	for idx := 0; idx < 6; idx++ {
		sock.Send([][]byte{[]byte("hi!!!"), []byte("guys!!!")})
	}
	time.Sleep(time.Millisecond * 100)

	sock.Close()
}

func runPebbePushSock(
	srvPub, srvPriv string,
) {
	sock, err := zmq4.NewSocket(zmq4.PUSH)
	if err != nil {
		log.Fatalf("Failed creating socket: %s", err.Error())
	}
	sock.ServerAuthCurve("u", srvPriv)

	if err := sock.Bind("tcp://127.0.0.1:8089"); err != nil {
		log.Fatalf("Failed connecting to local endpoint: %s", err.Error())
	}
	if err := sock.SetCurveServer(1); err != nil {
		panic(err)
	}
	if err := sock.SetCurveSecretkey(srvPriv); err != nil {
		panic(err)
	}

	for idx := 0; idx < 6; idx++ {
		sock.SendMessage("hi!!!", "guys!!!")
	}
	time.Sleep(time.Millisecond * 100)

	sock.Close()
}
