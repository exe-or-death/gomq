package main

import (
	"context"
	"log"
	"time"

	"github.com/pebbe/zmq4"
	"github.com/workspace-9/gomq"
	_ "github.com/workspace-9/gomq/transport/ipc"
	_ "github.com/workspace-9/gomq/types/pull"
	_ "github.com/workspace-9/gomq/types/push"
	_ "github.com/workspace-9/gomq/zmtp/null"
)

func main() {
	go runPushSock()
	time.Sleep(time.Second)
	runPullSock()
}

func runPullSock() {
	ctx := gomq.NewContext(context.Background())
	sock, err := ctx.NewSocket("PULL", "NULL")
	if err != nil {
		log.Fatalf("Failed creating socket: %s", err.Error())
	}

	if err := sock.Bind("ipc://file.txt"); err != nil {
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

func runPebbePullSock() {
	sock, err := zmq4.NewSocket(zmq4.PULL)
	if err != nil {
		log.Fatalf("Failed creating socket: %s", err.Error())
	}

	if err := sock.Bind("ipc://file.txt"); err != nil {
		log.Fatalf("Failed connecting to local endpoint: %s", err.Error())
	}

	for idx := 0; idx < 2; idx++ {
		msg, err := sock.RecvMessage(0)
		if err != nil {
			log.Printf("Failed receiving: %s", err.Error())
		}

		for _, part := range msg {
			log.Println("RX:", idx, string(part))
		}
	}
}

func runPushSock() {
	time.Sleep(time.Second)
	ctx := gomq.NewContext(context.Background())
	sock, err := ctx.NewSocket("PUSH", "NULL")
	if err != nil {
		log.Fatalf("Failed creating socket: %s", err.Error())
	}

	if err := sock.Connect("ipc://file.txt"); err != nil {
		log.Fatalf("Failed binding to local endpoint: %s", err.Error())
	}

	if err := sock.Send([][]byte{[]byte("hola!"), []byte("senor!")}); err != nil {
		log.Fatalf("Failed sending: %s", err.Error())
	}
	time.Sleep(time.Second)

	if err := sock.Disconnect("ipc://file.txt"); err != nil {
		log.Fatalf("Failed disconnecting: %s", err.Error())
	}

	time.Sleep(time.Second)
	if err := sock.Connect("ipc://file.txt"); err != nil {
		log.Fatalf("Failed connecting to local endpoint: %s", err.Error())
	}

	if err := sock.Send([][]byte{[]byte("hola!")}); err != nil {
		log.Fatalf("Failed sending: %s", err.Error())
	}
	sock.Close()
}
