package main

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"reflect"

	"github.com/jackc/pgproto3/v2"
	_ "github.com/mbobakov/grpc-consul-resolver" // It's important
	"google.golang.org/grpc"
)

type Proxy struct {
	backend *pgproto3.Backend

	frontendConn    net.Conn
	frontendTLSConn *tls.Conn
}

func NewProxy(frontendConn net.Conn) *Proxy {
	backend := pgproto3.NewBackend(pgproto3.NewChunkReader(frontendConn), frontendConn)
	proxy := &Proxy{
		backend:      backend,
		frontendConn: frontendConn,
	}

	return proxy
}

func (p *Proxy) Run() error {
	defer p.Close()

	frontendErrChan := make(chan error, 1)
	frontendMsgChan := make(chan pgproto3.FrontendMessage)
	frontendNextChan := make(chan struct{})
	go p.readClientConn(frontendMsgChan, frontendNextChan, frontendErrChan)

	for {
		select {
		case msg := <-frontendMsgChan:
			buf, err := json.Marshal(msg)
			if err != nil {
				return err
			}
			fmt.Println("F", string(buf))

			//err = p.frontend.Send(msg)
			//if err != nil {
			//	return err
			//}
			frontendNextChan <- struct{}{}
		case err := <-frontendErrChan:
			return err
		}
	}
}

func (p *Proxy) Close() error {
	frontendCloseErr := p.frontendConn.Close()

	if frontendCloseErr != nil {
		return frontendCloseErr
	}
	return nil
}

func (p *Proxy) readClientConn(msgChan chan pgproto3.FrontendMessage, nextChan chan struct{}, errChan chan error) {
	startupMessage, err := p.backend.ReceiveStartupMessage()
	if err != nil {
		errChan <- err
		return
	}
	buf, err := json.Marshal(startupMessage)
	if err != nil {
		return
	}
	fmt.Println("F", string(buf))

	sslRequest := startupMessage.(*pgproto3.SSLRequest)
	fmt.Println(sslRequest)
	buf, err = json.Marshal(sslRequest)
	if err != nil {
		return
	}
	fmt.Println("F", string(buf))

	if sslRequest != nil {
		s := []byte{'S'}
		n, err := p.frontendConn.Write(s)
		fmt.Println("write", n, err)

		cer, err := tls.LoadX509KeyPair("./server.crt", "./server.key")
		if err != nil {
			log.Println(err)
			return
		}

		config := &tls.Config{Certificates: []tls.Certificate{cer}}
		fmt.Println("config", config)

		p.frontendTLSConn = tls.Server(p.frontendConn, config)
		err = p.frontendTLSConn.Handshake()
		if err != nil {
			return
		}
		fmt.Println("Handshake", err)
		//p.frontendConn.
		p.backend = pgproto3.NewBackend(pgproto3.NewChunkReader(p.frontendTLSConn), p.frontendTLSConn)
		fmt.Println("reset backend", p.backend)
		//p.frontendConn = p.frontendTLSConn
		/*
			for {
				var b []byte
				n, err := p.frontendTLSConn.Read(b)
				if n != 0 {
					fmt.Println("read", n, err)
				}
			}
		*/

		startupMessage, err := p.backend.ReceiveStartupMessage()
		if err != nil {
			errChan <- err
			return
		}
		buf, err := json.Marshal(startupMessage)
		if err != nil {
			return
		}
		fmt.Println("F", string(buf))

		//ready := new(pgproto3.AuthenticationOk)
		//p.frontendTLSConn(ready)
	}

	//nbNetConn := nbconn.NewNetConn(p.frontendConn, false)

	// msgChan <- startupMessage

	//<-nextChan
	//fmt.Println("StartupMessage", msg)
	//password := new(pgproto3.AuthenticationCleartextPassword)
	//p.backend.Send(password)
	ok := new(pgproto3.AuthenticationOk)
	p.backend.Send(ok)

	ready := new(pgproto3.ReadyForQuery)
	ready.TxStatus = 'I'
	p.backend.Send(ready)

	for {
		baseMsg, err := p.backend.Receive()
		fmt.Println(baseMsg, err)
		fmt.Println("type", reflect.TypeOf(baseMsg))
		if err != nil {
			return
		}
		buf, err := json.Marshal(baseMsg)
		if err != nil {
			return
		}
		fmt.Println("K", string(buf))
		switch msg := baseMsg.(type) {
		case *pgproto3.Query:
			fmt.Println("Query", msg)
			conn, err := grpc.Dial(
				"consul://127.0.0.1:8500/whoami?wait=14s&tag=manual",
				grpc.WithInsecure(),
				grpc.WithDefaultServiceConfig(`{"loadBalancingPolicy": "round_robin"}`),
			)
			if err != nil {
				log.Fatal(err)
			}
			defer conn.Close()
			break
		}

	}
}
