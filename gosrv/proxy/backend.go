package main

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"reflect"
	"time"

	//"github.com/htner/sdb/gosrv/pkg/grpcresolver"
	"github.com/htner/sdb/gosrv/pkg/service"
	"github.com/htner/sdb/gosrv/proto/sdb"
	"github.com/jackc/pgproto3/v2"

	_ "github.com/mbobakov/grpc-consul-resolver" // It's important
	//"github.com/hashicorp/consul/api"
	"google.golang.org/grpc"
)

const grpcServiceConfig = `{"loadBalancingConfig": [ { "round_robin": {} } ]}`

type Proxy struct {
	backend *pgproto3.Backend

	frontendConn    net.Conn
	frontendTLSConn *tls.Conn

	username      string
	database      string
	uid           uint64
	dbid          uint64
	sessionid     uint64
	transcationid uint64
}

func NewProxy(frontendConn net.Conn) *Proxy {
	backend := pgproto3.NewBackend(pgproto3.NewChunkReader(frontendConn), frontendConn)
	proxy := &Proxy{
		backend:       backend,
		frontendConn:  frontendConn,
		username:      "",
		database:      "",
		dbid:          0,
		uid:           0,
		sessionid:     0,
		transcationid: 0,
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

		startupMessage, err = p.backend.ReceiveStartupMessage()
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
	relStartupRequest := startupMessage.(*pgproto3.StartupMessage)
	if relStartupRequest == nil {
		return
	}
	p.username = relStartupRequest.Parameters["user"]

	ok := new(pgproto3.AuthenticationOk)
	p.backend.Send(ok)

	ready := new(pgproto3.ReadyForQuery)
	ready.TxStatus = 'I'
	p.backend.Send(ready)

	stop := false
	for !stop {
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

		// 自定义 LB，并使用刚才写的 Consul Resolver
		//lbrr := grpc.RoundRobin(grpcresolver.ForConsul(registry))

		switch msg := baseMsg.(type) {
		case *pgproto3.Query:
			fmt.Println("Query", msg)
			scheduleServerName := service.ScheduleName()
			consul := "consul://127.0.0.1:8500/" + scheduleServerName + "?wait=14s&tag=public"
			conn, err := grpc.Dial(
				consul,
				grpc.WithInsecure(),
				grpc.WithBlock(),
				grpc.WithDefaultServiceConfig(grpcServiceConfig),
				grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(128e+6)),
			)
			if err != nil {
				err = p.backend.Send(&pgproto3.ErrorResponse{Code: "58000"})
				if err != nil {
					return // fmt.Errorf("error writing query response: %w", err)
				}
				return
			}
			defer conn.Close()
			// create a client and call the server
			client := sdb.NewScheduleClient(conn)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			resp, err := client.Depart(ctx, &sdb.ExecQueryRequest{Sql: msg.String})
			log.Println("get resp:", resp, err)
			if err != nil {
				buf := (&pgproto3.ErrorResponse{Code: "58030"}).Encode(nil)
				_, err = p.frontendConn.Write(buf)
				if err != nil {
					return // fmt.Errorf("error writing query response: %w", err)
				}
				return
			}

			time.Sleep(10 * time.Second)
			err = p.sendQueryResultToFronted(resp)
			if err != nil {
				log.Println("send query result to fronted err ", err)
				return
			}
			//port, err := strconv.Atoi(resp.Message)
		case *pgproto3.CancelRequest:
			p.frontendConn.Close()
			p.frontendTLSConn.Close()
			stop = true
			return
		}

	}
}

func (p *Proxy) sendQueryResultToFronted(resp *sdb.ExecQueryReply) error {
	fileName := fmt.Sprintf("%d_%d_%d.queryres", resp.QueryId, resp.Uid, resp.Dbid)
	resFile := fmt.Sprintf("%s/%s", resp.ResultDir, fileName)
	file, err := os.Open(resFile)
	if err != nil {
		return err
	}
	reader := bufio.NewReader(file)

	buf := make([]byte, 8)
	n, err := reader.Read(buf)
	if err != nil && err != io.EOF {
		return err
	}

	if n == 0 {
		return nil
	}

	descSize := binary.BigEndian.Uint64(buf)

	descBuf := make([]byte, descSize)
	reader.Read(descBuf)
	var rowDesc pgproto3.RowDescription
	rowDesc.Decode(descBuf[1:])
	err = p.backend.Send(&rowDesc)

	for {
		n, err := reader.Read(buf)
		if err != nil && err != io.EOF {
			return err
		}

		if n == 0 {
			break
		}

		dataSize := binary.BigEndian.Uint64(buf)
		dataBuf := make([]byte, dataSize)
		n, err = reader.Read(dataBuf)

		if err != nil {
			return nil	
		}

		var rowData pgproto3.DataRow
		rowData.Decode(dataBuf[1:])
		err = p.backend.Send(&rowData)
	}

	p.backend.Send(&pgproto3.CommandComplete{CommandTag: []byte("SELECT 1")})
	err = p.backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
	if err != nil {
		return err
	}
	return nil
}
