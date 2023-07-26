package main

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"reflect"
	"strings"
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

	organization string
	username     string
	database     string

	uid            uint64
	organizationId uint64
	dbid           uint64
	sessionid      uint64
	transcationid  uint64
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

  err := p.readClientConn()
	log.Printf("read client conn %s", err.Error())

  /*
	for {
		select {
		case msg := <-frontendMsgChan:
			buf, err := json.Marshal(msg)
			if err != nil {
				return err
			}

			//err = p.frontend.Send(msg)
			//if err != nil {
			//	return err
			//}
			frontendNextChan <- struct{}{}
		case err := <-frontendErrChan:
			return err
		}
	}
  */
  return nil
}

func (p *Proxy) Close() error {
	log.Printf("close client conn")
	frontendCloseErr := p.frontendConn.Close()

	if frontendCloseErr != nil {
		return frontendCloseErr
	}
	return nil
}

func (p *Proxy) readClientConn() error{
	startupMessage, err := p.backend.ReceiveStartupMessage()
	if err != nil {
		return err
	}
	buf, err := json.Marshal(startupMessage)
	if err != nil {
		return err
	}
  fmt.Println("startup:", string(buf))

	sslRequest := startupMessage.(*pgproto3.SSLRequest)
	fmt.Println("sslrequest", sslRequest)
	buf, err = json.Marshal(sslRequest)
	if err != nil {
		return err
	}
  fmt.Println("sslrequest detail:", string(buf))

	if sslRequest != nil {
		s := []byte{'S'}
		n, err := p.frontendConn.Write(s)
		fmt.Println("write", n, err)

		cer, err := tls.LoadX509KeyPair("./server.crt", "./server.key")
		if err != nil {
			log.Println(err)
			return err
		}

		config := &tls.Config{Certificates: []tls.Certificate{cer}}
		fmt.Println("config", config)

		p.frontendTLSConn = tls.Server(p.frontendConn, config)
		err = p.frontendTLSConn.Handshake()
		if err != nil {
			return err
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
			return err
		}
		buf, err := json.Marshal(startupMessage)
		if err != nil {
			return err
		}
    fmt.Println("startup message in ssh:", string(buf))

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
		fmt.Println("not a startupMessage")
		return err
	}
	//p.username = relStartupRequest.Parameters["user"]
	err = p.checkUser(relStartupRequest)
  if err != nil {
		return err
	}

	ready := new(pgproto3.ReadyForQuery)
	ready.TxStatus = 'I'
	p.backend.Send(ready)

	stop := false
	for !stop {
		baseMsg, err := p.backend.Receive()
		fmt.Println(baseMsg, err)
		fmt.Println("type", reflect.TypeOf(baseMsg))
		if err != nil {
			return err
		}
		buf, err := json.Marshal(baseMsg)
		if err != nil {
			return err
		}
    fmt.Println("basemsg:", string(buf))

		// 自定义 LB，并使用刚才写的 Consul Resolver
		//lbrr := grpc.RoundRobin(grpcresolver.ForConsul(registry))

		switch msg := baseMsg.(type) {
		case *pgproto3.Query:
      /*
<<<<<<< HEAD
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
				p.backend.Send(&pgproto3.ErrorResponse{Severity: "ERROR",
								Code : "-1",
								Message: err.Error()})
				p.backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
				return
			}
      */
			//port, err := strconv.Atoi(resp.Message)
//=======
	//port, err := strconv.Atoi(resp.Message)
 //     p.sendQueryToSchedule(msg)
//>>>>>>> tmp task manager
//=======
			//port, err := strconv.Atoi(resp.Message)
			err = p.sendQueryToSchedule(msg)
      if err != nil {
        return err
      }
//>>>>>>> task ok
		case *pgproto3.CancelRequest:
			p.frontendConn.Close()
			p.frontendTLSConn.Close()
			stop = true
			return nil
		}

	}
  return nil
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

func (p *Proxy) sendQueryToSchedule(msg *pgproto3.Query) (err error) {
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
		p.SendError("58000")
		return err
	}
	defer conn.Close()
	// create a client and call the server
	client := sdb.NewScheduleClient(conn)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

  req := &sdb.ExecQueryRequest{Sql: msg.String, Sid:p.sessionid, Uid:p.uid, Dbid: p.dbid}
  log.Println("get req:", req)
  resp, err := client.Depart(ctx, req)
	log.Println("get resp:", resp, err)
	if err != nil {
		p.SendError("58030")
		return err
	}

	queryId := resp.QueryId
	var respResult *sdb.CheckQueryResultReply
	for true {
		respResult, err = client.CheckQueryResult(ctx, &sdb.CheckQueryResultRequest{QueryId: queryId})
		log.Println("check query result resp:", respResult, err)
		if err != nil {
			p.SendError("58030")
			return err
		}
    time.Sleep(time.Second)
	}

	err = p.sendQueryResultToFronted(resp)
	if err != nil {
		log.Println("send query result to fronted err ", err)
		p.SendError("58040")
		return
	}
	return nil
}

func (p *Proxy) SendError(code string) {
	buf := (&pgproto3.ErrorResponse{Code: "58030"}).Encode(nil)
	_, err := p.frontendConn.Write(buf)
	if err != nil {
		return // fmt.Errorf("error writing query response: %w", err)
	}
}

func (p *Proxy) checkUser(msg *pgproto3.StartupMessage) error {
	fullDatabase := msg.Parameters["database"]
	names := strings.Split(fullDatabase, ".")
	if len(names) != 2 {
		return errors.New("must has organization and username")
	}
	p.organization = names[0]
	p.database = names[1]
	p.username = msg.Parameters["user"]
	passwd := ""

  log.Println("checkuser ", p.organization, p.username)

	accountServerName := service.AccountName()
	consul := "consul://127.0.0.1:8500/" + accountServerName + "?wait=14s&tag=public"
	conn, err := grpc.Dial(
		consul,
		grpc.WithInsecure(),
		grpc.WithBlock(),
		grpc.WithDefaultServiceConfig(grpcServiceConfig),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(128e+6)),
	)
	if err != nil {
		return err
	}
	defer conn.Close()
	// create a client and call the server
	client := sdb.NewAccountClient(conn)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	count := 0

	for count < 2 {
    log.Println("account server user login ", p.organization, p.username, passwd)
		resp, err := client.UserLogin(ctx, &sdb.UserLoginRequest{Organization: p.organization, Name: p.username, Passwd: passwd, Database: p.database})
		log.Println("get user login resp:", resp, err)
		if err != nil {
			return err
		}
		if count == 0 && resp.Rescode == "28P01" {
			password := new(pgproto3.AuthenticationCleartextPassword)
			p.backend.Send(password)
		} else if resp.Rescode == "00000" {
      p.dbid = resp.Dbid
      p.uid = resp.UserId
      p.organizationId = resp.OrganizationId
      p.sessionid = resp.SessionId

      ok := new(pgproto3.AuthenticationOk)
      p.backend.Send(ok)
      return nil
    } else {
      p.SendError(resp.Rescode)
      return errors.New("error")
    }

		passwdMsg, err := p.backend.Receive()
		switch msg := passwdMsg.(type) {
		case *pgproto3.PasswordMessage:
			passwd = msg.Password
		default:
			return errors.New("unkonw message")
		}
    count++
	}
	p.SendError("58030")
	return errors.New("unkonw message")
}
