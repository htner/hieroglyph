package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"reflect"
	"strconv"
	"strings"
	"time"

	//"github.com/htner/sdb/gosrv/pkg/grpcresolver"
	"github.com/htner/sdb/gosrv/pkg/service"
	"github.com/htner/sdb/gosrv/pkg/utils/postgres"
	"github.com/htner/sdb/gosrv/proto/sdb"
	"github.com/jackc/pgproto3/v2"
	log "github.com/sirupsen/logrus"

	_ "github.com/mbobakov/grpc-consul-resolver" // It's important "github.com/hashicorp/consul/api"
	"google.golang.org/grpc"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
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

  sessionParas map[string]string
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
	if err != nil {
		log.Printf("read client conn %s", err.Error())
	}

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

func (p *Proxy) readClientConn() error {
	startupMessage, err := p.backend.ReceiveStartupMessage()
	if err != nil {
		return err
	}
	buf, err := json.Marshal(startupMessage)
	if err != nil {
		return err
	}
	fmt.Println("startup:", string(buf))

	var ok bool
	var sslRequest *pgproto3.SSLRequest
	if sslRequest, ok = startupMessage.(*pgproto3.SSLRequest); ok {
		fmt.Println("sslrequest", sslRequest)
		buf, err = json.Marshal(sslRequest)
		if err != nil {
			return err
		}
		fmt.Println("sslrequest detail:", string(buf))

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
	var relStartupRequest *pgproto3.StartupMessage
	if relStartupRequest, ok = startupMessage.(*pgproto3.StartupMessage); !ok {
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
			err = p.processQuery(msg)
			if err != nil {
				return err
			}
		case *pgproto3.CancelRequest:
			p.frontendConn.Close()
			p.frontendTLSConn.Close()
			stop = true
			//log.Println("send query result to fronted err ", err)
			log.Println("Cancel ", msg)
			return nil
		}
	}
	return nil
}

type RawMessage struct {
  data []byte 
}

func (R *RawMessage) Decode(data []byte) error {
  R.data = data
  return nil
}

func (R *RawMessage) Encode(data []byte) []byte {
  return append(data, R.data...)
}

func (R *RawMessage) Backend() {
}

func (p *Proxy) sendQueryResultToFronted(resp *sdb.CheckQueryResultReply) error {

  if resp.Result.Rescode != 0 {
      if resp.Result.Message != nil {
        message := resp.Result.Message

	      errMsg := &pgproto3.ErrorResponse{
        Severity: message.Severity, 
        Code: message.Code, 
        Message: message.Message,
        Detail: message.Detail,
        Hint: message.Hint,
        Position: message.Position,
        InternalPosition: message.InternalPosition,
        InternalQuery: message.InternalQuery,
        Where: message.Where,
        SchemaName: message.SchemaName,
        TableName: message.TableName,
        ColumnName: message.ColumnName,
        DataTypeName: message.DataTypeName,
        ConstraintName: message.ConstraintName,
        File: message.File,
        Line: message.Line} 

	      return p.backend.Send(errMsg)
      } else {
			  return p.SendError("58000", "check result empty")
      }
	}

	if resp.Result.CmdType == postgres.CMD_UPDATE ||
		resp.Result.CmdType == postgres.CMD_INSERT ||
		resp.Result.CmdType == postgres.CMD_DELETE {
		var commandTag string

		switch resp.Result.CmdType {
		case postgres.CMD_UPDATE:
			commandTag = "UPDATE "
		case postgres.CMD_INSERT:
			commandTag = "INSERT 0 "
		case postgres.CMD_DELETE:
			commandTag = "DELETE "
		}
		commandTag += strconv.FormatUint(resp.Result.ProcessRows, 10)
		var command pgproto3.CommandComplete
		command.CommandTag = []byte(commandTag)
		return p.backend.Send(&command)
	}

	if resp.Result.CmdType == postgres.CMD_UTILITY {
		  var command pgproto3.CommandComplete
      if resp.Result.Message != nil {
        command.CommandTag = []byte(resp.Result.Message.Message)
      }
		  return p.backend.Send(&command)
  }

	result := resp.Result
  if len(result.DataFiles) == 0 {
		if result.Message != nil {
        message := result.Message
	      errMsg := &pgproto3.ErrorResponse{Code: message.Code, Message: message.Message, Severity: message.Severity} 
	      return p.backend.Send(errMsg)
    } else {
			  return p.SendError("58000", "data file miss")
    }
  }

	//const defaultRegion = "us-east-1"
	staticResolver := aws.EndpointResolverFunc(func(service, region string) (aws.Endpoint, error) {
		return aws.Endpoint{
			PartitionID:       "aws",
			URL:               "http://localhost:9000", // or where ever you ran minio
			SigningRegion:     "us-east-1",
			HostnameImmutable: true,
		}, nil
	})

	cfg := aws.Config{
		Region:           "us-east-1",
		Credentials:      credentials.NewStaticCredentialsProvider("minioadmin", "minioadmin", ""),
		EndpointResolver: staticResolver,
	}

	s3Client := s3.NewFromConfig(cfg)
	out, err := s3Client.GetObject(context.TODO(), &s3.GetObjectInput{
		Bucket: aws.String("sdb1"),
		Key:    aws.String(fmt.Sprintf("%s/%s", result.ResultDir, result.DataFiles[0])),
	})

	if err != nil {
		//log.Printf("Unable to copy item from bucket %q to bucket %q, %v", L.sourceSpace.Base.Bucket, L.sourceSpace.Base.Bucket, err)
		return err
	}

	size := int(out.ContentLength)

	buffer := make([]byte, size)
	defer out.Body.Close()
	var bbuffer bytes.Buffer
	for true {
		num, rerr := out.Body.Read(buffer)
		if num > 0 {
			bbuffer.Write(buffer[:num])
		} else if rerr == io.EOF || rerr != nil {
			break
		}
	}

	/*
		fileName := fmt.Sprintf("%d_%d_%d.queryres", resp.QueryId, resp.Uid, resp.Dbid)
		resFile := fmt.Sprintf("%s/%s", resp.ResultDir, fileName)
		file, err := os.Open(resFile)
		if err != nil {
			return err
		}
	*/
	reader := bufio.NewReader(&bbuffer)

	// result is len:type:data
  for true {
    buf := make([]byte, 8)
    n, err := io.ReadFull(reader, buf)
    if err != nil && err != io.EOF {
      log.Println("reader error:", err)
      return err
    }
    if n == 0 {
      log.Println("reader end")
      return nil
    }

    if n != 8 {
      log.Println("reader len error:", n)
      return nil
    }

    descSize := binary.BigEndian.Uint64(buf)

    descBuf := make([]byte, descSize)
    n, err = io.ReadFull(reader, descBuf)

    if err != nil && err != io.EOF {
      log.Println("reader error:", err)
      return err
    }

    if n != int(descSize) {
      log.Println("reader len error:", n)
      return nil
    }

		log.Println("send message, type: ", string(rune(descBuf[0])))

    var raw RawMessage
    raw.data = descBuf

    err = p.backend.Send(&raw)
    if err != nil {
      return err
    }
  }


  /*
	switch descBuf[0] {
	case 'C':
		log.Println("dddtest: ", descBuf)
		var command pgproto3.CommandComplete
		err = command.Decode(descBuf[1:])
		if err != nil {
			return err
		}
		err = p.backend.Send(&command)
		if err != nil {
			return err
		}
	case 'T':
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
				return err
			}

			var rowData pgproto3.DataRow
			rowData.Decode(dataBuf[1:])
			err = p.backend.Send(&rowData)
			if err != nil {
				return err
			}
		}
		p.backend.Send(&pgproto3.CommandComplete{CommandTag: []byte("END")})
  case 'D':
    var command pgproto3.DataRow
		err = command.Decode(descBuf[1:])
		if err != nil {
			return err
		}
		err = p.backend.Send(&command)
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("unknown message type: %c", descBuf[0])
	}
  */
	return nil
}

func (p *Proxy) checkCancel() bool {
	baseMsg, err := p.backend.Receive()
	fmt.Println(baseMsg, err)
	fmt.Println("type", reflect.TypeOf(baseMsg))
	if err != nil {
		return true
	}
	buf, err := json.Marshal(baseMsg)
	if err != nil {
		return true
	}
	fmt.Println("basemsg:", string(buf))

	// 自定义 LB，并使用刚才写的 Consul Resolver
	//lbrr := grpc.RoundRobin(grpcresolver.ForConsul(registry))

	switch msg := baseMsg.(type) {
	case *pgproto3.CancelRequest:
		p.frontendConn.Close()
		p.frontendTLSConn.Close()
		//log.Println("send query result to fronted err ", err)
		log.Println("Cancel ", msg)
		return true
	}
	return false
}

func (p *Proxy) processQuery(msg *pgproto3.Query) (err error) {
	defer func() {
		err = p.backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
		if err != nil {
			log.Println(err)
		}
	}()

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
		p.SendError("58000", "schedule server connect error")
		return err
	}
	defer conn.Close()
	// create a client and call the server
	client := sdb.NewScheduleClient(conn)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

  req := &sdb.ExecQueryRequest{Sql: msg.String, Sid: p.sessionid, Uid: p.uid, Dbid: p.dbid, Organization: p.organization, OrganizationId: p.organizationId}
	log.Println("get req:", req)
	resp, err := client.Depart(ctx, req)
	log.Println("get resp:", resp, err)
	if err != nil {
		p.SendError("58030", "exec query error")
		return err
	}

	queryId := resp.QueryId
	var respResult *sdb.CheckQueryResultReply
	for true {
		respResult, err = client.CheckQueryResult(ctx, &sdb.CheckQueryResultRequest{Dbid: p.dbid, QueryId: queryId})
		if err != nil {
		  log.Println("check query result resp:", respResult, err)
			p.SendError("58030", "get query result error")
			return err
		}
		if respResult.Rescode == 20000 {
			time.Sleep(time.Millisecond * 50)
      continue
    } 

    log.Println("check query result resp:", respResult, err)
    if respResult.Result == nil ||  respResult.Rescode != 0 {
			return p.SendError("58000", "check result empty")
		} else {
			break
		}
		/*
			    if (p.checkCancel()) {
						p.SendError("58030", fmt.Sprint("Recv Cancel"))
			      return nil
			    }
		*/
	}

	err = p.sendQueryResultToFronted(respResult)
	if err != nil {
		log.Println("send query result to fronted err ", err)
		p.SendError("58040", "query result error")
		return
	}
	return nil
}

func (p *Proxy) SendError(code string, msg string) error {
	errMsg := &pgproto3.ErrorResponse{Code: code, Message: msg, Severity: "ERROR"}
	return p.backend.Send(errMsg)
}

func (p *Proxy) checkUser(msg *pgproto3.StartupMessage) error {
	fullDatabase := msg.Parameters["database"]
	names := strings.Split(fullDatabase, ".")
	if len(names) == 1 {
    p.organization = "test"
    p.database = fullDatabase
    p.username = msg.Parameters["user"]
		// return errors.New("must has organization and username")
  } else if len(names) >= 2 {
    p.organization = names[0]
    p.database = names[1]
    p.username = msg.Parameters["user"]
  }
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
			p.SendError(resp.Rescode, resp.Msg)
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
	p.SendError("58030", "auth error")
	return errors.New("unkonw message")
}
