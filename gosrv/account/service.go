package main

import (
	"context"
	"fmt"
	"log"

	"github.com/htner/sdb/gosrv/pkg/fdbkv/kvpair" 
  "github.com/htner/sdb/gosrv/pkg/account"
	"github.com/htner/sdb/gosrv/pkg/types"
	"github.com/htner/sdb/gosrv/proto/sdb"
)

// server is used to implement proto.ScheduleServer
type AccountServer struct {
	sdb.UnimplementedAccountServer
	port int
}

func (s *AccountServer) Login(cxt context.Context, req *LoginRequest) (resp *LoginResponse, err error) {
  user_id, rescode, err := account.GetAccount(req.Organization, req.Account, req.Passwd) 
  if (err != null || rescode != 0) {
    return err;
  }
  tr.Start(true)

  var session kvpair.Session
  session.Uid = 1
  session.Id = types.SessionId(req.Sessionid)
  session.DbId = types.DatabaseId(req.Dbid)
  session.State = kvpair.SessionTransactionIdle
  session.ReadTranscationId = lakehouse.InvaildTranscaton
  session.WriteTranscationId = lakehouse.InvaildTranscaton
  lakehouse.WriteSession(&session)

  return new(sdb.StartTransactionResponse), nil
}

