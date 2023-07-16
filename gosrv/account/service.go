package main

import (
	"context"

  "github.com/htner/sdb/gosrv/pkg/account"
	"github.com/htner/sdb/gosrv/proto/sdb"
)

// server is used to implement proto.ScheduleServer
type AccountServer struct {
	sdb.UnimplementedAccountServer
	port int
}

func (s *AccountServer) Login(cxt context.Context, req *sdb.LoginRequest) (resp *sdb.LoginResponse, err error) {
  acc, err := account.GetSdbAccount(req.Account, req.Passwd) 
  if (err != nil) {
    return nil, err;
  }
  resp = new(sdb.LoginResponse)
  resp.AccountId = acc.Id
  resp.Rescode = 0

  return resp, err 
}

func (s *AccountServer) UserLogin(ctx context.Context, req *sdb.UserLoginRequest) (resp *sdb.UserLoginResponse, err error) {
  resp = new(sdb.UserLoginResponse)
  user, err := account.GetUser(req.Organization, req.Name, req.Passwd) 
  if (err != nil) {
    return nil, err;
  }
  resp = new(sdb.UserLoginResponse)
  resp.UserId = user.Id
  resp.OrganizationId = user.OrganizationId
  resp.Rescode = "00000"

  return resp, err 
}
