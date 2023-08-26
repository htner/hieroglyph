package main

import (
	"context"

	log "github.com/sirupsen/logrus"

	"github.com/htner/sdb/gosrv/pkg/account"
	"github.com/htner/sdb/gosrv/pkg/lakehouse"
	"github.com/htner/sdb/gosrv/proto/sdb"
)

// server is used to implement proto.ScheduleServer
type AccountServer struct {
	sdb.UnimplementedAccountServer
	port int
}

func (s *AccountServer) Login(cxt context.Context, req *sdb.LoginRequest) (resp *sdb.LoginResponse, err error) {
	acc, err := account.GetSdbAccount(req.Account, req.Passwd)
	if err != nil {
		return nil, err
	}
	resp = new(sdb.LoginResponse)
	resp.AccountId = acc.Id
	resp.Rescode = 0

	return resp, err
}

func (s *AccountServer) UserLogin(ctx context.Context, req *sdb.UserLoginRequest) (resp *sdb.UserLoginResponse, err error) {
	resp = new(sdb.UserLoginResponse)
	user, err := account.GetUser(req.Organization, req.Name, req.Passwd)
	resp = new(sdb.UserLoginResponse)
	if err == account.ErrorPasswdMismatch {
		resp.Rescode = "28P01"
		resp.Msg = "invalid_password"
		log.Printf("passwd mismatch")
		return resp, nil
	} else if err != nil {
		return nil, err
	}

	db, err := account.GetDatabase(req.Organization, req.Database)
	log.Println("get databadse ", db, err, user)
	if err != nil {
		resp.Rescode = "28000"
		resp.Msg = "invalid_authorization_specification"
		log.Printf("invalid database %s, %s", req.Organization, req.Database)
		return resp, nil
	}

	sess, err := lakehouse.CreateSession(uint64(db.Dbid), user.Id)
	if err != nil {
		resp.Rescode = "28000"
		resp.Msg = "invalid_authorization_specification"
		log.Printf("invalid session %d %d", db.Dbid, user.Id)
	}

	resp.UserId = user.Id
	resp.OrganizationId = user.OrganizationId
	resp.SessionId = uint64(sess.Id)
	// TODO
	//resp.Dbid = db.Dbid
	resp.Dbid = 1

	resp.Rescode = "00000"
	log.Printf("get user %d", user.Id)
	return resp, nil
}
