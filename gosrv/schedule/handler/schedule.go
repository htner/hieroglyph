package handler

import (
	"context"

	"go-micro.dev/v4/logger"

	pb "github.com/htner/sdb/gosrv/schedule/proto"
)

type Schedule struct{}

func (e *Schedule) Depart(ctx context.Context, req *pb.ExecQueryRequest, rsp *pb.ExecQueryReply) error {
	logger.Infof("Received Schedule.Call request: %v", req)
	rsp.Message = req.Name
	return nil
}
