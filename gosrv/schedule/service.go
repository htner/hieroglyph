package main

import (
	"context"

	"github.com/htner/sdb/gosrv/pkg/schedule"
	"github.com/htner/sdb/gosrv/pkg/types"
	"github.com/htner/sdb/gosrv/proto/sdb"
	//"google.golang.org/grpc/credentials/insecure"
)

// server is used to implement proto.ScheduleServer
type ScheduleServer struct {
	sdb.UnimplementedScheduleServer
	port int
}

// Depart implements proto.ScheduleServer
// It just returns commid
/*
func (s *ScheduleServer) Depart(ctx context.Context, in *pb.ExecQueryRequest) (*pb.ExecQueryReply, error) {
	log.Printf("get request %s", in.Sql)
	return &pb.ExecQueryReply{}, nil
}
*/

func (c *ScheduleServer) PushWorkerResult(ctx context.Context, in *sdb.PushWorkerResultRequest) (*sdb.PushWorkerResultReply, error) {
	out := new(sdb.PushWorkerResultReply)
	mgr := schedule.NewQueryMgr(types.DatabaseId(in.Result.Dbid))
	err := mgr.WriterWorkerResult(in)
	return out, err
}

func (c *ScheduleServer) CheckQueryResult(ctx context.Context, in *sdb.CheckQueryResultRequest) (*sdb.CheckQueryResultReply, error) {
	//out := new(sdb.QueryResultReply)
	mgr := schedule.NewQueryMgr(types.DatabaseId(in.Result.Dbid))
	result, err := mgr.ReadQueryResult(in.QueryId)
	reply := new(sdb.CheckQueryResultReply)

	return reply, err
}

func (s *ScheduleServer) Depart(ctx context.Context, query *sdb.ExecQueryRequest) (*sdb.ExecQueryReply, error) {
	queryScheduler := &QueryScheduler{request: query}
	queryScheduler.run()

	resRepy := &sdb.ExecQueryReply{
		QueryId:   query.TaskIdentify.QueryId,
		Sessionid: query.Sessionid,
		Uid:       query.Uid,
		Dbid:      query.Dbid,
		ResultDir: query.ResultDir,
	}
	return resRepy, nil
}
