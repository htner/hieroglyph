package main

import (
	"context"
	"log"

	"github.com/htner/sdb/gosrv/pkg/fdbkv"
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

  log.Println(in)
  if in.TaskId.SliceId == 0 {
    mgr.WriteQueryResult(in.TaskId.QueryId, uint32(in.Result.Rescode), in.Result.Message, "", in.Result)
  }
	return out, err
}

func (c *ScheduleServer) CheckQueryResult(ctx context.Context, in *sdb.CheckQueryResultRequest) (*sdb.CheckQueryResultReply, error) {
	//out := new(sdb.QueryResultReply)
  log.Println(in)
	mgr := schedule.NewQueryMgr(types.DatabaseId(in.Dbid))
	result, err := mgr.ReadQueryResult(in.QueryId)
	reply := new(sdb.CheckQueryResultReply)
  if err == fdbkv.EmptyDataErr {
    log.Println(result, err)
    reply.Rescode = 20000
    reply.Resmsg = "wait"
    err = nil
  } else if result.State != 0 {
    reply.Rescode = int32(result.State)
    reply.Resmsg = result.Detail 
    err = nil
  } else {
    reply.Result = result.Result
  }
	return reply, err
}

func (s *ScheduleServer) Depart(ctx context.Context, query *sdb.ExecQueryRequest) (*sdb.ExecQueryReply, error) {
	queryScheduler := &QueryHandler{request: query}
  queryId, err := queryScheduler.run(query)

	resRepy := &sdb.ExecQueryReply{
		QueryId:   queryId,
		Sessionid: query.Sid,
		Uid:       query.Uid,
		Dbid:      query.Dbid,
		//ResultDir: query.ResultDir,
	}
	return resRepy, err 
}
