package main

import (
	"context"
	"time"

	"github.com/htner/sdb/gosrv/pkg/fdbkv"
	"github.com/htner/sdb/gosrv/pkg/lakehouse"
	"github.com/htner/sdb/gosrv/pkg/schedule"
	"github.com/htner/sdb/gosrv/proto/sdb"
	log "github.com/sirupsen/logrus"
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
	mgr := schedule.NewQueryMgr(in.Result.Dbid)
	err := mgr.WriterWorkerResult(in)

	log.Println(in)
	if in.TaskId.SliceId == 0 {
		isFinish, err := mgr.WriteQueryResult(in.TaskId.QueryId, in.Result)
		if err != nil {
			return nil, err
		}
		log.Println("wirte query result ", in)
		if isFinish {
			tr := lakehouse.NewTranscation(in.Dbid, in.Sessionid)
		  log.Println("try auto commit")
			tr.TryAutoCommitWithQuery(in.TaskId.QueryId)
		}
	}
	return out, err
}

func (c *ScheduleServer) CheckQueryResult(ctx context.Context, in *sdb.CheckQueryResultRequest) (*sdb.CheckQueryResultReply, error) {
	//out := new(sdb.QueryResultReply)
	// log.Println(in)
	mgr := schedule.NewQueryMgr(in.Dbid)
	result, err := mgr.ReadQueryResult(in.QueryId)
	reply := new(sdb.CheckQueryResultReply)
	if err == fdbkv.ErrEmptyData {
		log.Println(result, err)
		reply.Rescode = 20000
		reply.Resmsg = "wait"
		err = nil
  } else if result.State <= uint32(sdb.QueryStates_QueryPartitionSuccess) {
		reply.Rescode = 20000
		reply.Resmsg = "wait"
		err = nil
	} else {
    log.Println("get query result", result)
    reply.Rescode = 0
		if len(result.Result) == 0 {
      worker_result := new(sdb.WorkerResultData)
      worker_result.Rescode = -1
      worker_result.Message = result.Message 
      reply.Result = worker_result
		} else {
		  reply.Result = result.Result[0]
		//reply.Result.State = result.State
      if result.State == uint32(sdb.QueryStates_QuerySuccess) {
        for i := 1; i < len(result.Result); i++ {
          result_sub := result.Result[i]
          reply.Result.ProcessRows += result_sub.ProcessRows
          reply.Result.DataFiles = append(reply.Result.DataFiles, result_sub.DataFiles...)
        }
      }
    }
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

func (s *ScheduleServer) NewWorkerId(context.Context, *sdb.NewWorkerIdRequest) (*sdb.NewWorkerIdReply, error) {
  id := time.Now().UnixNano()
  workerId := uint32(id)
  reply := &sdb.NewWorkerIdReply{WorkerId: uint64(workerId)}
	log.Println("new workerid ", workerId)
  return reply, nil
}

func (s *ScheduleServer) WorkerPing(ctx context.Context, req *sdb.WorkerPingRequest) (*sdb.WorkerPongReply, error) {
  reply := &sdb.WorkerPongReply{}
  mgr := schedule.NewWorkerMgr()
  mgr.Ping(req)

  //log.Println("ping")
  return reply, nil
}
