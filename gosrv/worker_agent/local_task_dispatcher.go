package main

import (
	"context"

	log "github.com/sirupsen/logrus"

	"github.com/htner/sdb/gosrv/pkg/account"
	"github.com/htner/sdb/gosrv/pkg/lakehouse"
	"github.com/htner/sdb/gosrv/proto/sdb"
)

// server is used to implement proto.ScheduleServer
type LocalTaskDispatcher struct {
	sdb.UnimplementedWorkerServer
  
}

func (s *LocalTaskDispatcher) Prepare(context.Context, req *PrepareTaskRequest) (reply *PrepareTaskReply, error) {
  while (true) {
    worker := kWorkerMgr.GetIdleWroker(req) 
    if worker != nil {
      return worker.Prepare(req)
    }
  }
    
}

func (s *LocalTaskDispatcher) Start(context.Context, req *StartTaskRequest) (reply *StartTaskReply, error) {
   while (true) {
    worker := kWorkerMgr.GetWroker(req) 
    if worker != nil {
      return worker.Start(req)
    }
  }
     
}

func (s *LocalTaskDispatcher) StartStream(context.Context, *MotionStreamRequest) (*MotionStreamReply, error) {

}
