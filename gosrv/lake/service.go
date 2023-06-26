package main

import (
	"context"
	"fmt"
	"log"

	"github.com/htner/sdb/gosrv/pkg/fdbkv/kvpair"
	"github.com/htner/sdb/gosrv/pkg/lakehouse"
	"github.com/htner/sdb/gosrv/pkg/types"
	"github.com/htner/sdb/gosrv/proto/sdb"
)

// server is used to implement proto.ScheduleServer
type LakeServer struct {
	sdb.UnimplementedLakeServer
	port int
}

func (s *LakeServer) Start (ctx context.Context, req *sdb.StartTransactionRequest) (*sdb.StartTransactionResponse, error) {
  tr := lakehouse.NewTranscation(types.DatabaseId(req.Dbid), types.SessionId(req.Sessionid)) 
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
func (s *LakeServer) Commit(ctx context.Context, req *sdb.CommitRequest) (*sdb.CommitResponse, error) {
  tr := lakehouse.NewTranscation(types.DatabaseId(req.Dbid), types.SessionId(req.Sessionid)) 
  tr.Commit()
  return new(sdb.CommitResponse), nil
}

func (s *LakeServer) Abort(ctx context.Context, req *sdb.AbortRequest) (*sdb.AbortResponse, error) {
  tr := lakehouse.NewTranscation(types.DatabaseId(req.Dbid), types.SessionId(req.Sessionid)) 
  tr.Commit()
  return new(sdb.AbortResponse), nil
}

func (s *LakeServer) AllocateXid(ctx context.Context, req *sdb.AllocateXidRequest) (*sdb.AllocateXidResponse, error) {
  tr := lakehouse.NewTranscation(types.DatabaseId(req.Dbid), types.SessionId(req.Sessionid)) 
  tr.Commit()
  sess, err := tr.ReadAble()
  if err != nil {
    return nil, err
  }

  sess, err = tr.WriteAble()
  if err != nil {
    return nil, err
  }

  var result sdb.AllocateXidResponse
  result.ReadXid = uint64(sess.ReadTranscationId)
  result.WriteXid = uint64(sess.WriteTranscationId)
  return &result, nil
}

// Depart implements proto.ScheduleServer
// It just returns commid
func (s *LakeServer) PrepareInsertFiles(ctx context.Context, request *sdb.PrepareInsertFilesRequest) (*sdb.PrepareInsertFilesResponse, error) {
	log.Println("prepare request", request)
	lakeop := lakehouse.NewLakeRelOperator(
		types.DatabaseId(request.Dbid),
		types.SessionId(request.Sessionid),
		types.TransactionId(request.CommitXid))
	err := lakeop.MarkFiles(types.RelId(request.Rel), request.AddFiles)
	if err != nil {
		return nil, fmt.Errorf("mark files error")
	}
	return &sdb.PrepareInsertFilesResponse{}, nil
}

func (s *LakeServer) UpdateFiles(ctx context.Context, request *sdb.UpdateFilesRequest) (*sdb.UpdateFilesResponse, error) {
	log.Println("update files", request)
	//log.Printf("get request %s", in.Sql)
	lakeop := lakehouse.NewLakeRelOperator(types.DatabaseId(request.Dbid),
		types.SessionId(request.Sessionid),
		types.TransactionId(request.CommitXid))

	newFiles := make([]*kvpair.FileMeta, 0)
	removeFiles := make([]*kvpair.FileMeta, 0)
	for _, file := range request.AddFiles {
		var f kvpair.FileMeta
		f.Database = types.DatabaseId(request.Dbid)
		f.Relation = types.RelId(request.Rel)
		f.Filename = file.FileName

		newFiles = append(newFiles, &f)
	}

	for _, file := range request.RemoveFiles {
		var f kvpair.FileMeta
		f.Database = types.DatabaseId(request.Dbid)
		f.Relation = types.RelId(request.Rel)
		f.Filename = file.FileName

		removeFiles = append(removeFiles, &f)
	}

	err := lakeop.InsertFiles(types.RelId(request.Rel), newFiles)
	if err != nil {
    log.Printf("insert files error: %s", err.Error())
		return nil, fmt.Errorf("insert files error")
	}
	err = lakeop.DeleleFiles(types.RelId(request.Rel), removeFiles)
	if err != nil {
		return nil, fmt.Errorf("delete files error")
	}
	return &sdb.UpdateFilesResponse{}, nil
}


