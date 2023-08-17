package main

import (
	"context"
	"fmt"

	"github.com/htner/sdb/gosrv/pkg/fdbkv/kvpair" 
  "github.com/htner/sdb/gosrv/pkg/lakehouse"
	"github.com/htner/sdb/gosrv/pkg/types"
	"github.com/htner/sdb/gosrv/proto/sdb"

	_ "github.com/htner/sdb/gosrv/pkg/utils/logformat"
	log "github.com/sirupsen/logrus"
)

// server is used to implement proto.ScheduleServer
type LakeServer struct {
	sdb.UnimplementedLakeServer
	port int
}

func (s *LakeServer) Start(ctx context.Context, req *sdb.StartTransactionRequest) (*sdb.StartTransactionResponse, error) {
	log.Println("prepare start transaction ", req)

  var session kvpair.Session
  session.Uid = req.Uid 
  session.Id = types.SessionId(req.Sessionid)
  session.DbId = types.DatabaseId(req.Dbid)
  session.State = kvpair.SessionTransactionIdle
  session.ReadTranscationId = lakehouse.InvaildTranscaton
  session.WriteTranscationId = lakehouse.InvaildTranscaton
  lakehouse.WriteSession(&session)

  tr := lakehouse.NewTranscation(types.DatabaseId(req.Dbid), types.SessionId(req.Sessionid)) 
  tr.Start(true)

  return new(sdb.StartTransactionResponse), nil
}
func (s *LakeServer) Commit(ctx context.Context, req *sdb.CommitRequest) (*sdb.CommitResponse, error) {
  log.Println("commit ", req)
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
	log.Println("prepare insert request", request)
	lakeop := lakehouse.NewLakeRelOperator(
		types.DatabaseId(request.Dbid),
		types.SessionId(request.Sessionid),
		types.TransactionId(request.CommitXid))
	files, err := lakeop.PrepareFiles(types.RelId(request.Rel), request.Count)
	if err != nil {
		log.Println("PrepareFiles error: ", err)
		return nil, fmt.Errorf("mark files error %v", err)
	}
  return &sdb.PrepareInsertFilesResponse{Files: files}, nil
}

func (s *LakeServer) DeleteFiles(ctx context.Context, request *sdb.DeleteFilesRequest) (*sdb.DeleteFilesResponse, error) {
	log.Println("delete files", request)
	lakeop := lakehouse.NewLakeRelOperator(types.DatabaseId(request.Dbid),
		types.SessionId(request.Sessionid),
		types.TransactionId(request.CommitXid))

  /*
	for _, file := range request.RemoveFiles {
		var f kvpair.FileMeta
		f.Database = types.DatabaseId(request.Dbid)
		f.Relation = types.RelId(request.Rel)
		f.Filename = file.FileName

		removeFiles = append(removeFiles, &f)
	}
  */

	err := lakeop.DeleteFiles(types.RelId(request.Rel), request.RemoveFiles)
	if err != nil {
    log.Printf("delete files error: %s", err.Error())
		return nil, fmt.Errorf("insert files error")
	}
  /*
	err = lakeop.DeleleFiles(types.RelId(request.Rel), request.GetRemoveFiles())
	if err != nil {
		return nil, fmt.Errorf("delete files error")
	}
  */
	return &sdb.DeleteFilesResponse{}, nil
}


func (s *LakeServer) GetFileList(ctx context.Context, req *sdb.GetFilesRequest) (*sdb.GetFilesResponse, error) {
  log.Println("get file list", req)
	lakeop := lakehouse.NewLakeRelOperator(
		types.DatabaseId(req.Dbid),
		types.SessionId(req.Sessionid),
		types.TransactionId(req.CommitXid))
  files := make([]*sdb.LakeFileDetail, 0)
  var err error
  if req.GetIsWrite() {
    files, err = lakeop.GetAllFile(types.RelId(req.Rel), req.SliceCount, req.SliceSegIndex /*, types.TransactionId(req.ReadXid), types.TransactionId(req.CommitXid)*/)
  } else {
    //files, err = lakeop.GetAllFileForRead(types.RelId(req.Rel), req.ReadXid, req.CommitXid)
    files, err = lakeop.GetAllFile(types.RelId(req.Rel), req.SliceCount, req.SliceSegIndex/*, types.TransactionId(req.ReadXid), types.TransactionId(req.CommitXid)*/)
  }
  if err != nil {
    return nil, err
  }

  log.Println("get files from lake:", files)

  lakeFiles := make([]*sdb.LakeFile, 0)
  for _, file := range files {
    lakeFiles = append(lakeFiles, file.BaseInfo)
  }

  var response sdb.GetFilesResponse
  response.Files = lakeFiles

  log.Println("files :", lakeFiles, response.String())

  return &response, nil
}
