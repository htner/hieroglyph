package main

import (
	"context"
	"fmt"

	"github.com/htner/sdb/gosrv/pkg/fdbkv/keys"
	"github.com/htner/sdb/gosrv/pkg/lakehouse"
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

	var session sdb.Session
	session.Uid = req.Uid
	session.Id = req.Sessionid
	session.Dbid = req.Dbid
	session.State = keys.SessionTransactionIdle
	session.ReadTransactionId = lakehouse.InvaildTranscaton
	session.WriteTransactionId = lakehouse.InvaildTranscaton
  session.AutoCommit = false
  session.QueryId = 0
	lakehouse.WriteSession(&session)

	tr := lakehouse.NewTranscation(req.Dbid, req.Sessionid)
	tr.Start(false)

	return new(sdb.StartTransactionResponse), nil
}
func (s *LakeServer) Commit(ctx context.Context, req *sdb.CommitRequest) (*sdb.CommitResponse, error) {
	log.Println("commit ", req)
	tr := lakehouse.NewTranscation(req.Dbid, req.Sessionid)
	tr.Commit()
	return new(sdb.CommitResponse), nil
}

func (s *LakeServer) Abort(ctx context.Context, req *sdb.AbortRequest) (*sdb.AbortResponse, error) {
	tr := lakehouse.NewTranscation(req.Dbid, req.Sessionid)
	tr.Rollback()
	return new(sdb.AbortResponse), nil
}

func (s *LakeServer) AllocateXid(ctx context.Context, req *sdb.AllocateXidRequest) (*sdb.AllocateXidResponse, error) {
	tr := lakehouse.NewTranscation(req.Dbid, req.Sessionid)
	//tr.Commit()
	err := tr.ReadAble()
	if err != nil {
		return nil, err
	}

	err = tr.WriteAble()
	if err != nil {
		return nil, err
	}

	sess := tr.GetSession()

	var result sdb.AllocateXidResponse
	result.ReadXid = sess.ReadTransactionId
	result.WriteXid = sess.WriteTransactionId
	return &result, nil
}

// Depart implements proto.ScheduleServer
// It just returns commid
func (s *LakeServer) PrepareInsertFiles(ctx context.Context, request *sdb.PrepareInsertFilesRequest) (*sdb.PrepareInsertFilesResponse, error) {
	log.Println("prepare insert request", request)
	lakeop := lakehouse.NewLakeRelOperator(
		request.Dbid,
		request.Sessionid)
	files, err := lakeop.PrepareFiles(uint64(request.Rel), request.Count)
	if err != nil {
		log.Println("PrepareFiles error: ", err)
		return nil, fmt.Errorf("mark files error %v", err)
	}
	return &sdb.PrepareInsertFilesResponse{Files: files}, nil
}

func (s *LakeServer) DeleteFiles(ctx context.Context, request *sdb.DeleteFilesRequest) (*sdb.DeleteFilesResponse, error) {
	log.Println("delete files", request)
	lakeop := lakehouse.NewLakeRelOperator(request.Dbid, request.Sessionid)

	/*
		for _, file := range request.RemoveFiles {
			var f keys.FileMeta
			f.Database = uint64(request.Dbid)
			f.Relation = uint64(request.Rel)
			f.Filename = file.FileName

			removeFiles = append(removeFiles, &f)
		}
	*/

	err := lakeop.DeleteFiles(uint64(request.Rel), request.RemoveFiles)
	if err != nil {
		log.Printf("delete files error: %s", err.Error())
		return nil, fmt.Errorf("insert files error")
	}
	/*
		err = lakeop.DeleleFiles(uint64(request.Rel), request.GetRemoveFiles())
		if err != nil {
			return nil, fmt.Errorf("delete files error")
		}
	*/
	return &sdb.DeleteFilesResponse{}, nil
}

func (s *LakeServer) GetFileList(ctx context.Context, req *sdb.GetFilesRequest) (*sdb.GetFilesResponse, error) {
	log.Println("get file list", req)
	lakeop := lakehouse.NewLakeRelOperator(req.Dbid, req.Sessionid)
	files := make([]*sdb.LakeFileDetail, 0)
	var err error
	if req.GetIsWrite() {
		files, err = lakeop.GetAllFile(uint64(req.Rel), req.SliceCount, req.SliceSegIndex /*, uint64(req.ReadXid), uint64(req.CommitXid)*/)
	} else {
		//files, err = lakeop.GetAllFileForRead(uint64(req.Rel), req.ReadXid, req.CommitXid)
		files, err = lakeop.GetAllFile(uint64(req.Rel), req.SliceCount, req.SliceSegIndex /*, uint64(req.ReadXid), uint64(req.CommitXid)*/)
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
