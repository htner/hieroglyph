package main

import (
	"time"

	"github.com/htner/sdb/gosrv/pkg/lakehouse"
	"github.com/htner/sdb/gosrv/pkg/schedule"
	"github.com/htner/sdb/gosrv/pkg/utils/postgres"
	"github.com/htner/sdb/gosrv/proto/sdb"

	_ "github.com/htner/sdb/gosrv/pkg/utils/logformat"
	log "github.com/sirupsen/logrus"
)

type QueryHandler struct {
	request *sdb.ExecQueryRequest

	optimizerResult *sdb.OptimizeReply

	baseWorkerQuery *sdb.PrepareTaskRequest
	sliceTable      *sdb.PBSliceTable
	workers         []*sdb.WorkerInfo
	workerSlices    []*sdb.WorkerSliceInfo

	catalogFiles []*sdb.RelFiles
	userRelFiles []*sdb.RelFiles

	lasterr    error
	newQueryId uint64
}

func (Q *QueryHandler) prelock() error {
	// if Q.request. CHECK CMD TYPE
	var mgr lakehouse.LockMgr

  updates := make([]uint64, 0)
  updates = append(updates, Q.optimizerResult.UpdateRels...)
  updates = append(updates, Q.optimizerResult.DeleteRels...)

	err := mgr.PreLock(Q.request.Dbid, Q.request.Sid, updates)
	if err != nil {
		log.Printf("lock result error: %v", err)
		return err
	}
	return nil
}

func (Q *QueryHandler) run(req *sdb.ExecQueryRequest) (uint64, error) {
	// NewQueryId
	newQueryId := uint64(time.Now().UnixMilli())
	Q.newQueryId = newQueryId

	Q.request = req
	Q.sliceTable = new(sdb.PBSliceTable)
	log.Println("get exec query request ", req)
	// Set up a connection to the server.

	// start transtion
	tr := lakehouse.NewTranscation(req.Dbid, req.Sid)
  err := tr.NewQuery(Q.newQueryId)
  if err != nil && err != lakehouse.ErrTransactionStarted {
    return 0, nil
  }

	//catalogFiles := make(map[uint32][]*sdb.LakeFileDetail)
	lakeop := lakehouse.NewLakeRelOperator(req.Dbid, req.Sid)
	Q.catalogFiles = make([]*sdb.RelFiles, 0)
	for oid := range postgres.CatalogNames {
		relLakeList, err := lakeop.GetRelLakeList(uint64(oid))
		if err != nil {
      log.Println("get lake file fail:", req, err)
			return 0, nil
		}
		Q.catalogFiles = append(Q.catalogFiles, relLakeList)
	}


	mgr := schedule.NewQueryMgr(uint64(req.Dbid))
	err = mgr.WriterQueryDetail(req, newQueryId)
	if err != nil {
		return 0, err
	}
  go Q.runReal(tr)
  return Q.newQueryId, nil
}

func (Q *QueryHandler) runReal(tr *lakehouse.Transaction) error {
  mgr := schedule.NewQueryMgr(Q.request.Dbid)

  isProcessedByUtility, err := Q.processUtility()
  if isProcessedByUtility {
    return nil
  }

	err = Q.optimize()
	if err != nil {
    if Q.optimizerResult == nil {
      var result sdb.ErrorResponse
      result.Code = "5800"
      result.Message = "optimizer access failure"
      mgr.WriteErrorQueryResult(Q.newQueryId, uint32(sdb.QueryStates_QueryError), &result)
    } else {
      mgr.WriteErrorQueryResult(Q.newQueryId, uint32(sdb.QueryStates_QueryError), Q.optimizerResult.Message)
    }
    tr.Rollback()
		return err
	}

	err = Q.prelock()
	if err != nil {
		log.Printf("lock result error: %v", err)
    mgr.QueryError(Q.newQueryId, uint32(sdb.QueryStates_QueryError), "40P01", err.Error())
    tr.Rollback()
		return err
	}

	err = mgr.WriterOptimizerResult(Q.optimizerResult, Q.newQueryId)
	if err != nil {
		log.Printf("write optimize result error: %v", err)
    mgr.QueryError(Q.newQueryId, uint32(sdb.QueryStates_QueryError), "58000", err.Error())
    tr.Rollback()
		return err
	}

	err = Q.prepareSliceTable()
	if err != nil {
		log.Printf("prepareSliceTable error: %v", err)
    mgr.QueryError(Q.newQueryId, uint32(sdb.QueryStates_QueryError), "58000", err.Error())
    tr.Rollback()
		return err
	}

	Q.buildPrepareTaskRequest()

	err = mgr.WriterWorkerInfo(Q.baseWorkerQuery)
	if err != nil {
    mgr.QueryError(Q.newQueryId, uint32(sdb.QueryStates_QueryError), "58000", err.Error())
    tr.Rollback()
		return err
	}

	Q.prepareWorker()

	Q.startWorkers()

	/*


		resRepy := &sdb.ExecQueryReply{
			QueryId:   query.TaskIdentify.QueryId,
			Sessionid: query.Sessionid,
			Uid:       query.Uid,
			Dbid:      query.Dbid,
			ResultDir: query.ResultDir,
		}

		return resRepy, nil
	*/
	return nil
}


