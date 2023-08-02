package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/htner/sdb/gosrv/pkg/config"
	"github.com/htner/sdb/gosrv/pkg/lakehouse"
	"github.com/htner/sdb/gosrv/pkg/schedule"
	"github.com/htner/sdb/gosrv/pkg/types"
	"github.com/htner/sdb/gosrv/pkg/utils/postgres"
	"github.com/htner/sdb/gosrv/proto/sdb"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	//"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
)

type QueryHandler struct {
	request *sdb.ExecQueryRequest

	optimizerResult *sdb.OptimizeReply

  baseWorkerQuery *sdb.PrepareTaskRequest
	sliceTable   *sdb.PBSliceTable
	workers      []*sdb.WorkerInfo
	workerSlices []*sdb.WorkerSliceInfo

  rel_versions []*sdb.LakeRelVersion

	lasterr error
  newQueryId uint64
  reloadCatalogOid []uint32
}

func (Q *QueryHandler) run(req *sdb.ExecQueryRequest) (uint64, error) {
	Q.request = req
  Q.sliceTable = new(sdb.PBSliceTable)
	log.Println("get request ", req)
	// Set up a connection to the server.

	// start transtion
	tr := lakehouse.NewTranscation(types.DatabaseId(req.Dbid), types.SessionId(req.Sid))
	tr.Start(true)

  //catalogFiles := make(map[uint32][]*sdb.LakeFileDetail)
	//lakeop := lakehouse.NewLakeRelOperator(types.DatabaseId(req.Dbid), types.SessionId(req.Sid), lakehouse.InvaildTranscaton)
	for oid := range postgres.CatalogNames {
		//files, err := lakeop.GetAllFileForRead(types.RelId(oid))
		//if err != nil {
		//}
		//catalogFiles[oid] = files
    Q.reloadCatalogOid = append(Q.reloadCatalogOid, oid)
	}

	// NewQueryId
	newQueryId := uint64(time.Now().UnixMilli())
  Q.newQueryId = newQueryId

	mgr := schedule.NewQueryMgr(types.DatabaseId(req.Dbid))
	err := mgr.WriterQueryDetail(req, newQueryId)
	if err != nil {
		return 0, err
	}

	err = Q.optimize()
	if err != nil {
		return 0, err
	}

	err = mgr.WriterOptimizerResult(Q.optimizerResult, newQueryId)
	if err != nil {
		log.Printf("write optimize result error: %v", err)
		return 0, err
	}

  err = Q.prepareSliceTable()
	if err != nil {
		log.Printf("prepareSliceTable error: %v", err)
		return 0, err
	}

  Q.buildPrepareTaskRequest()

	err = mgr.WriterWorkerInfo(Q.baseWorkerQuery)
	if err != nil {
		return 0, err
	}

  Q.prepare()

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
  return newQueryId, nil
}

func (Q *QueryHandler) buildPrepareTaskRequest() {
	workinfos := make(map[int32]*sdb.WorkerInfo, 0)
	//workerList
	for _, worker := range Q.workers {
		workinfos[worker.Segid] = worker
	}

  var spaceConfig config.LakeSpaceConfig
  conf, _ := spaceConfig.GetConfig(Q.request.Dbid)

	Q.baseWorkerQuery = &sdb.PrepareTaskRequest{
		TaskIdentify: &sdb.TaskIdentify{QueryId: Q.newQueryId, SliceId: 0, SegId: 0},
		Sessionid:    Q.request.Sid,
		Uid:          Q.request.Uid,
		Dbid:         Q.request.Dbid,
		Sql:       Q.request.Sql,
		QueryInfo: nil,
		PlanInfo:  Q.optimizerResult.PlanstmtStr,
		//PlanInfoDxl: r.PlanDxlStr,
		PlanParams: Q.optimizerResult.PlanParamsStr,
		GucVersion: 1,
		Workers:    workinfos,
		SliceTable: Q.sliceTable,
		ResultDir:  "base/result",
    ResultSpace: conf,
    DbSpace: conf, 
	}
}


func (Q *QueryHandler) prepareSliceTable() error {
	var workerMgr schedule.WorkerMgr
	log.Printf("slices: %v", Q.optimizerResult.Slices)
	slices := Q.optimizerResult.Slices
  var err error
	Q.workers, Q.workerSlices, err = workerMgr.GetServerSliceList(slices)
	if err != nil {
		log.Printf("get server list error: %v", err)
		return err
	}
	// prepare segments
	Q.sliceTable.InstrumentOptions = 0
	Q.sliceTable.HasMotions = false
	// Slice Info
	Q.sliceTable.Slices = make([]*sdb.PBExecSlice, len(Q.optimizerResult.Slices))
	segindex := int32(1)
	for i, planSlice := range Q.optimizerResult.Slices {
		log.Printf("%d.%s", i, planSlice.String())
		execSlice := new(sdb.PBExecSlice)
		execSlice.SliceIndex = planSlice.SliceIndex
		execSlice.PlanNumSegments = planSlice.NumSegments

		rootIndex := int32(0)
		parentIndex := planSlice.ParentIndex
		if parentIndex < -1 || int(parentIndex) >= len(Q.optimizerResult.Slices) {
			log.Errorf("invalid parent slice index %d", parentIndex)
			return fmt.Errorf("get server list error")
		}
		if parentIndex >= 0 {
			parentExecSlice := Q.sliceTable.Slices[parentIndex]
			children := parentExecSlice.Children
			if children == nil {
				children = make([]int32, 0)
			}
			parentExecSlice.Children = append(children, execSlice.SliceIndex)

			rootIndex = execSlice.SliceIndex
			count := 0
			for Q.optimizerResult.Slices[rootIndex].ParentIndex >= 0 {
				rootIndex = Q.optimizerResult.Slices[rootIndex].ParentIndex

				count++
				if count > len(Q.optimizerResult.Slices) {
					log.Errorf("circular parent-child relationship")
					return fmt.Errorf("?")
				}
			}
			Q.sliceTable.HasMotions = true
		} else {
			rootIndex = int32(i)
		}
		execSlice.ParentIndex = parentIndex
		execSlice.RootIndex = rootIndex
		execSlice.GangType = planSlice.GangType

		numSegments := planSlice.NumSegments
		// dispatchInfo := planSlice.DirectDispatchInfo
		switch planSlice.GangType {
		case 0:
			execSlice.PlanNumSegments = 1
		case 1:
			fallthrough
		case 2:
			execSlice.PlanNumSegments = 1
		//execSlice.Segments = dispatchInfo.Segments
		/*
			if dispatchInfo != nil && dispatchInfo.IsDirectDispatch {
			} else {
			execSlice.Segments = dispatchInfo.Segments
			}
		*/
		case 3:
			fallthrough
		case 4:
			fallthrough
		default:
			execSlice.PlanNumSegments = numSegments
		}

		for k := int32(0); k < execSlice.PlanNumSegments; k++ {
			execSlice.Segments = append(execSlice.Segments, segindex)
			log.Printf("init segs %d(%d) %d/%d->%d", execSlice.SliceIndex, planSlice.GangType, k, execSlice.PlanNumSegments, segindex)
			segindex++
		}
		Q.sliceTable.Slices[i] = execSlice
	}
	log.Println(Q.sliceTable.String())
  return nil
}

func (Q *QueryHandler) prepare() bool {
	var wg sync.WaitGroup
	wg.Add(len(Q.workerSlices))
	allSuccess := true
	for _, workerSlice := range Q.workerSlices {
		// Send To Work
		var cloneTask *sdb.PrepareTaskRequest
		cloneTask = proto.Clone(Q.baseWorkerQuery).(*sdb.PrepareTaskRequest)
		//proto.Clone(&workerSlice)

		go func(query *sdb.PrepareTaskRequest, workerSlice *sdb.WorkerSliceInfo) {
			defer wg.Done()
			sliceid := workerSlice.Sliceid
			//localSliceTable.LocalSlice = int32(sliceid)
			taskid := &sdb.TaskIdentify{QueryId: Q.newQueryId, SliceId: int32(sliceid), SegId: workerSlice.WorkerInfo.Segid}
			query.TaskIdentify = taskid
			query.SliceTable.LocalSlice = int32(sliceid)
      query.ReloadCatalogOid = Q.reloadCatalogOid

			//localSliceTable := sliceTable
			//addr := fmt.Sprintf("%s:%d", *workIp, *workPort+int(i))
			log.Printf("addr:%s", workerSlice.WorkerInfo.Addr)

			workConn, err := grpc.Dial(workerSlice.WorkerInfo.Addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				log.Errorf("did not connect: %v", err)
				//return nil, fmt.Errorf("?")
				allSuccess = false
				return
			}
			defer workConn.Close()
			workClient := sdb.NewWorkerClient(workConn)

			// Contact the server and print out its response.
			ctx, workCancel := context.WithTimeout(context.Background(), time.Second*6000)
			defer workCancel()

			reply, err := workClient.Prepare(ctx, query)
			log.Println("----- query -----")
			log.Println(query)
			if err != nil {
				log.Errorf("could not prepare: %v", err)
				allSuccess = false
				return
			}
			if reply != nil {
				log.Errorf("prepare reply: %v", reply.String())
				allSuccess = false
				return
			}
		}(cloneTask, workerSlice)
	}

	wg.Wait()
  return allSuccess
}

func (Q *QueryHandler) startWorkers() bool {
	var wg sync.WaitGroup
	wg.Add(len(Q.workerSlices))
	allSuccess := true

  i := 0
  log.Println("dddtest worker slices ", Q.workerSlices)
  for _, workerSlice := range Q.workerSlices {
    // Send To Work
    taskid := sdb.TaskIdentify{QueryId: Q.newQueryId, SliceId: int32(workerSlice.Sliceid), SegId: workerSlice.WorkerInfo.Segid}
    if i > 0 {
      time.Sleep(2 * time.Second)
      i++
    }
    go func(tid sdb.TaskIdentify, addr string) {
      defer wg.Done()
      log.Printf("addr:%s", addr)

      workConn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
      if err != nil {
        log.Errorf("did not connect: %v", err)
        return
      }
      defer workConn.Close()
      workClient := sdb.NewWorkerClient(workConn)

      // Contact the server and print out its response.
      ctx, workCancel := context.WithTimeout(context.Background(), time.Second*60)
      defer workCancel()
      query := &sdb.StartTaskRequest{
        TaskIdentify: &taskid,
      }

      reply, err := workClient.Start(ctx, query)
      if err != nil {
        log.Errorf("could not start: %v", err)
        return
      }
      if reply != nil {
        log.Printf("start reply: %v", reply.String())
      }
    }(taskid, workerSlice.WorkerInfo.Addr)
  }
  wg.Wait()
  return allSuccess
}

func (Q *QueryHandler) optimize() (error) {
  conn, err := grpc.Dial(*optimizerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
  if err != nil {
    log.Fatalf("did not connect: %v", err)
  }
  defer conn.Close()
  c := sdb.NewOptimizerClient(conn)

  var spaceConfig config.LakeSpaceConfig
  conf, _ := spaceConfig.GetConfig(Q.request.Dbid)
  // Contact the server and print out its response.
  ctx, cancel := context.WithTimeout(context.Background(), time.Second*6000)
  defer cancel()
  req := &sdb.OptimizeRequest{
    Name : "query",
    Sql  : Q.request.Sql,
    ReloadCatalogOid : Q.reloadCatalogOid,
    DbSpace: conf,
  }
  optimizerResult, err := c.Optimize(ctx, req)
  if err != nil {
    log.Printf("could not optimize: %v", err)
    return fmt.Errorf("optimizer error")
  }

  Q.optimizerResult = optimizerResult
  log.Printf("Greeting: %s %d %d %d", string(optimizerResult.PlanDxlStr), len(optimizerResult.PlanDxlStr), len(optimizerResult.PlanstmtStr), len(optimizerResult.PlanParamsStr))
  return nil
}
