package main

import (
	"fmt"

	"github.com/htner/sdb/gosrv/pkg/config"
	"github.com/htner/sdb/gosrv/pkg/schedule"
	"github.com/htner/sdb/gosrv/proto/sdb"

	_ "github.com/htner/sdb/gosrv/pkg/utils/logformat"
	log "github.com/sirupsen/logrus"
)

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
		Sql:          Q.request.Sql,
		QueryInfo:    nil,
		PlanInfo:     Q.optimizerResult.PlanstmtStr,
		//PlanInfoDxl: r.PlanDxlStr,
		PlanParams:  Q.optimizerResult.PlanParamsStr,
		GucVersion:  1,
		Workers:     workinfos,
		SliceTable:  Q.sliceTable,
		ResultDir:   "base/result",
		ResultSpace: conf,
		DbSpace:     conf,
		CatalogList: Q.catalogFiles,
		UserRelList: Q.userRelFiles,
	}
}

func (Q *QueryHandler) prepareSliceTable() error {
	workerMgr := schedule.NewWorkerMgr()
	log.Printf("slices: %v", Q.optimizerResult.Slices)
	slices := Q.optimizerResult.Slices
	// Q.workers, Q.workerSlices, err = workerMgr.GetServerSliceList(slices)
	// prepare segments
	Q.sliceTable.InstrumentOptions = 0
	Q.sliceTable.HasMotions = false
	// Slice Info
	Q.sliceTable.Slices = make([]*sdb.PBExecSlice, len(Q.optimizerResult.Slices))

	root_slice_count := 0

	if len(slices) == 0 {
		var err error
		Q.workers, Q.workerSlices, err = workerMgr.GetServerList(1, 1, 0)
		if err != nil {
			log.Printf("get server list error: %v", err)
			return err
		}
		root_slice_count = 1

	}

	for i, planSlice := range slices {
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

		/*
		  GANGTYPE_UNALLOCATED = 0
		  GANGTYPE_ENTRYDB_READER = 1
		  GANGTYPE_SINGLETON_READER = 2
		  GANGTYPE_PRIMARY_READER = 3
		  GANGTYPE_PRIMARY_WRITER = 4
		*/

		numSegments := planSlice.NumSegments
		// dispatchInfo := planSlice.DirectDispatchInfo
		switch planSlice.GangType {
		case schedule.GANGTYPE_UNALLOCATED:
			execSlice.PlanNumSegments = 1
		case schedule.GANGTYPE_ENTRYDB_READER:
			execSlice.PlanNumSegments = 1
		case schedule.GANGTYPE_SINGLETON_READER:
			execSlice.PlanNumSegments = 1
		//execSlice.Segments = dispatchInfo.Segments
		/*
			if dispatchInfo != nil && dispatchInfo.IsDirectDispatch {
			} else {
			execSlice.Segments = dispatchInfo.Segments
			}
		*/
		case schedule.GANGTYPE_PRIMARY_READER:
			execSlice.PlanNumSegments = numSegments
		case schedule.GANGTYPE_PRIMARY_WRITER:
			// FIXME
			execSlice.PlanNumSegments = numSegments
		default:
			execSlice.PlanNumSegments = 1
			//execSlice.PlanNumSegments = numSegments
		}

		if execSlice.SliceIndex == 0 {
			root_slice_count = int(execSlice.PlanNumSegments)
		}
		/*
			  segindex := int32(1)
				for k := int32(0); k < execSlice.PlanNumSegments; k++ {
					segindex++
				}
		*/
		workers, workerSlices, err := workerMgr.GetServerList(1, execSlice.PlanNumSegments, execSlice.SliceIndex)
		if err != nil {
			log.Printf("get server list error: %v", err)
			return err
		}

		for _, worker := range workers {
			execSlice.Segments = append(execSlice.Segments, worker.Segid)
			log.Printf("init segs %d(%d) %d->%d", execSlice.SliceIndex, execSlice.PlanNumSegments, worker.Segid, worker.Id)

			found := false
			for _, w := range Q.workers {
				if w.Id == worker.Id {
					found = true
					break
				}
			}
			if !found {
				Q.workers = append(Q.workers, worker)
			}
		}
		//
		// workers, workerSlices, err = workerMgr.GetServerSliceList(slices)
		Q.workerSlices = append(Q.workerSlices, workerSlices...)
		Q.sliceTable.Slices[i] = execSlice
	}

	log.Printf("init root count %d->%d", Q.newQueryId, root_slice_count)
	mgr := schedule.NewQueryMgr(Q.request.Dbid)
	err := mgr.InitQueryResult(Q.newQueryId, uint32(sdb.QueryStates_QueryInit), uint32(root_slice_count), "")
	if err != nil {
		log.Printf("InitQueryResult errro %v", err)
		return err
	}

	log.Println(Q.sliceTable.String())
	return nil
}
