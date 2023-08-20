package main

import (
	"context"
	"sync"
	"time"

	"github.com/htner/sdb/gosrv/proto/sdb"

	_ "github.com/htner/sdb/gosrv/pkg/utils/logformat"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"google.golang.org/protobuf/proto"
)

func (Q *QueryHandler) prepareWorker() bool {
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

