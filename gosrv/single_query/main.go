/*
 *
 * Copyright 2015 gRPC authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

// Package main implements a client for Greeter service.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/htner/sdb/gosrv/proto/sdb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	defaultName = "world"
)

var (
	optimizerAddr = flag.String("optimizer_addr", "localhost:40000", "the address to connect to optimizer")
	workIp        = flag.String("worker_addr", "localhost", "the int to connect to worker")
	workPort      = flag.Int("worker_port", 40001, "the port to connect to worker")
	name          = flag.String("name", defaultName, "Name to greet")
)

func main() {
	flag.Parse()

	// Set up a connection to the server.
	conn, err := grpc.Dial(*optimizerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := sdb.NewOptimizerClient(conn)

	// Contact the server and print out its response.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*60)
	defer cancel()
	r, err := c.Optimize(ctx, &sdb.OptimizeRequest{Name: *name, Sql: "select * from student"})
	if err != nil {
		log.Fatalf("could not greet: %v", err)
	}
	log.Printf("Greeting: %s %d %d %d", string(r.PlanDxlStr), len(r.PlanDxlStr), len(r.PlanstmtStr), len(r.PlanParamsStr))

	for i, slice := range r.Slices {
		if int32(i) != slice.SliceIndex {
			log.Fatalf("slice index not match %d.%s", i, slice.String())
		}
		log.Printf("%d.%s", i, slice.String())
	}

	// prepare segments
	var sliceTable sdb.PBSliceTable
	sliceTable.InstrumentOptions = 0
	sliceTable.HasMotions = false
	// Slice Info
	sliceTable.Slices = make([]*sdb.PBExecSlice, len(r.Slices))
	segindex := int32(1)
	for i, planSlice := range r.Slices {
		log.Printf("%d.%s", i, planSlice.String())
		execSlice := new(sdb.PBExecSlice)
		execSlice.SliceIndex = planSlice.SliceIndex
		execSlice.PlanNumSegments = planSlice.NumSegments

		rootIndex := int32(0)
		parentIndex := planSlice.ParentIndex
		if parentIndex < -1 || int(parentIndex) >= len(r.Slices) {
			log.Fatal("invalid parent slice index %d", parentIndex)
		}
		if parentIndex >= 0 {
			parentExecSlice := sliceTable.Slices[parentIndex]
			children := parentExecSlice.Children
			if children == nil {
				children = make([]int32, 0)
			}
			parentExecSlice.Children = append(children, execSlice.SliceIndex)

			rootIndex = execSlice.SliceIndex
			count := 0
			for r.Slices[rootIndex].ParentIndex >= 0 {
				rootIndex = r.Slices[rootIndex].ParentIndex

				count++
				if count > len(r.Slices) {
					log.Fatal("circular parent-child relationship")
				}
			}
			sliceTable.HasMotions = true
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
		sliceTable.Slices[i] = execSlice
	}
	log.Println(sliceTable.String())

	log.Printf("------------------------------------")

	for i := int32(0); i < 4; i++ {
		// Send To Work
		go func(i int32, localSliceTable sdb.PBSliceTable) {
			sliceid := 0
			if i > 0 {
				sliceid = 1
			}
			//localSliceTable := sliceTable
			addr := fmt.Sprintf("%s:%d", *workIp, *workPort+int(i))
			log.Printf("addr:%s", addr)

			workConn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				log.Fatalf("did not connect: %v", err)
			}
			defer workConn.Close()
			workClient := sdb.NewWorkerClient(workConn)

			// Contact the server and print out its response.
			ctx, workCancel := context.WithTimeout(context.Background(), time.Second*60)
			defer workCancel()

			workinfos := make(map[int32]*sdb.WorkerInfo, 0)
			for j := int32(1); j < 5; j++ {
				workinfo := &sdb.WorkerInfo{
					Addr:  fmt.Sprintf("127.0.0.1:%d", 40000+j),
					Id:    int64(j),
					Segid: j,
				}
				workinfos[j] = workinfo
			}

			localSliceTable.LocalSlice = int32(sliceid)
			taskid := &sdb.TaskIdentify{QueryId: 1, SliceId: int32(sliceid), SegId: i + 1}

			query := &sdb.PrepareTaskRequest{
				TaskIdentify: taskid,
				Sessionid:    1,
				Uid:          1,
				Dbid:         1,
				MinXid:       1,
				MaxXid:       1,
				Sql:          "select * from student",
				QueryInfo:    nil,
				PlanInfo:     r.PlanstmtStr,
				//PlanInfoDxl: r.PlanDxlStr,
				PlanParams: r.PlanParamsStr,
				GucVersion: 1,
				Workers:    workinfos,
				SliceTable: &localSliceTable,
			}

			reply, err := workClient.Prepare(ctx, query)
			log.Println("----- query -----")
			log.Println(query)
			if err != nil {
				log.Fatalf("could not prepare: %v", err)
			}
			if reply != nil {
				log.Printf("prepare reply: %v", reply.String())
			}

			time.Sleep(2 * time.Second)
			// all ok , start request
			query1 := &sdb.StartTaskRequest{
				TaskIdentify: taskid,
			}

			reply1, err := workClient.Start(ctx, query1)
			if err != nil {
				log.Fatalf("could not start: %v", err)
			}
			if reply1 != nil {
				log.Printf("start reply: %v", reply1.String())
			}
			time.Sleep(2 * time.Second)
		}(i, sliceTable)

		log.Printf("start next: %d", i)
		time.Sleep(1 * time.Second)
	}

	time.Sleep(160 * time.Second)
}
