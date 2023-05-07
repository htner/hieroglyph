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

	pb "github.com/htner/sdb/gosrv/optimizer/proto"
	wpb "github.com/htner/sdb/gosrv/worker/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	defaultName = "world"
)

var (
	optimizerAddr = flag.String("optimizer_addr", "localhost:40000", "the address to connect to optimizer")
	workIp = flag.String("worker_addr", "localhost", "the int to connect to worker")
	workPort = flag.Int("worker_port", 40001, "the port to connect to worker")
	name = flag.String("name", defaultName, "Name to greet")
)

func main() {
	flag.Parse()

	// Set up a connection to the server.
	conn, err := grpc.Dial(*optimizerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := pb.NewOptimizerClient(conn)

	// Contact the server and print out its response.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second * 60)
	defer cancel()
  r, err := c.Optimize(ctx, &pb.OptimizeRequest{Name: *name, Sql: "select * from student"})
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

  //rootSliceIndex := 0




  // prepare segments

  var sliceTable wpb.PBSliceTable
  sliceTable.InstrumentOptions = 0
  sliceTable.HasMotions = false
  // Slice Info
  sliceTable.Slices = make([]*wpb.PBExecSlice, len(r.Slices))
  for i, planSlice := range r.Slices {
      log.Printf("%d.%s", i, planSlice.String())
      execSlice := new(wpb.PBExecSlice)
      execSlice.SliceIndex= planSlice.SliceIndex
      execSlice.PlanNumSegments = planSlice.NumSegments

      rootIndex := int32(0)
      parentIndex := planSlice.ParentIndex
      if (parentIndex < -1 || int(parentIndex) >= len(r.Slices)) {
        log.Fatal("invalid parent slice index %d", parentIndex)
      }
      if (parentIndex >= 0) {
        parentExecSlice := sliceTable.Slices[parentIndex]
        children := parentExecSlice.Children
        if children == nil {
            children = make([]int32, 0)
        }
        parentExecSlice.Children = append(children, execSlice.SliceIndex)

        rootIndex = execSlice.SliceIndex
        count := 0
        for (r.Slices[rootIndex].ParentIndex >= 0) {
            rootIndex = r.Slices[rootIndex].ParentIndex
            
            count++
            if (count > len(r.Slices)) {
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
      dispatchInfo := planSlice.DirectDispatchInfo
      switch planSlice.GangType {
        case 0:
          execSlice.PlanNumSegments = 1 
        case 1:
          fallthrough
        case 2:
          execSlice.PlanNumSegments = numSegments
          if dispatchInfo != nil && dispatchInfo.IsDirectDispatch {
            execSlice.Segments = dispatchInfo.Segments
          } else {
            execSlice.Segments = dispatchInfo.Segments
          }
        case 3:
          execSlice.PlanNumSegments = 1 
        case 4:
          execSlice.PlanNumSegments = 1 
      }
      sliceTable.Slices[0] = execSlice
    } 

  for i := 0; i < 4; i++ { 
    // Send To Work
    sliceTable.LocalSlice = int32(i)
    addr := fmt.Sprintf("%s:%d", *workIp, *workPort + i)

    workConn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
    if err != nil {
      log.Fatalf("did not connect: %v", err)
    }
    defer workConn.Close()
    workClient := wpb.NewWorkerClient(workConn)

    // Contact the server and print out its response.
    ctx, workCancel := context.WithTimeout(context.Background(), time.Second * 60)
    defer workCancel()

    workinfo := &wpb.WorkerInfo {
      Addr: "127.0.0.1:40001",
      Id : 1,
      Segid: 1,
    } 

    workinfos := make([]*wpb.WorkerInfo, 0)
    workinfos = append(workinfos, workinfo)

    query := &wpb.QueryRequest {
      QueryId: 1,
      Sessionid: 1,
      Uid: 1,
      Dbid: 1,
      SliceId: 0,
      MinXid: 1,
      MaxXid: 1,
      Sql: "select * from student",
      QueryInfo: nil,
      PlanInfo: r.PlanstmtStr,
      PlanInfoDxl: r.PlanDxlStr,
      PlanParams: r.PlanParamsStr,
      GucVersion: 1,
      Workers: workinfos,
      SliceTable: &sliceTable,
    }

    reply, err := workClient.Exec(ctx, query)
    if err != nil {
      log.Fatalf("could not greet: %v", err)
    }
    log.Fatalln("could not greet: ", reply)
  }

}
