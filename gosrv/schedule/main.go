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
	"log"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	pb "github.com/htner/sdb/gosrv/optimizer/proto"
)

const (
	defaultName = "world"
)

var (
	addr = flag.String("addr", "localhost:40000", "the address to connect to")
	name = flag.String("name", defaultName, "Name to greet")
)

func main() {
	flag.Parse()
	// Set up a connection to the server.
	conn, err := grpc.Dial(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
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
    log.Printf("%d->%s", i, slice.String())
  }

  // prepare segments

  // Slice Info

  // Send To Work
}
