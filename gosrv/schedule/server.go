package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"time"

	consulapi "github.com/hashicorp/consul/api"
	"github.com/htner/sdb/gosrv/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/reflection"
)

var (
	optimizerAddr = flag.String("optimizer_addr", "localhost:40000", "the address to connect to optimizer")
	workIp        = flag.String("worker_addr", "localhost", "the int to connect to worker")
	workPort      = flag.Int("worker_port", 40001, "the port to connect to worker")
	//name          = flag.String("name", defaultName, "Name to greet")
)

// rungRPC starts a gRPC greeting server on the given port
// This is blocking, so it should be run in a goroutine
// gRPC server is stopped when the context is cancelled
// gRPC server is using reflection
func rungRPC(done chan bool, port int) error {

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return fmt.Errorf("failed to listen: %w", err)
	}

	s := grpc.NewServer()
  proto.RegisterScheduleServer(s, &ScheduleServer{port: port})
	reflection.Register(s)

	go stopWhenDone(done, s)

	if err := s.Serve(lis); err != nil {
		return fmt.Errorf("failed to serve: %w", err)
	}

	return nil
}

func stopWhenDone(done chan bool, server *grpc.Server) {
	<-done
	server.GracefulStop()
}

// server is used to implement proto.ScheduleServer
type ScheduleServer struct {
	proto.UnimplementedScheduleServer
	port int
}

// Depart implements proto.ScheduleServer
// It just returns commid
/*
func (s *ScheduleServer) Depart(ctx context.Context, in *pb.ExecQueryRequest) (*pb.ExecQueryReply, error) {
	log.Printf("get request %s", in.Sql)
	return &pb.ExecQueryReply{}, nil
}
*/

func findNextFreePort() (int, error) {
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		return 0, fmt.Errorf("failed to listen: %w", err)
	}
	defer listener.Close()

	port := listener.Addr().(*net.TCPAddr).Port
	return port, nil
}

func main() {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, os.Kill)

	done := make(chan bool, 1)
	go rungRPC(done, 17000)
	registerService("127.0.0.1:8500", "SchedulerServer", 17000)
	<-c
}

func registerService(caddr, name string, port int) error {

	config := consulapi.DefaultConfig()
	config.Address = caddr

	consul, err := consulapi.NewClient(config)
	if err != nil {
		log.Printf("Failed to create consul client: %v", err)
	}

	registration := &consulapi.AgentServiceRegistration{
		Name:    name,
		ID:      name + "-service-" + fmt.Sprintf("%d", port),
		Port:    port,
		Address: "localhost",
		Tags:    []string{"public"},
	}

	err = consul.Agent().ServiceRegister(registration)
	if err != nil {
		log.Printf("Failed to register service: %v", err)
	}

	return nil

}

func (s *ScheduleServer) Depart(ctx context.Context, in *proto.ExecQueryRequest) (*proto.ExecQueryReply, error) {
	log.Printf("get request %s", in.Sql)
	// Set up a connection to the server.
	conn, err := grpc.Dial(*optimizerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := proto.NewOptimizerClient(conn)

	// Contact the server and print out its response.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*60)
	defer cancel()
	r, err := c.Optimize(ctx, &proto.OptimizeRequest{Name: "query", Sql: "select * from student"})
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
	var sliceTable proto.PBSliceTable
	sliceTable.InstrumentOptions = 0
	sliceTable.HasMotions = false
	// Slice Info
	sliceTable.Slices = make([]*proto.PBExecSlice, len(r.Slices))
	segindex := int32(1)
	for i, planSlice := range r.Slices {
		log.Printf("%d.%s", i, planSlice.String())
		execSlice := new(proto.PBExecSlice)
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
		go func(i int32, localSliceTable proto.PBSliceTable) {
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
			workClient := proto.NewWorkerClient(workConn)

			// Contact the server and print out its response.
			ctx, workCancel := context.WithTimeout(context.Background(), time.Second*60)
			defer workCancel()

			workinfos := make(map[int32]*proto.WorkerInfo, 0)
			for j := int32(1); j < 5; j++ {
				workinfo := &proto.WorkerInfo{
					Addr:  fmt.Sprintf("127.0.0.1:%d", 40000+j),
					Id:    int64(j),
					Segid: j,
				}
				workinfos[j] = workinfo
			}

			localSliceTable.LocalSlice = int32(sliceid)
			taskid := &proto.TaskIdentify{QueryId: 1, SliceId: int32(sliceid), SegId: i + 1}

			query := &proto.PrepareTaskRequest{
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

			query1 := &proto.StartTaskRequest{
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
	return &proto.ExecQueryReply{}, nil
}
