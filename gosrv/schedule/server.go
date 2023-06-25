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
	"github.com/htner/sdb/gosrv/pkg/schedule"
	"github.com/htner/sdb/gosrv/pkg/types"
	"github.com/htner/sdb/gosrv/proto/sdb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/reflection"
	"google.golang.org/protobuf/proto"
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
	sdb.RegisterScheduleServer(s, &ScheduleServer{port: port})
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
	sdb.UnimplementedScheduleServer
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
	go rungRPC(done, 10001)
	registerService("127.0.0.1:8500", "SchedulerServer", 10001)
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

func (c *ScheduleServer) WorkerReportResult(ctx context.Context, in *sdb.WorkerResultReportRequest) (*sdb.WorkerResultReportReply, error) {
	out := new(sdb.WorkerResultReportReply)
	mgr := schedule.NewQueryMgr(types.DatabaseId(in.Dbid))
	err := mgr.WriterExecResult(in)
	return out, err
}

func (s *ScheduleServer) Depart(ctx context.Context, in *sdb.ExecQueryRequest) (*sdb.ExecQueryReply, error) {
	log.Printf("get request %s", in.Sql)
	// Set up a connection to the server.

	mgr := schedule.NewQueryMgr(types.DatabaseId(in.Dbid))
	err := mgr.WriterQueryDetail(in)
	if err != nil {
		return nil, err
	}

	conn, err := grpc.Dial(*optimizerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := sdb.NewOptimizerClient(conn)

	// Contact the server and print out its response.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*60)
	defer cancel()
	optimizerResult, err := c.Optimize(ctx, &sdb.OptimizeRequest{Name: "query", Sql: "select * from student"})
	if err != nil {
		log.Fatalf("could not greet: %v", err)
	}
	err = mgr.WriterOptimizerResult(0, optimizerResult)
	if err != nil {
		return nil, err
	}

	log.Printf("Greeting: %s %d %d %d", string(optimizerResult.PlanDxlStr), len(optimizerResult.PlanDxlStr), len(optimizerResult.PlanstmtStr), len(optimizerResult.PlanParamsStr))

	var workerMgr schedule.WorkerMgr
	workerList, workerSliceList, err := workerMgr.GetServerSliceList(optimizerResult.Slices)
	if err != nil {
		return nil, err
	}
	// prepare segments
	var sliceTable sdb.PBSliceTable
	sliceTable.InstrumentOptions = 0
	sliceTable.HasMotions = false
	// Slice Info
	sliceTable.Slices = make([]*sdb.PBExecSlice, len(optimizerResult.Slices))
	segindex := int32(1)
	for i, planSlice := range optimizerResult.Slices {
		log.Printf("%d.%s", i, planSlice.String())
		execSlice := new(sdb.PBExecSlice)
		execSlice.SliceIndex = planSlice.SliceIndex
		execSlice.PlanNumSegments = planSlice.NumSegments

		rootIndex := int32(0)
		parentIndex := planSlice.ParentIndex
		if parentIndex < -1 || int(parentIndex) >= len(optimizerResult.Slices) {
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
			for optimizerResult.Slices[rootIndex].ParentIndex >= 0 {
				rootIndex = optimizerResult.Slices[rootIndex].ParentIndex

				count++
				if count > len(optimizerResult.Slices) {
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

	workinfos := make(map[int32]*sdb.WorkerInfo, 0)
	//workerList
	for _, worker := range workerList {
		//workinfo := &sdb.WorkerInfo{
		//	Addr:  fmt.Sprintf("127.0.0.1:%d", 40000+j),
		//	Id:    int64(j),
		//	Segid: j,
		//}
		workinfos[worker.Id] = worker
	}

	query := sdb.PrepareTaskRequest{
		TaskIdentify: &sdb.TaskIdentify{QueryId: 1, SliceId: 0, SegId: 0},
		Sessionid:    1,
		Uid:          1,
		Dbid:         1,
		MinXid:       1,
		MaxXid:       1,
		Sql:          "select * from student",
		QueryInfo:    nil,
		PlanInfo:     optimizerResult.PlanstmtStr,
		//PlanInfoDxl: r.PlanDxlStr,
		PlanParams: optimizerResult.PlanParamsStr,
		GucVersion: 1,
		Workers:    workinfos,
		SliceTable: &sliceTable,
	}

	err = mgr.WriterExecDetail(query)
	if err != nil {
		return nil, err
	}

	for _, workerSlice := range workerSliceList {
		// Send To Work
		go func(query sdb.PrepareTaskRequest, worker *sdb.WorkerSliceInfo) {
			sliceid := 0
			//localSliceTable.LocalSlice = int32(sliceid)
			taskid := &sdb.TaskIdentify{QueryId: 1, SliceId: int32(worker.Sliceid), SegId: worker.Segid}
			query.TaskIdentify = taskid
			query.SliceTable.LocalSlice = int32(sliceid)

			//localSliceTable := sliceTable
			//addr := fmt.Sprintf("%s:%d", *workIp, *workPort+int(i))
			log.Printf("addr:%s", worker.WorkerInfo.Addr)

			workConn, err := grpc.Dial(worker.WorkerInfo.Addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				log.Fatalf("did not connect: %v", err)
			}
			defer workConn.Close()
			workClient := sdb.NewWorkerClient(workConn)

			// Contact the server and print out its response.
			ctx, workCancel := context.WithTimeout(context.Background(), time.Second*60)
			defer workCancel()

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
		}(proto.Clone(query), workerSlice)
		time.Sleep(1 * time.Second)
	}

	time.Sleep(160 * time.Second)
	return &sdb.ExecQueryReply{}, nil
}
