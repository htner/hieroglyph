package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	consulapi "github.com/hashicorp/consul/api"
	"github.com/htner/sdb/gosrv/pkg/lakehouse"
	"github.com/htner/sdb/gosrv/pkg/schedule"
	"github.com/htner/sdb/gosrv/pkg/service"
	"github.com/htner/sdb/gosrv/pkg/types"
	"github.com/htner/sdb/gosrv/pkg/utils/slog"
	"github.com/htner/sdb/gosrv/proto/sdb"
	"github.com/htner/sdb/gosrv/pkg/utils/postgres"
	log "github.com/sirupsen/logrus"
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
	fdb.MustAPIVersion(710)
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, os.Kill)
	slog.SetLogger(true)

	port := 10002
	done := make(chan bool, 1)
	go rungRPC(done, port)
	registerService("127.0.0.1:8500", service.ScheduleName(), port)
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

func (c *ScheduleServer) PushWorkerResult(ctx context.Context, in *sdb.PushWorkerResultRequest) (*sdb.PushWorkerResultReply, error) {
  out := new(sdb.PushWorkerResultReply)
	mgr := schedule.NewQueryMgr(types.DatabaseId(in.Result.Dbid))
	err := mgr.WriterWorkerResult(in)
	return out, err
}


func (s *ScheduleServer) Depart(ctx context.Context, in *sdb.ExecQueryRequest) (*sdb.ExecQueryReply, error) {
	log.Printf("get request %s", in.Sql)
	// Set up a connection to the server.

  // start transtion
  tr := lakehouse.NewTranscation(1, 1) 
  tr.Start(true)

  catalogFiles := make(map[uint32][]*sdb.LakeFileDetail)
  lakeop := lakehouse.NewLakeRelOperator(1, 1, tr.Xid)
  for oid, _:= range postgres.CatalogNames {
    files,  err := lakeop.GetAllFileForRead(types.RelId(oid), 1, 1)
    if err != nil {
		log.Printf("GetAllFileForRead failed %v", err)
		continue
    }
	log.Printf("dddtest oid = %d, files:%v", oid, files)
    catalogFiles[oid] = files
  }

	mgr := schedule.NewQueryMgr(types.DatabaseId(in.Dbid))
	err := mgr.WriterQueryDetail(in)
	if err != nil {
		return nil, err
	}
	
	var conn *grpc.ClientConn
    var optimizerResult *sdb.OptimizeReply
	for port := 40000; port > 39980; port-- {
		log.Printf("pick port %d to connect optimizer", port)
		conn, err = grpc.Dial(fmt.Sprintf("localhost:%d", port), grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Fatalf("did not connect: %v", err)
			time.Sleep(1)
			continue
		}

		defer conn.Close()
		c := sdb.NewOptimizerClient(conn)

	// Contact the server and print out its response.
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*60)
		defer cancel()
		optimizerResult, err = c.Optimize(ctx, &sdb.OptimizeRequest{Name: "query", Sql: in.Sql})
		if err != nil {
			log.Printf("could not optimize: %v", err)
			time.Sleep(1)
			continue
		}
		break;
	}

	if err != nil {
		log.Printf("finalize could not optimize: %v", err)
		return nil, fmt.Errorf("optimizer error")
	}

	err = mgr.WriterOptimizerResult(0, optimizerResult)
	if err != nil {
		log.Printf("write optimize result error: %v", err)
		return nil, err
	}


	log.Printf("Greeting: %s %d %d %d", string(optimizerResult.PlanDxlStr), len(optimizerResult.PlanDxlStr), len(optimizerResult.PlanstmtStr), len(optimizerResult.PlanParamsStr))

	var workerMgr schedule.WorkerMgr
	log.Printf("slices: %v", optimizerResult.Slices)
	workerList, workerSliceList, err := workerMgr.GetServerSliceList(optimizerResult.Slices)
	if err != nil {
		log.Printf("get server list error: %v", err)
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
			log.Errorf("invalid parent slice index %d", parentIndex)
			return nil, fmt.Errorf("get server list error")
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
					log.Errorf("circular parent-child relationship")
					return nil, fmt.Errorf("?")
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

	log.Println("------------------------------------")

	workinfos := make(map[int32]*sdb.WorkerInfo, 0)
	//workerList
	for _, worker := range workerList {
		workinfos[worker.Segid] = worker
	}

	query := sdb.PrepareTaskRequest{
		TaskIdentify: &sdb.TaskIdentify{QueryId: 1, SliceId: 0, SegId: 0},
		Sessionid:    1,
		Uid:          1,
		Dbid:         1,
		ReadXid:       1,
		CommitXid:       1,
		Sql:          in.Sql,
		QueryInfo:    nil,
		PlanInfo:     optimizerResult.PlanstmtStr,
		//PlanInfoDxl: r.PlanDxlStr,
		PlanParams: optimizerResult.PlanParamsStr,
		GucVersion: 1,
		Workers:    workinfos,
		SliceTable: &sliceTable,
		ResultDir: "/home/gpadmin/code/pg/scloud/gpAux/gpdemo/datadirs/",
	}

	err = mgr.WriterWorkerInfo(&query)
	if err != nil {
		return nil, err
	}

	for _, workerSlice := range workerSliceList {
		// Send To Work
		var cloneTask *sdb.PrepareTaskRequest
		cloneTask = proto.Clone(&query).(*sdb.PrepareTaskRequest)
		//proto.Clone(&workerSlice)
		go func(query *sdb.PrepareTaskRequest, workerSlice *sdb.WorkerSliceInfo) {
			sliceid := workerSlice.Sliceid
			//localSliceTable.LocalSlice = int32(sliceid)
			taskid := &sdb.TaskIdentify{QueryId: 1, SliceId: int32(sliceid), SegId: workerSlice.WorkerInfo.Segid}
			query.TaskIdentify = taskid
			query.SliceTable.LocalSlice = int32(sliceid)

			//localSliceTable := sliceTable
			//addr := fmt.Sprintf("%s:%d", *workIp, *workPort+int(i))
			log.Printf("addr:%s", workerSlice.WorkerInfo.Addr)

			workConn, err := grpc.Dial(workerSlice.WorkerInfo.Addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				log.Errorf("did not connect: %v", err)
				//return nil, fmt.Errorf("?")
				return
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
				log.Errorf("could not prepare: %v", err)
				return
			}
			if reply != nil { log.Errorf("prepare reply: %v", reply.String())
			}

			time.Sleep(2 * time.Second)

			query1 := &sdb.StartTaskRequest{
				TaskIdentify: taskid,
			}

			reply1, err := workClient.Start(ctx, query1)
			if err != nil {
				log.Errorf("could not start: %v", err)
				return
			}
			if reply1 != nil {
				log.Printf("start reply: %v", reply1.String())
			}
			time.Sleep(1 * time.Second)
		}(cloneTask, workerSlice)
		time.Sleep(1 * time.Second)
	}

	time.Sleep(10 * time.Second)

	resRepy := &sdb.ExecQueryReply{
				QueryId : query.TaskIdentify.QueryId,
				Sessionid : query.Sessionid,
				Uid : query.Uid,
				Dbid : query.Dbid,
				ResultDir : query.ResultDir,
			}

	return resRepy, nil
}

