//go:build integration

package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"

	consulapi "github.com/hashicorp/consul/api"
	"github.com/htner/sdb/gosrv/lake/proto"
	pb "github.com/htner/sdb/gosrv/lake/proto"
	"github.com/htner/sdb/gosrv/pkg/lakehouse"
	"github.com/htner/sdb/gosrv/pkg/lakehouse/kvpair"
	"github.com/htner/sdb/gosrv/pkg/types"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
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
	pb.RegisterLakeServer(s, &LakeServer{port: port})
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
type LakeServer struct {
	proto.UnimplementedLakeServer
	port int
}

// Depart implements proto.ScheduleServer
// It just returns commid
func (s *LakeServer) PrepareInsertFiles(ctx context.Context, request *pb.PrepareInsertFilesRequest) (*pb.PrepareInsertFilesResponse, error) {
	//log.Printf("get request %s", in.Sql)
	lakeop := lakehouse.NewLakeRelOperator(
		types.DatabaseId(request.Dbid),
		types.SessionId(request.Sessionid),
		types.TransactionId(request.CommitXid))
	err := lakeop.MarkFiles(types.RelId(request.Rel), request.AddFiles)
	if err != nil {
		return nil, fmt.Errorf("mark files error")
	}
	return &pb.PrepareInsertFilesResponse{}, nil
}

func (s *LakeServer) UpdateFiles(ctx context.Context, request *pb.UpdateFilesRequest) (*pb.UpdateFilesResponse, error) {
	//log.Printf("get request %s", in.Sql)
	lakeop := lakehouse.NewLakeRelOperator(types.DatabaseId(request.Dbid),
		types.SessionId(request.Sessionid),
		types.TransactionId(request.CommitXid))

	newFiles := make([]*kvpair.FileMeta, 0)
	removeFiles := make([]*kvpair.FileMeta, 0)
	for _, file := range request.AddFiles {
		var f kvpair.FileMeta
		f.Database = types.DatabaseId(request.Dbid)
		f.Relation = types.RelId(request.Rel)
		f.Filename = file.FileName

		newFiles = append(newFiles, &f)
	}

	for _, file := range request.RemoveFiles {
		var f kvpair.FileMeta
		f.Database = types.DatabaseId(request.Dbid)
		f.Relation = types.RelId(request.Rel)
		f.Filename = file.FileName

		removeFiles = append(removeFiles, &f)
	}

	err := lakeop.InsertFiles(types.RelId(request.Rel), newFiles)
	if err != nil {
		return nil, fmt.Errorf("insert files error")
	}
	err = lakeop.DeleleFiles(types.RelId(request.Rel), removeFiles)
	if err != nil {
		return nil, fmt.Errorf("insert files error")
	}
	return &pb.UpdateFilesResponse{}, nil
}

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
