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
	"github.com/htner/sdb/gosrv/lakehouse/proto"
	pb "github.com/htner/sdb/gosrv/lakehouse/proto"
	"github.com/htner/sdb/gosrv/pkg/lakehouse"
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
func (s *LakeServer) PrepareInsertFiles(ctx context.Context, request *pb.PrepareInsertFilesRequest) (*pb.ExecQueryReply, error) {
	//log.Printf("get request %s", in.Sql)
	lakeop := lakehouse.NewLakeRelOperator(request.DBId(), request.Sid())
	lakeop.MarkFiles(request.RelId(), request.AddFiles())
	return &pb.PrepareInsertFilesResponse{}, nil
}

func (s *LakeServer) UpdateFiles(ctx context.Context, request *pb.UpdateFilesFilesRequest) (*pb.UpdateFilesRequest, error) {
	//log.Printf("get request %s", in.Sql)
	lakeop := lakehouse.NewLakeRelOperator(request.DBId(), request.Sid())
	lakeop.InsertFiles(request.RelId(), request.AddFiles())
	lakeop.DeleleFiles(request.RelId(), request.DelteFiles())
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
