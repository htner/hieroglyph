//go:build integration

package main

import (
	"context"
	"fmt"
	"net"
	"strconv"

	pb "github.com/htner/sdb/gosrv/schedule/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

// rungRPC starts a gRPC greeting server on the given port
// This is blocking, so it should be run in a goroutine
// gRPC server is stopped when the context is cancelled
// gRPC server is using reflection
func rungRPC(ctx context.Context, port int) error {

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return fmt.Errorf("failed to listen: %w", err)
	}

	s := grpc.NewServer()
	pb.RegisterScheduleServer(s, &server{port: port})
	reflection.Register(s)

	go stopWhenDone(ctx, s)

	if err := s.Serve(lis); err != nil {
		return fmt.Errorf("failed to serve: %w", err)
	}

	return nil
}

func stopWhenDone(ctx context.Context, server *grpc.Server) {
	<-ctx.Done()
	server.GracefulStop()
}

// server is used to implement proto.ScheduleServer
type ScheduleServer struct {
	port int
}

// Depart implements proto.ScheduleServer
// It just returns commid
func (s *ScheduleServer) Depart(ctx context.Context, in *pb.ExecQueryRequest) (*pb.ExecQueryReply, error) {
	return &pb.ExecQueryReply{}, nil
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
