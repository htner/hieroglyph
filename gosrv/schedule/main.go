package main

import (
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	consulapi "github.com/hashicorp/consul/api"
	"github.com/htner/sdb/gosrv/pkg/service"

	_ "github.com/htner/sdb/gosrv/pkg/utils/logformat"
	"github.com/htner/sdb/gosrv/proto/sdb"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	//"google.golang.org/grpc"
	//"google.golang.org/grpc/reflection"
)

var (
	optimizerAddr = flag.String("optimizer_addr", "localhost:8999", "the address to connect to optimizer")
	//workIp        = flag.String("worker_addr", "localhost", "the int to connect to worker")
	//workPort      = flag.Int("worker_port", 40000, "the port to connect to worker")
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
		Address: "127.0.0.1",
		Tags:    []string{"public"},
	}

	err = consul.Agent().ServiceRegister(registration)
	if err != nil {
		log.Printf("Failed to register service: %v", err)
	}

	return nil

}
