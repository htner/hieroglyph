package main

import (
	"context"
	"fmt"
	"time"

	"github.com/htner/sdb/gosrv/pkg/config"
	"github.com/htner/sdb/gosrv/proto/sdb"

	_ "github.com/htner/sdb/gosrv/pkg/utils/logformat"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func (Q *QueryHandler) optimize() error {
	conn, err := grpc.Dial(*optimizerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := sdb.NewOptimizerClient(conn)

	var spaceConfig config.LakeSpaceConfig
	conf, _ := spaceConfig.GetConfig(Q.request.Dbid)
	// Contact the server and print out its response.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*6000)
	defer cancel()
	req := &sdb.OptimizeRequest{
		Name:        "query",
		Sql:         Q.request.Sql,
		DbSpace:     conf,
		CatalogList: Q.catalogFiles,
	}
	optimizerResult, err := c.Optimize(ctx, req)
	if err != nil {
		log.Printf("could not optimize: %v", err)
		return fmt.Errorf("optimizer error")
	}

	Q.optimizerResult = optimizerResult

	if optimizerResult.Rescode != 0 {
		log.Printf("optimize err: %v", optimizerResult)
		return fmt.Errorf("optimizer error")
	}

	log.Printf("Greeting: %s %d %d %d", string(optimizerResult.PlanDxlStr), len(optimizerResult.PlanDxlStr), len(optimizerResult.PlanstmtStr), len(optimizerResult.PlanParamsStr))
  if (len(optimizerResult.PlanstmtStr) == 0) {
    return fmt.Errorf("plan stmt str is empty")
  }
	return nil
}
