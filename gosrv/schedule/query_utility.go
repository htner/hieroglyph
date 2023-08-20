package main

import (
	"errors"

	"github.com/auxten/postgresql-parser/pkg/sql/parser"
	"github.com/auxten/postgresql-parser/pkg/sql/sem/tree"
	"github.com/htner/sdb/gosrv/pkg/lakehouse"
	"github.com/htner/sdb/gosrv/proto/sdb"
	"github.com/htner/sdb/gosrv/pkg/utils/postgres"
	"github.com/htner/sdb/gosrv/pkg/schedule"

	//"github.com/auxten/postgresql-parser/pkg/walk"
	_ "github.com/htner/sdb/gosrv/pkg/utils/logformat"
	log "github.com/sirupsen/logrus"
)

func (Q *QueryHandler) processUtility() (bool, error) {
  /*
  w := &walk.AstWalker{
		Fn: func(ctx interface{}, node interface{}) (stop bool) {
			log.Printf("node type %T", node)
			return false
		},
	}
  */
	
	stmts, err := parser.Parse(Q.request.Sql)
	if err != nil {
		return false, errors.New("golang parser error")
	}

  // FIXME mutli sql
  for _, stmt := range stmts {
    isProcessed := false
    message := ""
    switch stmt.AST.(type) {
    case *tree.BeginTransaction:
      log.Println("begin", stmt)
      tr := lakehouse.NewTranscation(Q.request.Dbid, Q.request.Sid)
      tr.Start(false);
      isProcessed = true
      message = "begin"
    case *tree.RollbackTransaction:
      log.Println("rollback", stmt)
      tr := lakehouse.NewTranscation(Q.request.Dbid, Q.request.Sid)
      tr.Rollback();
      isProcessed = true
      message = "rollback"
    case *tree.CommitTransaction:
      log.Println("commit", stmt)
      tr := lakehouse.NewTranscation(Q.request.Dbid, Q.request.Sid)
      tr.Commit();
      isProcessed = true
      message = "commit"
    }
    if isProcessed {
      var result sdb.WorkerResultData
      result.CmdType = postgres.CMD_UTILITY
      result.Dbid = Q.request.Dbid
      result.QueryId = Q.newQueryId
      result.Rescode = 0
      result.Message = message 

      mgr := schedule.NewQueryMgr(Q.request.Dbid)
      err := mgr.InitQueryResult(Q.newQueryId, uint32(sdb.QueryStates_QueryInit), 1, "")
      if err != nil {
        log.Printf("InitQueryResult errro %v", err)
        return true, err
      }

      x, err := mgr.WriteQueryResult(Q.newQueryId, &result)

      log.Println("InitQueryResult errro", x, err, result)
      return true, err
    }
  }
	//_, _ = w.Walk(stmts, nil)
  return false, nil
}
