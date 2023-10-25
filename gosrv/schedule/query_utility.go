package main

import (
	"errors"
	//"reflect"

	"github.com/auxten/postgresql-parser/pkg/sql/parser"
	"github.com/auxten/postgresql-parser/pkg/sql/sem/tree"
	"github.com/htner/sdb/gosrv/pkg/account"
	"github.com/htner/sdb/gosrv/pkg/lakehouse"
	"github.com/htner/sdb/gosrv/pkg/schedule"
	"github.com/htner/sdb/gosrv/pkg/utils/postgres"
	"github.com/htner/sdb/gosrv/proto/sdb"

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
      tr.SetAutoCommit(false);
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
    case *tree.DropDatabase:
      log.Println("drop database", stmt)
      isProcessed = true
      message = "DROP DATABASE"
    case *tree.CreateDatabase:
      log.Println("create database", stmt)
      s := stmt.AST.(*tree.CreateDatabase)
      // s.IfNotExists
      var template string
      organization := Q.request.Organization
      if s.Template == "template0" || s.Template == "template1" || s.Template == "" {
        template = "template1"
        organization = "sdb"
      } else {
        template = s.Template
      }
      db, err := account.CloneDatabase(Q.request.Organization, string(s.Name), organization, template, Q.request.Uid)
      if err != nil {
        log.Printf("create database err: %s", err.Error())
      }
		  log.Println("create database ", db)
      isProcessed = true
      message = "CREATE DATABASE"
    case *tree.CreateExtension:
      log.Println("create extension", stmt)
      //s := stmt.AST.(*tree.CreateDatabase)
      // maybe broadcast to all worker for test
      isProcessed = true
      message = "CREATE EXTENSION"
    case *tree.ShowVar:
      s := stmt.AST.(*tree.ShowVar)
      log.Println("show ", s.Name)
      // maybe broadcast to all worker for test
      isProcessed = true
      message = "SHOW " + s.Name 
    case *tree.SetVar:
      s := stmt.AST.(*tree.SetVar)
      log.Println("Set", s.Name, s, s.Values)
      // maybe broadcast to all worker for test
      // schema
      if s.Name == "search_path" {
        if len(s.Values) == 1 {
          log.Println("change session search path to", s.Values[0].String())
        }
      }
      isProcessed = true
      message = "SET " + s.Name 
 
    default:
      tr := lakehouse.NewTranscation(Q.request.Dbid, Q.request.Sid)
      tr.WriteAble()
    }
    if isProcessed {
      var result sdb.WorkerResultData
      result.CmdType = postgres.CMD_UTILITY
      result.Dbid = Q.request.Dbid
      result.QueryId = Q.newQueryId
      result.Rescode = 0
      result.Message = new(sdb.ErrorResponse)
      result.Message.Code = "00000"
      result.Message.Message = message 

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
