%{
/*-------------------------------------------------------------------------
 *
 * bootparse.y
 *	  yacc grammar for the "bootstrap" mode (BKI file format)
 *
 * Portions Copyright (c) 2006-2009, Greenplum inc
 * Portions Copyright (c) 2012-Present Pivotal Software, Inc.
 * Portions Copyright (c) 1996-2014, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/bootstrap/bootparse.y
 *
 *-------------------------------------------------------------------------
 */
package main

import (
	"log"
	"strconv"
	"fmt"
)

// MaxUint is the maximum value of an uint.
const MaxUint = ^uint(0)
// MaxInt is the maximum value of an int.
const MaxInt = int(MaxUint >> 1)
const InvalidOid = 0
var num_columns_read int
const BOOTCOL_NULL_AUTO	=		1
const BOOTCOL_NULL_FORCE_NULL	=	2
const BOOTCOL_NULL_FORCE_NOT_NULL  = 3

/*
func unimplemented(sqllex sqlLexer, feature string) int {
    sqllex.(*lexer).Unimplemented(feature)
    return 1
}

func purposelyUnimplemented(sqllex sqlLexer, feature string, reason string) int {
    sqllex.(*lexer).PurposelyUnimplemented(feature, reason)
    return 1
}

func setErr(sqllex sqlLexer, err error) int {
    sqllex.(*lexer).setErr(err)
    return 1
}

func unimplementedWithIssue(sqllex sqlLexer, issue int) int {
    sqllex.(*lexer).UnimplementedWithIssue(issue)
    return 1
}

func unimplementedWithIssueDetail(sqllex sqlLexer, issue int, detail string) int {
    sqllex.(*lexer).UnimplementedWithIssueDetail(issue, detail)
    return 1
}
*/



func heap_create(relname string, relnamespace uint64,
 			reltablespace uint64,
			relid uint64,
			relfilenode ,
			accessmtd uint64,
			tupDesc TupleDesc,
			relkind byte,
			relpersistence byte,
			shared_relation bool,
			mapped_relation bool,
			allow_system_table_mods,
			relfrozenxid *TransactionId,
			relminmxid *MultiXactId) {

}

func heap_create_with_catalog() {
}

%}


%union
{
    ival  int32
    pos   int32
    str   string
    oidval uint64 
	stmt StatementUnion
}


%type <str>  boot_index_params
%type <str> boot_index_param
%type <str>   boot_const boot_ident
%type <ival>  optbootstrap optsharedrelation optwithoutoids boot_column_nullness
%type <oidval> oidspec optoideq optrowtypeoid
%type <str> error_nothing

%type <stmt> TopLevel
%type <stmt> Boot_Queries
%type <stmt> Boot_Query
%type <stmt> Boot_OpenStmt
%type <stmt> Boot_CloseStmt
%type <stmt> Boot_CreateStmt
%type <stmt> Boot_InsertStmt
%type <stmt> Boot_DeclareIndexStmt
%type <stmt> Boot_DeclareUniqueIndexStmt
%type <stmt> Boot_DeclareToastStmt
%type <stmt> Boot_BuildIndsStmt

// Ordinary key words
%token <str> NOTHING
%token <str> CONST_P ID
%token <str> OPEN XCLOSE XCREATE INSERT_TUPLE
%token <str> XDECLARE INDEX ON USING XBUILD INDICES UNIQUE XTOAST
%token <str> OBJ_ID XBOOTSTRAP XSHARED_RELATION XWITHOUT_OIDS XROWTYPE_OID 
%token <str> NULLVAL
%token <str> XFORCE XNOT XNULL

%token <str> COMMA EQUALS LPAREN RPAREN


%nonassoc low
%nonassoc high

%%

TopLevel: Boot_Queries
		;

Boot_Queries:
		  Boot_Query
		| Boot_Queries Boot_Query
		;

Boot_Query :
		  Boot_OpenStmt
		| Boot_CloseStmt
		| Boot_CreateStmt
		| Boot_InsertStmt
		| Boot_DeclareIndexStmt
		| Boot_DeclareUniqueIndexStmt
		| Boot_DeclareToastStmt
		| Boot_BuildIndsStmt
		;

Boot_OpenStmt:
		  OPEN boot_ident
				{
                    log.Println("open", $2)
					boot.OpenRelation($2)
				}
		;

Boot_CloseStmt:
		  XCLOSE boot_ident %prec low
				{
					//do_start();
					//closerel($2);
					//do_end();
				}
		| XCLOSE %prec high
				{
					//do_start();
					//closerel(NULL);
					//do_end();
				}
		;

Boot_CreateStmt:
		  XCREATE boot_ident oidspec optbootstrap optsharedrelation optwithoutoids optrowtypeoid LPAREN
				{
					log.Println("start create")
					//do_start();
					//numattr = 0;
					//elog(DEBUG4, "creating%s%s relation %s %u",
					//	 $4 ? " bootstrap" : "",
					//	 $5 ? " shared" : "",
					//	 $2,
					//	 $3);
				}
		  boot_column_list
				{
					log.Println("boot create")
					//do_end();
				}
		  RPAREN
				{
					log.Println("end create")
					//var shared_relation bool
					//var	mapped_relation bool

					//do_start();

					//boot.CreateTupleDesc()

					//shared_relation = ($5 != 0)

					/*
					 * The catalogs that use the relation mapper are the
					 * bootstrap catalogs plus the shared catalogs.  If this
					 * ever gets more complicated, we should invent a BKI
					 * keyword to mark the mapped catalogs, but for now a
					 * quick hack seems the most appropriate thing.  Note in
					 * particular that all "nailed" heap rels (see formrdesc
					 * in relcache.c) must be mapped.
					 */
                
					// mapped_relation = ($4 != 0) || shared_relation;
					boot.CreateRelation($2, 
										PG_CATALOG_NAMESPACE, 
										shared_relation ? GLOBALTABLESPACE_OID : 0,
										$3,
										)
					if ($4 != 0) {
				//		if (boot_reldesc)
				//		{
				//			elog(DEBUG4, "create bootstrap: warning, open relation exists, closing first");
				//			closerel(NULL);
				//		}

				//		boot_reldesc = heap_create($2,
				//								   PG_CATALOG_NAMESPACE,
				//								   shared_relation ? GLOBALTABLESPACE_OID : 0,
				//								   $3,
				//								   InvalidOid,
				//								   tupdesc,
				//								   /* relam */ InvalidOid,
				//								   RELKIND_RELATION,
				//								   RELPERSISTENCE_PERMANENT,
				//								   RELSTORAGE_HEAP,
				//								   shared_relation,
				//								   mapped_relation,
				//								   true);
				//		elog(DEBUG4, "bootstrap relation created");
			//		}
			//		else
			//		{
			//			Oid id;
//
///						id = heap_create_with_catalog($2,
//													  PG_CATALOG_NAMESPACE,
//													  shared_relation ? GLOBALTABLESPACE_OID : 0,
//													  $3,
//													  $7,
//													  InvalidOid,
//													  BOOTSTRAP_SUPERUSERID,
//													  tupdesc,
//													  NIL,
//													  /* relam */ InvalidOid,
//													  RELKIND_RELATION,
//													  RELPERSISTENCE_PERMANENT,
//													  RELSTORAGE_HEAP,
//													  shared_relation,
//													  mapped_relation,
//													  true,
//													  0,
//													  ONCOMMIT_NOOP,
//													  NULL,			/*CDB*/
//													  (Datum) 0,
//													  false,
//													  true,
//													  false,
//													  /* valid_opts */ false,
//													  /* is_part_child */ false,
//													  /* is_part_parent */ false);
//						elog(DEBUG4, "relation created with oid %u", id);
//					}
//					do_end();
  //                  */
				}
			}
		;

Boot_InsertStmt:
		  INSERT_TUPLE optoideq
				{
					//fmt.Println($2)
					//do_start();
					//if ($2)
					//	elog(DEBUG4, "inserting row with oid %u", $2);
					//else
					//	elog(DEBUG4, "inserting row");
					ResetInsert()
				}
		  LPAREN boot_column_val_list RPAREN
				{
                    /*
					if (num_columns_read != numattr)
					//	elog(ERROR, "incorrect number of columns in row (expected %d, got %d)",
							 numattr, num_columns_read);
					if (boot_reldesc == NULL)
						elog(FATAL, "relation not open");
					
					do_end();
                    */
					InsertOneTuple();
				}
		;

Boot_DeclareIndexStmt:
		  XDECLARE INDEX boot_ident oidspec ON boot_ident USING boot_ident LPAREN boot_index_params RPAREN
				{

				}
		;

Boot_DeclareUniqueIndexStmt:
		  XDECLARE UNIQUE INDEX boot_ident oidspec ON boot_ident USING boot_ident LPAREN boot_index_params RPAREN
				{
	
				}
		;

Boot_DeclareToastStmt:
		  XDECLARE XTOAST oidspec oidspec ON boot_ident
				{
				}
		;

Boot_BuildIndsStmt:
		  XBUILD INDICES
				{
				}
		;


boot_index_params:
		boot_index_params COMMA boot_index_param	{ /* $$ = lappend($1, $3); */ }
		| boot_index_param							{ /* $$ = list_make1($1); */}
		;

boot_index_param:
		boot_ident boot_ident
				{
                    /*
					IndexElem *n = makeNode(IndexElem);
					n->name = $1;
					n->expr = NULL;
					n->indexcolname = NULL;
					n->collation = NIL;
					n->opclass = list_make1(makeString($2));
					n->ordering = SORTBY_DEFAULT;
					n->nulls_ordering = SORTBY_NULLS_DEFAULT;
					$$ = n;
                    */
				}
		;

optbootstrap:
			XBOOTSTRAP	{ $$ = 1; }
		|				{ $$ = 0; }
		;

optsharedrelation:
			XSHARED_RELATION	{ $$ = 1; }
		|						{ $$ = 0; }
		;

optwithoutoids:
			XWITHOUT_OIDS	{ $$ = 1; }
		|					{ $$ = 0; }
		;

optrowtypeoid:
			XROWTYPE_OID oidspec	{ $$ = $2; }
		|							{ $$ = InvalidOid; }
		;

boot_column_list:
		  boot_column_def
		| boot_column_list COMMA boot_column_def
		;

boot_column_def:
		  boot_ident EQUALS boot_ident boot_column_nullness
				{
				   boot.DefineAttr($1, $3, $4);
				   fmt.Println($1, " = ", $3)
				}
		;

boot_column_nullness:
			XFORCE XNOT XNULL	{ $$ = BOOTCOL_NULL_FORCE_NOT_NULL; }
		|	XFORCE XNULL		{  $$ = BOOTCOL_NULL_FORCE_NULL; }
		| { $$ = BOOTCOL_NULL_AUTO; }
		;

oidspec:
			boot_ident							
			{ 
				intVar, _ := strconv.ParseUint($1, 0, 64)
				$$ = intVar
			}
		;

optoideq:
			OBJ_ID EQUALS oidspec				{ $$ = $3; }
		|										{ $$ = InvalidOid; }
		;

boot_column_val_list:
		   boot_column_val
		|  boot_column_val_list boot_column_val
		|  boot_column_val_list COMMA boot_column_val
		;

boot_column_val:
			boot_ident
			{
				boot.AppendValueToRow($1)
			}
		| boot_const
			{
				boot.AppendValueToRow($1)
			}
		| NULLVAL
			{
				boot.AppendNullToRow()
			}
		;

boot_const :
		  CONST_P { 

		   }
		;

boot_ident :
		  ID	{  

		   }
		;

error_nothing:
		NOTHING {log.Println("get nothing", $1)}
		;
%%


