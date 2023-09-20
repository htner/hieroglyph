/*
Copyright © 2022 NAME HERE <EMAIL ADDRESS>\
*/

package main

import (
	"fmt"
	"os"

	log "github.com/sirupsen/logrus"
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/htner/sdb/gosrv/pkg/account"
	"github.com/spf13/cobra"
)

// Debug log
var debug bool = false
var accountName string
var username string
var database string
var organization string
var password string

var destinationTable string
var sourceTable string

var sourceDatabase string
var sourceOrganization string

var SDBCmd = &cobra.Command{
	Use:   "help",
	Short: "CloudDB",
	Long:  `The CloudDB.`,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Printf("help")
	},
}

var CreateAccountCmd = &cobra.Command{
	Use:   "createaccount",
	Short: "Create CloudDB Account",
	Long:  `The utility is used to create the SDB CloudDB Account.`,
	Run: func(cmd *cobra.Command, args []string) {
		acc, err := account.CreateSdbAccount(accountName, password, organization)
		if err != nil {
			log.Printf("create account err: %s", err.Error())
		}
		log.Println("create account ", acc)
	},
}

var CreateDatabaseCmd = &cobra.Command{
	Use:   "createdb",
	Short: "Create CloudDB Database",
	Long:  `The utility is used to create the SDB CloudDB Database.`,
	Run: func(cmd *cobra.Command, args []string) {
		db, err := account.CreateDatabase(organization, database)
		if err != nil {
			log.Printf("create database err: %s", err.Error())
		}
		log.Println("create database ", db)
	},
}

var CloneDatabaseCmd = &cobra.Command{
	Use:   "clonedb",
	Short: "Clone CloudDB Database",
	Long:  `The utility is used to clone the SDB CloudDB Database.`,
	Run: func(cmd *cobra.Command, args []string) {
		db, err := account.CloneDatabase(organization, database, sourceOrganization, sourceDatabase, 1)
		if err != nil {
			log.Printf("create database err: %s", err.Error())
		}
		log.Println("create database ", db)
	},
}

var CreateUserCmd = &cobra.Command{
	Use:   "createuser",
	Short: "Create CloudDB User",
	Long:  `The utility is used to create the SDB CloudDB user.`,
	Run: func(cmd *cobra.Command, args []string) {
		user, err := account.CreateUser(organization, username, password)
		if err != nil {
			log.Printf("create user err: %s", err.Error())
		}
		log.Println("create user ", user)
	},
}

var CopyTableCmd = &cobra.Command{
	Use:   "copytable",
	Short: "Copy CloudDB Table",
	Long:  `The utility is used to copy the SDB CloudDB table.`,
	Run: func(cmd *cobra.Command, args []string) {
		/*
		   err := tool.CopyTable(database, destinationTable, sourceTable)
		   if err != nil {
		     log.Printf("create user err: %s", err.Error())
		   }
		*/
	},
}

func init() {

	// Here we define your flags and configuration settings.
	SDBCmd.Flags().BoolVar(&debug, "debug", false, "show debug log")

	CreateAccountCmd.Flags().StringVarP(&organization, "organization", "o", "", "organizatio")
	CreateAccountCmd.Flags().StringVarP(&accountName, "account", "a", "", "account name")
	CreateAccountCmd.Flags().StringVarP(&password, "password", "p", "", "password")

	CreateUserCmd.Flags().StringVarP(&organization, "organization", "o", "", "organizatio")
	CreateUserCmd.Flags().StringVarP(&username, "username", "u", "", "user name")
	CreateUserCmd.Flags().StringVarP(&password, "password", "p", "", "password")

	CreateDatabaseCmd.Flags().StringVarP(&organization, "organization", "o", "", "organizatio")
	CreateDatabaseCmd.Flags().StringVarP(&database, "database", "d", "", "database name")

	CloneDatabaseCmd.Flags().StringVarP(&organization, "organization", "o", "", "organization")
	CloneDatabaseCmd.Flags().StringVarP(&database, "database", "d", "", "database name")
	CloneDatabaseCmd.Flags().StringVarP(&sourceOrganization, "source_organization", "s", "", "source organization")
	CloneDatabaseCmd.Flags().StringVarP(&sourceDatabase, "source_database", "b", "", "source database")

	CopyTableCmd.Flags().StringVarP(&database, "database", "d", "", "database name")
	CopyTableCmd.Flags().StringVarP(&sourceTable, "source", "s", "", "source table name")
	CopyTableCmd.Flags().StringVarP(&destinationTable, "destination", "t", "", "destination table name")
}

// This tool is mainly for managing the pdb server.
// Due to user habits and other reasons, we continue to name our tool gpstart/gpstop.
// Of course, we don't rule out the possibility that the full name of the tool will start with Pdb in the future.
// Notice: gpstart calls pdbstart directly
func main() {
	fdb.MustAPIVersion(710)

	SDBCmd.AddCommand(CreateAccountCmd)
	SDBCmd.AddCommand(CreateDatabaseCmd)
	SDBCmd.AddCommand(CreateUserCmd)
	SDBCmd.AddCommand(CloneDatabaseCmd)
	//SDBCmd.AddCommand(CopyTableCmd)
	err := SDBCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}
