/*
Copyright © 2022 NAME HERE <EMAIL ADDRESS>\
*/

package main

import (
	"fmt"
	"log"
	"os"

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

var SDBCmd = &cobra.Command{
	Use:   "help",
	Short: "CloudDB",
	Long: `The CloudDB.`,
	Run: func(cmd *cobra.Command, args []string) {
    fmt.Printf("help")
	},
}

var CreateAccountCmd = &cobra.Command{
	Use:   "createaccount",
	Short: "Create CloudDB Account",
	Long: `The utility is used to create the SDB CloudDB Account.`,
	Run: func(cmd *cobra.Command, args []string) {
    err := account.CreateSdbAccount(accountName, password, organization)
    if err != nil {
      log.Printf("create account err: %s", err.Error())
    }
	},
}

var CreateDatabaseCmd = &cobra.Command{
	Use:   "createdb",
	Short: "Create CloudDB Database",
	Long: `The utility is used to create the SDB CloudDB Database.`,
	Run: func(cmd *cobra.Command, args []string) {
    err := account.CreateDatabase(organization, database)
    if err != nil {
      log.Printf("create database err: %s", err.Error())
    }
	},
}

var CreateUserCmd = &cobra.Command{
	Use:   "createuser",
	Short: "Create CloudDB User",
	Long: `The utility is used to create the SDB CloudDB user.`,
	Run: func(cmd *cobra.Command, args []string) {
    err := account.CreateUser(organization, username, password)
    if err != nil {
      log.Printf("create user err: %s", err.Error())
    }
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
	err := SDBCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}
