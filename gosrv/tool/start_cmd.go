/*
Copyright © 2022 NAME HERE <EMAIL ADDRESS>\
*/

package main

import (
	"os"

	"github.com/spf13/cobra"
)



// Debug log
var debug bool = false
var username string 
var database string 
var organization string 
var account string 

var CreateAccountCmd = &cobra.Command{
	Use:   "createaccount",
	Short: "Create CloudDB Account",
	Long: `The utility is used to create the SDB CloudDB Account.`,
	Run: func(cmd *cobra.Command, args []string) {
    createAccount(account, organization)
	},
}

var CreateDatabaseCmd = &cobra.Command{
	Use:   "createdb",
	Short: "Create CloudDB Database",
	Long: `The utility is used to create the SDB CloudDB Database.`,
	Run: func(cmd *cobra.Command, args []string) {
    createDatabase(organization, database)
	},
}

var CreateUserCmd = &cobra.Command{
	Use:   "createuser",
	Short: "Create CloudDB User",
	Long: `The utility is used to create the SDB CloudDB user.`,
	Run: func(cmd *cobra.Command, args []string) {
    createUser(organization, user)
	},
}

func init() {

	// Here we define your flags and configuration settings.
	CreateAccountCmd.Flags().StringVarP(&username, "username", "u", "", "user name")
	CreateAccountCmd.Flags().StringVarP(&database, "database", "d", "", "database name")
	CreateAccountCmd.Flags().StringVarP(&organization, "organization", "o", "", "organizatio")
	CreateAccountCmd.Flags().StringVarP(&account, "account", "a", "", "account name")

	CreateAccountCmd.Flags().BoolVar(&debug, "debug", false, "show debug log")
}


// This tool is mainly for managing the pdb server.
// Due to user habits and other reasons, we continue to name our tool gpstart/gpstop.
// Of course, we don't rule out the possibility that the full name of the tool will start with Pdb in the future.
// Notice: gpstart calls pdbstart directly
func main() {
	CreateAccountCmd.AddCommand(CreateDatabaseCmd)
	CreateAccountCmd.AddCommand(CreateUserCmd)
	err := CreateAccountCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}
