package cmd

import (
	"context"
	"database/sql"
	"log"
	"os"
	"path/filepath"

	"github.com/pinpt/go-dremio/console"
	"github.com/spf13/cobra"
)

var consoleCmd = &cobra.Command{
	Use:   "console",
	Short: "starts a simpe console",
	Long:  "simple console to make queries against a dremio database",
	Run: func(cmd *cobra.Command, args []string) {
		// All of these are optional flags.
		// If not provided, you'll be prompted at the start of the console
		username, _ := cmd.Flags().GetString("username")
		password, _ := cmd.Flags().GetString("password")
		endpoint, _ := cmd.Flags().GetString("endpoint")
		cwd, _ := os.Getwd()
		console.SetCredentials(username, password, endpoint)
		console.SetHistoryFile(filepath.Join(cwd, "history.txt"))
		logoutPlugin := console.Plugin{
			Query:       "^logout$",
			Usage:       "logout",
			Description: "deletes the config file and exists",
			Callback: func(ctx context.Context, conn *sql.DB, input string) error {
				er := os.Remove(console.ConfigFile())
				if er != nil {
					return er
				}
				os.Exit(0)
				return nil
			},
		}
		console.Register(logoutPlugin)

		if err := console.Run(); err != nil {
			log.Fatal(err)
		}
	},
}

func init() {
	consoleCmd.Flags().String("username", "", "optional - username of the dremio account")
	consoleCmd.Flags().String("password", "", "optional - password of the dremio account")
	consoleCmd.Flags().String("endpoint", "", "optional - endpoint to the dremio database, must have the correct url prefix, sucha as http or https")
	rootCmd.AddCommand(consoleCmd)
}
