package cmd

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strings"

	"github.com/fatih/color"
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

		console.SetCredentials(username, password, endpoint)

		printPlugin := console.Plugin{
			Query:       "^print\\s",
			Description: "prints a silly message",
			Callback: func(ctx context.Context, conn *sql.DB, input string) error {
				parts := strings.Split(input, " ")
				parts = parts[1:]
				str := strings.Join(parts, " ")
				fmt.Println(color.HiMagentaString(str))
				return nil
			},
		}
		console.Register(printPlugin)

		err := console.Run()
		if err != nil {
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
