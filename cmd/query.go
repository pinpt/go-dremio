package cmd

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net/url"
	"time"

	// Needs the driver to access the sql database
	_ "github.com/pinpt/go-dremio/driver"
	"github.com/spf13/cobra"
)

var queryCmd = &cobra.Command{
	Use:   "query",
	Short: "makes a simple sql query",
	Long:  "this is an example on how to use the go-dremio sql driver",
	Args:  cobra.MinimumNArgs(2),
	Run: func(cmd *cobra.Command, args []string) {
		u, err := url.Parse(args[0])
		if err != nil {
			log.Fatal("error parsing url", err)
		}
		q := u.Query()
		query := `SELECT IncidntNum FROM "SF_incidents2016.json"`
		if len(args) > 2 {
			query = args[1]
		} else {
			ctx := q.Get("context")
			if ctx == "" {
				// add in the context to the default samples
				q.Set("context", `Samples."samples.dremio.com"`)
				u, _ = url.Parse(u.String() + "?" + q.Encode())
			}
		}
		conn, err := sql.Open("dremio", u.String())
		if err != nil {
			log.Fatal(err)
		}
		defer conn.Close()
		started := time.Now()
		rows, err := conn.QueryContext(context.Background(), query)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("query took: %v\n", time.Since(started))
		cols, err := rows.Columns()
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println("columns:", cols)
		defer rows.Close()
		var c int
		for rows.Next() {
			var val sql.NullString
			if err := rows.Scan(&val); err != nil {
				log.Fatal(err)
			}
			fmt.Println(1+c, "val:", val.String)
			c++
		}
	},
}

func init() {
	rootCmd.AddCommand(queryCmd)
}
