package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net/url"
	"os"
	"time"

	_ "github.com/pinpt/go-dremio"
)

func main() {
	if len(os.Args) < 2 {
		log.Fatal("usage: <url> <query>")
	}
	u, err := url.Parse(os.Args[1])
	if err != nil {
		log.Fatal("error parsing url", err)
	}
	q := u.Query()
	query := `SELECT IncidntNum FROM "SF_incidents2016.json"`
	if len(os.Args) > 2 {
		query = os.Args[2]
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
}
