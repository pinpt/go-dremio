package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"

	_ "github.com/pinpt/go-dremio"
)

func main() {
	if len(os.Args) != 2 {
		log.Fatal("usage: <url>")
	}
	conn, err := sql.Open("dremio", os.Args[1])
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()
	rows, err := conn.QueryContext(context.Background(), `SELECT IncidntNum FROM Samples."samples.dremio.com"."SF_incidents2016.json"`)
	if err != nil {
		log.Fatal(err)
	}
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
