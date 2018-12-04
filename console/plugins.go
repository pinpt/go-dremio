package console

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"os"
	"os/exec"
	"strings"

	"github.com/fatih/color"
	pstrings "github.com/pinpt/go-common/strings"
)

var showTables = Plugin{
	Query:       "^show tables$",
	Description: "Shows all tables",
	Callback:    showTablesFunc,
}
var describeTables = Plugin{
	Query:       "^desc ",
	Description: "Call desc <table_name>",
	Callback:    describeTablesFunc,
}
var showHelp = Plugin{
	Query:       "^help$",
	Description: "Shows this help dialog",
	Callback:    showHelpFunc,
}

var regexpExample = Plugin{
	Query:       "^reg_example\\s",
	Description: "Quick test for starts with",
	Callback: func(ctx context.Context, conn *sql.DB, input string) error {
		fmt.Println(input)
		return nil
	},
}

var clearScreen = Plugin{
	Query:       "^clear$",
	Description: "clears the screen",
	Callback: func(ctx context.Context, conn *sql.DB, input string) error {
		cmd := exec.Command("clear")
		cmd.Stdout = os.Stdout
		return cmd.Run()
	},
}

func showHelpFunc(ctx context.Context, conn *sql.DB, input string) error {
	fmt.Println("Available commands:")
	fmt.Println("")
	var longest float64
	for _, p := range queryPlugins {
		longest = math.Max(float64(len(p.Query)), float64(20))
	}
	for _, p := range queryPlugins {
		fmt.Println(color.HiWhiteString("  " + pstrings.PadRight(p.Query, int(longest), ' ') + " " + color.CyanString(p.Description)))
	}
	fmt.Println("")
	return nil
}

func showTablesFunc(ctx context.Context, conn *sql.DB, input string) error {
	rows, err := conn.QueryContext(ctx, `SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE FROM INFORMATION_SCHEMA."TABLES"`)
	if err != nil {
		return err
	}
	type res struct {
		Schema string `json:"TABLE_SCHEMA"`
		Name   string `json:"TABLE_NAME"`
		Type   string `json:"TABLE_TYPE"`
	}
	for rows.Next() {
		var a res
		rows.Scan(&a.Schema, &a.Name, &a.Type)
		if a.Type == "TABLE" || a.Type == "VIEW" {
			fmt.Println(color.HiWhiteString("  " + JoinWords([]string{a.Schema, a.Name}, ".")))
		}
	}
	return nil
}

func dequote(val string) string {
	if val[0:1] == `"` {
		return val[1 : len(val)-1]
	}
	return val
}

func describeTablesFunc(ctx context.Context, conn *sql.DB, query string) error {
	table := strings.TrimSpace(query[5:])
	tok := strings.Split(table, ".")
	sql := `SELECT * FROM INFORMATION_SCHEMA."COLUMNS"`
	var hasschema bool
	if len(tok) > 1 {
		schemastrs := make([]string, 0)
		for _, s := range tok[0 : len(tok)-1] {
			schemastrs = append(schemastrs, dequote(s))
		}
		schema := strings.Join(schemastrs, ".")
		table = dequote(tok[len(tok)-1])
		hasschema = true
		sql += `WHERE TABLE_SCHEMA = '` + schema + `' AND TABLE_NAME = '` + table + "' order by ORDINAL_POSITION"
	} else {
		sql += `WHERE TABLE_NAME = '` + table + "' order by TABLE_SCHEMA, ORDINAL_POSITION"
	}
	rows, err := conn.QueryContext(ctx, `SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE FROM INFORMATION_SCHEMA."TABLES"`)

	if err != nil {
		return err
	}

	type res struct {
		Schema string `json:"TABLE_SCHEMA"`
		Name   string `json:"COLUMN_NAME"`
		Type   string `json:"DATA_TYPE"`
	}

	for rows.Next() {
		var a res
		rows.Scan(&a.Schema, &a.Name, &a.Type)
		if hasschema {
			fmt.Println(color.HiWhiteString("  " + pstrings.PadRight(a.Name, 50, ' ') + " " + color.CyanString(a.Type)))
		} else {
			fmt.Println(color.HiWhiteString("  " + pstrings.PadRight(JoinWords([]string{a.Schema, a.Name}, "."), 50, ' ') + " " + color.CyanString(a.Type)))
		}
	}
	return nil
}

func init() {
	Register(showTables)
	Register(describeTables)
	Register(showHelp)
	Register(clearScreen)
	Register(regexpExample)
}
