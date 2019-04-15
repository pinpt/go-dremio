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
	Query:       `^(show tables)(\s+\w+|$)`,
	Usage:       "show tables <optional portion of table name>",
	Description: "Displays all of the tables in the current schema",
	Callback:    showTablesFunc,
}
var describeTables = Plugin{
	Query:       "^desc ",
	Usage:       "desc [table name]",
	Description: "Provides a decription of the specified table or view",
	Callback:    describeTablesFunc,
}
var showHelp = Plugin{
	Query:       "^help$",
	Usage:       "help",
	Description: "Shows this help dialog",
	Callback:    showHelpFunc,
}

var clearScreen = Plugin{
	Query:       "^clear$",
	Usage:       "clear",
	Description: "Clears the screen",
	Callback: func(ctx context.Context, conn *sql.DB, input string) error {
		cmd := exec.Command("clear")
		cmd.Stdout = os.Stdout
		return cmd.Run()
	},
}

func showHelpFunc(ctx context.Context, conn *sql.DB, input string) error {
	fmt.Println("Available commands:")
	fmt.Println("")
	padding := float64(20)
	for _, p := range queryPlugins {
		padding = math.Max(float64(len(p.Query)), padding)
	}
	for _, p := range queryPlugins {
		var n string
		if p.Usage != "" {
			n = p.Usage
		} else {
			n = p.Query
		}
		fmt.Println(color.HiWhiteString(" " + pstrings.PadRight(n, int(padding), ' ') + "   " + color.CyanString(p.Description)))
	}
	fmt.Println("")
	return nil
}

func showTablesFunc(ctx context.Context, conn *sql.DB, query string) error {
	var tablename string
	parts := strings.Split(query, " ")
	if len(parts) > 2 {
		tablename = parts[2]
	}
	q := `SELECT CONCAT(TABLE_SCHEMA, '.', TABLE_NAME) FROM INFORMATION_SCHEMA."TABLES" WHERE TABLE_TYPE IN ('TABLE', 'VIEW')`
	if tablename != "" {
		q += ` AND CONCAT(TABLE_SCHEMA, '.', TABLE_NAME) LIKE '%%` + tablename + `%%'`
	}
	rows, err := conn.QueryContext(ctx, q)
	if err != nil {
		return err
	}
	for rows.Next() {
		var name string
		rows.Scan(&name)
		fmt.Println(color.HiWhiteString("  " + JoinWords(strings.Split(name, "."), ".")))
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
	sql := `SELECT TABLE_SCHEMA, COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA."COLUMNS" `
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
	rows, err := conn.QueryContext(ctx, sql)
	if err != nil {
		return err
	}
	type res struct {
		tableName string
		tableType string
	}
	var all []res
	padding := float64(20)
	for rows.Next() {
		var schema string
		var name string
		var each res
		rows.Scan(&schema, &name, &each.tableType)
		if hasschema {
			each.tableName = name
		} else {
			each.tableName = JoinWords([]string{schema, name}, ".")
		}
		padding = math.Max(float64(len(each.tableName)), padding)
		all = append(all, each)
	}
	for _, r := range all {
		fmt.Println(color.HiWhiteString(" " + pstrings.PadRight(r.tableName, int(padding), ' ') + "   " + color.CyanString(r.tableType)))
	}
	return nil
}

func init() {
	Register(showTables)
	Register(describeTables)
	Register(showHelp)
	Register(clearScreen)
}
