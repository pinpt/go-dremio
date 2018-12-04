package console

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/url"
	"os"
	"os/user"
	"path/filepath"
	"strings"
	"time"

	"github.com/chzyer/readline"
	"github.com/fatih/color"
	"github.com/pinpt/go-common/fileutil"

	pstrings "github.com/pinpt/go-common/strings"
	_ "github.com/pinpt/go-dremio"
)

var (
	dremioURL      string
	dremioUsername string
	dremioPassword string
	dremioToken    string
	dremioConfig   string
)

// Run runs the sql console
func Run() error {
	var rl *readline.Instance
	var err error
	var ctx context.Context
	var conn *sql.DB
	var connURL *url.URL
	var connString string
	var credsExists bool

	ctx = context.Background()
	rl, err = readline.New("")
	if err != nil {
		return err
	}
	defer rl.Close()
	credsExists, err = setupCredentials()
	if err != nil {
		return err
	}
	if !credsExists {
		promptURL(rl)
		promptUsername(rl)
		promptPassword(rl)
		if err := promptSaveCredsToFile(rl); err != nil {
			return err
		}
	}
	//  get the correct url format
	connURL, err = url.ParseRequestURI(dremioURL)
	if err != nil {
		return err
	}
	connString = fmt.Sprintf("%v://%v:%v@%v", connURL.Scheme, dremioUsername, dremioPassword, connURL.Host)
	conn, err = sql.Open("dremio", connString)
	if err != nil {
		return err
	}
	defer conn.Close()
	err = testConnection(ctx, conn)
	if err != nil {
		return err
	}
	return startPrompt(ctx, conn, rl)
}

type dremioCreds struct {
	Username string `json:"username"`
	Password string `json:"password"`
	URL      string `json:"url"`
}

// SetCredentials prepopulates creditials but does not save them to dist
func SetCredentials(username, password, url string) {
	dremioUsername = username
	dremioPassword = password
	dremioURL = url
}

func setupCredentials() (bool, error) {

	if dremioUsername != "" && dremioPassword != "" && dremioURL != "" {
		return true, nil
	}

	if !fileutil.FileExists(dremioConfig) {
		return false, nil
	}
	js, err := os.Open(dremioConfig)
	defer js.Close()
	if err != nil {
		return false, err
	}
	var creds dremioCreds
	err = json.NewDecoder(js).Decode(&creds)
	if err != nil {
		return false, err
	}
	dremioUsername = creds.Username
	dremioPassword = creds.Password
	dremioURL = creds.URL
	return true, nil
}

func saveCredentials() error {

	f, err := os.Create(dremioConfig)
	if err != nil {
		return err
	}
	j := dremioCreds{
		Username: dremioUsername,
		Password: dremioPassword,
		URL:      dremioURL,
	}
	str, _ := json.MarshalIndent(j, "", "  ")
	_, err = f.WriteString(string(str))
	if err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

func promptSaveCredsToFile(rl *readline.Instance) error {
	for {
		rl.SetPrompt("Save credentials? (y or n) ")
		yn, _ := rl.Readline()
		if strings.HasPrefix(yn, "n") {
			return nil
		} else if strings.HasPrefix(yn, "y") {
			return saveCredentials()
		}
	}

}
func promptPassword(rl *readline.Instance) {
	if dremioPassword != "" {
		return
	}
	pswd, _ := rl.ReadPasswordEx("", readline.FuncListener(func(line []rune, pos int, key rune) (newLine []rune, newPos int, ok bool) {
		str := "Enter password: "
		for range line {
			str += "*"
		}
		rl.SetPrompt(str)
		rl.Refresh()
		return nil, 0, false
	}))
	dremioPassword = strings.TrimSpace(string(pswd))
}

func promptUsername(rl *readline.Instance) {
	if dremioUsername != "" {
		return
	}
	rl.SetPrompt("Enter username:")
	user, _ := rl.Readline()
	dremioUsername = strings.TrimSpace(user)
}

func promptURL(rl *readline.Instance) {
	if dremioURL != "" {
		return
	}
	rl.SetPrompt("Enter url:")
	ur, _ := rl.Readline()
	dremioURL = strings.TrimSpace(ur)
}

func testConnection(ctx context.Context, conn *sql.DB) error {
	fmt.Println("testing connection")
	_, err := conn.QueryContext(ctx, "select * from INFORMATION_SCHEMA.CATALOGS")
	if err != nil {
		fmt.Println("failed")
		return err
	}
	fmt.Println("succeded")
	return nil
}

// TODO: Make this prettier
func showHelp() {
	fmt.Print(`
  help menu:
	
	  type "exit" to exit
	  type "quit" to exit
	  type any sql query for dremio
`)
}

// handle special options, returns:
//	- ignore	bool	Call "continue" on the for loop
//	- exit		bool	Exit program
//	- err		bool	Any error that might occur
func promptOptions(input string) (bool, bool, error) {
	if input == "exit" {
		return false, true, nil
	}
	if input == "quit" {
		return false, true, nil
	}
	if input == "help" {
		showHelp()
		return true, false, nil
	}
	return false, false, nil
}

func showTables(ctx context.Context, conn *sql.DB) error {
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

func describeTables(ctx context.Context, conn *sql.DB, query string) error {
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

func sqlPrompt(ctx context.Context, conn *sql.DB, query string) ([]map[string]interface{}, error) {
	if strings.ToLower(query) == "show tables" {
		showTables(ctx, conn)
		return nil, nil
	}
	if strings.HasPrefix(strings.ToLower(query), "desc ") {
		describeTables(ctx, conn, query)
		return nil, nil
	}

	rows, err := conn.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	count := len(columns)
	values := make([]interface{}, count)
	scanArgs := make([]interface{}, count)
	for i := range values {
		scanArgs[i] = &values[i]
	}
	var masterData []map[string]interface{}
	for rows.Next() {
		err := rows.Scan(scanArgs...)
		if err != nil {
			return nil, err
		}
		m := make(map[string]interface{})
		for i, v := range values {
			m[columns[i]] = v
		}
		masterData = append(masterData, m)
	}
	return masterData, nil
}

func startPrompt(ctx context.Context, conn *sql.DB, rl *readline.Instance) error {
	shouldExit := false
	for {
		var text string
		var err error
		var data []map[string]interface{}
		var started time.Time

		rl.SetPrompt(" > ")
		text, err = rl.Readline()
		if err != nil {
			if err == readline.ErrInterrupt {
				if text == "" {
					if !shouldExit {
						fmt.Println(`(To exit, press ^C again or type "exit")`)
					} else {
						return nil
					}
					shouldExit = true
				}
				continue
			} else {
				return err
			}
		}
		shouldExit = false
		query := strings.TrimSpace(text)
		if len(query) == 0 {
			continue
		}
		ignore, exit, err := promptOptions(query)
		if exit {
			return nil
		}
		if ignore {
			continue
		}
		if err != nil {
			goto errors
		}
		started = time.Now()
		data, err = sqlPrompt(ctx, conn, query)
		if err != nil {
			goto errors
		}
		if data != nil {
			var b []byte
			b, err = json.MarshalIndent(data, "", "  ")
			if err != nil {
				goto errors
			}
			fmt.Println(color.HiWhiteString(string(b)))
			fmt.Println(fmt.Sprintf("%v rows in set (%v)", len(data), time.Since(started)))
		}
		continue

	errors:
		fmt.Println(color.HiRedString(err.Error()))
		continue
	}
}

func init() {
	usr, err := user.Current()
	if err != nil {
		log.Fatal(err)
	}
	dremioConfig = filepath.Join(usr.HomeDir, ".dremio_console.json")
}
