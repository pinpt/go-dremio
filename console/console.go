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
	"regexp"
	"strings"
	"time"

	"github.com/chzyer/readline"
	"github.com/fatih/color"
	"github.com/pinpt/go-common/fileutil"

	_ "github.com/pinpt/go-dremio/driver"
)

// Plugin ...
type Plugin struct {
	Query       string
	Description string
	Callback    func(ctx context.Context, conn *sql.DB, input string) error
	AfterQuery  func(ctx context.Context, res []map[string]interface{}, duration time.Duration) (bool, error)
}

var (
	dremioURL      string
	dremioUsername string
	dremioPassword string
	dremioToken    string
	dremioConfig   string

	queryPlugins []Plugin
)

// Register registers plugin
func Register(p Plugin) {
	queryPlugins = append(queryPlugins, p)
}

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
	fmt.Print("INFO testing connection to dremio server .... ")
	_, err := conn.QueryContext(ctx, "select * from INFORMATION_SCHEMA.CATALOGS")
	if err != nil {
		fmt.Println("failed")
		return err
	}
	fmt.Println("succeeded")
	return nil
}

func checkPlugin(ctx context.Context, conn *sql.DB, input string) (bool, *Plugin, error) {
	for _, p := range queryPlugins {
		m, e := regexp.MatchString(p.Query, input)
		if e != nil {
			return false, &p, e
		}
		if m {
			return true, &p, p.Callback(ctx, conn, input)
		}
	}
	return false, nil, nil
}

func sqlPrompt(ctx context.Context, conn *sql.DB, query string) ([]map[string]interface{}, error) {
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

func trim(val string) string {
	val = strings.TrimSpace(val)
	if strings.HasSuffix(val, ";") {
		return val[0 : len(val)-1]
	}
	return val
}

func startPrompt(ctx context.Context, conn *sql.DB, rl *readline.Instance) error {
	shouldExit := false
	for {
		var text string
		var err error
		var data []map[string]interface{}
		var started time.Time
		var pluginFound bool

		rl.SetPrompt("> ")
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
		query := trim(text)
		if len(query) == 0 {
			continue
		}
		if query == "exit" || query == "quit" {
			return nil
		}
		started = time.Now()
		pluginFound, plugin, err := checkPlugin(ctx, conn, query)
		if err != nil {
			goto errors
		}
		if pluginFound {
			fmt.Println(fmt.Sprintf("took %v", time.Since(started)))
			continue
		}
		data, err = sqlPrompt(ctx, conn, query)
		if err != nil {
			goto errors
		}
		if plugin != nil && plugin.AfterQuery != nil {
			ok, err := plugin.AfterQuery(ctx, data, time.Since(started))
			if err != nil {
				goto errors
			}
			if !ok {
				continue // if return false, don't print out, just continue
			}
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
