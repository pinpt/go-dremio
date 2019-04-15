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

	"gopkg.in/AlecAivazis/survey.v1"

	"github.com/chzyer/readline"
	"github.com/fatih/color"
	"github.com/pinpt/go-common/fileutil"

	// load the db driver
	_ "github.com/pinpt/go-dremio/driver"
)

type pluginAfterFunc func(ctx context.Context, res []map[string]interface{}, duration time.Duration) (bool, error)

// Plugin ...
type Plugin struct {
	Query       string
	Usage       string
	Description string
	Callback    func(ctx context.Context, conn *sql.DB, input string) error
	AfterQuery  pluginAfterFunc
}

var (
	dremioURL      string
	dremioUsername string
	dremioPassword string
	dremioToken    string
	dremioConfig   string

	historyFile  string
	queryPlugins []Plugin
)

// Register registers plugin
func Register(p Plugin) {
	queryPlugins = append(queryPlugins, p)
}

// SetConfigFile sets the path for the config file, must be .json
func SetConfigFile(p string) error {
	if strings.HasPrefix(p, ".json") {
		return fmt.Errorf("SetConfigFile failed, file path is not json")
	}
	d, e := fileutil.Resolve(p)
	dremioConfig = d
	return e
}

// SetHistoryFile sets the history file to be used, call this before calling Run
func SetHistoryFile(p string) error {
	var err error
	historyFile, err = fileutil.Resolve(p)
	if err != nil {
		return err
	}
	if !fileutil.FileExists(historyFile) {
		_, err = os.Create(historyFile)
	}
	return err
}

// ConfigFile returns the config file path
func ConfigFile() string {
	return dremioConfig
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
	var autocomplete *readline.PrefixCompleter

	ctx = context.Background()
	rl, err = readline.NewEx(&readline.Config{
		HistoryFile:            historyFile,
		DisableAutoSaveHistory: true,
	})

	if err != nil {
		return err
	}
	defer rl.Close()
	credsExists, err = setupCredentials()
	if err != nil {
		return err
	}
	if !credsExists {
		urlprompt := &survey.Input{
			Message: "Dremio url:",
			Default: "http://localhost:9047",
		}
		survey.AskOne(urlprompt, &dremioURL, nil)
		usrprompt := &survey.Input{
			Message: "Dremio Username:",
		}
		survey.AskOne(usrprompt, &dremioUsername, nil)
		pwdprompt := &survey.Password{
			Message: "Dremio Password:",
		}
		survey.AskOne(pwdprompt, &dremioPassword, nil)
		saveprompt := &survey.Select{
			Message: "Save credentials for next time?",
			Options: []string{"yes", "no"},
			Default: "yes",
		}
		var res string
		survey.AskOne(saveprompt, &res, nil)
		if res == "yes" {
			if err := saveCredentials(); err != nil {
				return err
			}
		}
	}
	//  get the correct url format
	connURL, err = url.ParseRequestURI(dremioURL)
	if err != nil {
		return err
	}
	autocomplete = readline.NewPrefixCompleter(readline.PcItem("select "))
	re := regexp.MustCompile("^[A-Za-z\\s]*")
	for _, p := range queryPlugins {
		if p.Usage != "" {
			words := re.FindStringSubmatch(p.Usage)
			item := readline.PcItem(words[0])
			autocomplete.Children = append(autocomplete.Children, item)
		}
	}
	rl.Config.AutoComplete = autocomplete
	connString = fmt.Sprintf("%v://%v:%v@%v", connURL.Scheme, dremioUsername, dremioPassword, connURL.Host)
	conn, err = sql.Open("dremio", connString)
	if err != nil {
		return err
	}
	defer conn.Close()
	err = testConnection(ctx, conn)
	if err != nil {
		fmt.Println(err, "logging out")
		os.Remove(dremioConfig)
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

func testConnection(ctx context.Context, conn *sql.DB) error {
	fmt.Print("INFO testing connection to dremio server .... ")
	_, err := conn.QueryContext(ctx, "select * from INFORMATION_SCHEMA.CATALOGS")
	if err != nil {
		fmt.Println("failed...")
		return err
	}
	fmt.Println("succeeded")
	return nil
}

// returns:
//	bool   - plugin found
//	func   - plugin after function
//	error  - error, nil if not error
func checkPlugin(ctx context.Context, conn *sql.DB, input string) (bool, pluginAfterFunc, error) {
	for _, p := range queryPlugins {
		m, e := regexp.MatchString(p.Query, input)
		if e != nil {
			return false, p.AfterQuery, e
		}
		if m {
			return true, p.AfterQuery, p.Callback(ctx, conn, input)
		}
	}
	return false, nil, nil
}

// Execute a query and return array of mapped results
func Execute(ctx context.Context, conn *sql.DB, query string) ([]map[string]interface{}, []string, error) {
	rows, err := conn.QueryContext(ctx, query)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		return nil, nil, err
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
			return nil, nil, err
		}
		m := make(map[string]interface{})
		for i, v := range values {
			m[columns[i]] = v
		}
		masterData = append(masterData, m)
	}
	return masterData, columns, nil
}

func trim(val string) (bool, string) {
	val = strings.TrimSpace(val)
	if strings.HasSuffix(val, ";") {
		return true, val[0 : len(val)-1]
	}
	return false, val
}

func startPrompt(ctx context.Context, conn *sql.DB, rl *readline.Instance) error {
	shouldExit := false
	queryArray := []string{}
	for {
		var text string
		var err error
		var data []map[string]interface{}
		var started time.Time
		var pluginFound bool
		var afterQuery pluginAfterFunc

		multiline := len(queryArray) > 0
		rl.SetPrompt("> ")
		text, err = rl.Readline()
		if err != nil {
			if err == readline.ErrInterrupt {
				if multiline {
					fmt.Print(`^C`)
					queryArray = []string{}
					continue
				}
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
		semicolon, query := trim(text)
		if !multiline && len(query) == 0 {
			continue
		}
		started = time.Now()
		if !multiline {
			if query == "exit" || query == "quit" {
				return nil
			}
			pluginFound, afterQuery, err = checkPlugin(ctx, conn, query)
			if err != nil {
				goto errors
			}
			if pluginFound && afterQuery == nil {
				rl.SaveHistory(query)
				fmt.Println(fmt.Sprintf("took %v", time.Since(started)))
				continue
			}
		}
		if strings.HasPrefix(strings.ToLower(query), "select") && !semicolon {
			queryArray = append(queryArray, query)
			continue
		}
		if multiline {
			queryArray = append(queryArray, query)
			if !semicolon {
				continue
			}
			query = strings.Join(queryArray, " ")
		}
		rl.SaveHistory(query + ";")
		data, _, err = Execute(ctx, conn, query)
		queryArray = []string{}
		if err != nil {
			goto errors
		}
		if !multiline {
			if afterQuery != nil {
				ok, err := afterQuery(ctx, data, time.Since(started))
				if err != nil {
					goto errors
				}
				if !ok {
					continue // if return false, don't print out, just continue
				}
			}
		}
		if data != nil {
			var b []byte
			b, err = json.MarshalIndent(data, "", "  ")
			if err != nil {
				goto errors
			}
			if len(b) == 0 {
				fmt.Println(color.HiWhiteString("[]"))
			} else {
				fmt.Println(color.HiWhiteString(string(b)))
			}
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
	SetConfigFile(filepath.Join(usr.HomeDir, ".dremio_console.json"))
}
