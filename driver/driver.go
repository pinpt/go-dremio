package drill

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
)

// ErrTransactionNotSupported is returned if transactions are attempted to be used
var ErrTransactionNotSupported = errors.New("transactions not supported")

// ErrNonQueryNotSupported is returned if a non query is executed
var ErrNonQueryNotSupported = errors.New("non queries not supported")

type db struct {
}

// make sure our db implements the full driver interface
var _ driver.Driver = (*db)(nil)

type connection struct {
	proto    string
	hostname string
	port     int
	token    string
	pagesize int
	context  []string
}

// make sure our connection implements the full driver.Conn interfaces
var _ driver.Conn = (*connection)(nil)
var _ driver.Queryer = (*connection)(nil)
var _ driver.QueryerContext = (*connection)(nil)
var _ driver.Execer = (*connection)(nil)
var _ driver.ExecerContext = (*connection)(nil)
var _ driver.Pinger = (*connection)(nil)
var _ driver.ConnBeginTx = (*connection)(nil)
var _ driver.SessionResetter = (*connection)(nil)
var _ driver.ConnPrepareContext = (*connection)(nil)

type statement struct {
	query string
	conn  *connection
	ctx   context.Context
}

// make sure our statement implements the full driver.Stmt interfaces
var _ driver.Stmt = (*statement)(nil)
var _ driver.StmtExecContext = (*statement)(nil)
var _ driver.StmtQueryContext = (*statement)(nil)

type query struct {
	Query   string   `json:"sql"`
	Context []string `json:"context,omitempty"`

	buf []byte
}

var paramRe = regexp.MustCompile("\\s+(\\?)([\\s,]?)")

type valuer func(index int) driver.Value

func replacePlaceholders(q string, v valuer) string {
	var index int
	return paramRe.ReplaceAllStringFunc(q, func(s string) string {
		val := v(index)
		index++
		if val != nil {
			var res string
			switch v := val.(type) {
			case string:
				res = fmt.Sprintf(` '%s' `, v)
			default:
				res = fmt.Sprintf(" %v ", val)
			}
			if strings.HasSuffix(s, ",") {
				res += ","
			}
			return res
		}
		return s
	})
}

func (q query) buildNamed(args []driver.NamedValue) ([]byte, error) {
	if len(args) > 0 {
		q.Query = replacePlaceholders(q.Query, func(index int) driver.Value {
			if index < len(args) {
				return args[index].Value
			}
			return nil
		})
	}
	return json.Marshal(q)
}

func (q query) build(args []driver.Value) ([]byte, error) {
	if len(args) > 0 {
		q.Query = replacePlaceholders(q.Query, func(index int) driver.Value {
			if index < len(args) {
				return args[index]
			}
			return nil
		})
	}
	return json.Marshal(q)
}

func (q query) send(c *connection, buf []byte) (driver.Rows, error) {
	resp, err := c.post(c.getQueryURL(), buf)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		switch resp.StatusCode {
		case http.StatusBadGateway:
		case http.StatusGone:
		case http.StatusNotFound:
			return nil, sql.ErrConnDone
		}
		buf, _ := ioutil.ReadAll(resp.Body)
		return nil, errors.New(string(buf))
	}
	var job jobid
	if err := json.NewDecoder(resp.Body).Decode(&job); err != nil {
		return nil, fmt.Errorf("query error. status code: %v", resp.StatusCode)
	}
	q.buf = buf
	return newResult(c, job.ID, &q)
}

func (q query) query(ctx context.Context, c *connection, args []driver.Value) (driver.Rows, error) {
	r, err := q.build(args)
	if err != nil {
		return nil, err
	}
	return q.send(c, r)
}

func (q query) queryNamed(ctx context.Context, c *connection, args []driver.NamedValue) (driver.Rows, error) {
	r, err := q.buildNamed(args)
	if err != nil {
		return nil, err
	}
	return q.send(c, r)
}

const defaultPort = 443
const defaultPageSize = 500

// Open returns a new connection to the database.
// The name is a string in a driver-specific format.
//
// Open may return a cached connection (one previously
// closed), but doing so is unnecessary; the sql package
// maintains a pool of idle connections for efficient re-use.
//
// The returned connection is only used by one goroutine at a
// time.
func (d *db) Open(name string) (driver.Conn, error) {
	u, err := url.Parse(name)
	if err != nil {
		return nil, err
	}

	port := defaultPort

	if u.Port() != "" {
		port, err = strconv.Atoi(u.Port())
		if err != nil {
			return nil, err
		}
	}

	if u.User == nil {
		return nil, fmt.Errorf("missing username and password")
	}

	pagesize := defaultPageSize
	ps := u.Query().Get("pagesize")
	if ps != "" {
		pagesize, err = strconv.Atoi(ps)
		if err != nil {
			return nil, fmt.Errorf("error parsing pagesize. %v", err)
		}
		if pagesize <= 0 || pagesize > 500 {
			return nil, fmt.Errorf("invalid page size. must be between 1-500")
		}
	}

	var context []string
	c := u.Query().Get("context")
	if c != "" {
		// try and turn something like Samples."samples.dremio.com" -> [Samples, samples.dremio.com]
		re := regexp.MustCompile(`("[\w+\.\s]+")|([\w]+)`)
		toks := re.FindAllString(c, -1)
		for _, tok := range toks {
			context = append(context, strings.Replace(tok, `"`, "", -1))
		}
	}

	conn := &connection{
		hostname: u.Hostname(),
		port:     port,
		proto:    u.Scheme,
		pagesize: pagesize,
		context:  context,
	}

	// attempt to login to the connection
	pass, _ := u.User.Password()
	url := fmt.Sprintf("%s://%s:%d/apiv2/login", conn.proto, conn.hostname, conn.port)
	un := fmt.Sprintf(`{"userName":"%s","password":"%s"}`, u.User.Username(), pass)
	req, err := http.NewRequest(http.MethodPost, url, strings.NewReader(un))
	req.Header.Set("Content-Type", "application/json")
	if err != nil {
		return nil, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	type loginToken struct {
		Token        string  `json:"token"`
		ErrorMessage *string `json:"errorMessage,omitempty"`
	}
	var token loginToken
	if err := json.NewDecoder(resp.Body).Decode(&token); err != nil {
		return nil, fmt.Errorf("error decoding login token. %v", err)
	}
	if token.ErrorMessage != nil {
		return nil, fmt.Errorf("error during login: %v", *token.ErrorMessage)
	}
	conn.token = token.Token
	return conn, nil
}

func (c *connection) getResultStatusURL(id string) string {
	return fmt.Sprintf("%s://%s:%d/api/v3/job/%s", c.proto, c.hostname, c.port, id)
}

func (c *connection) getResultURL(id string, offset int, limit int) string {
	return fmt.Sprintf("%s://%s:%d/api/v3/job/%s/results?offset=%d&limit=%d", c.proto, c.hostname, c.port, id, offset, limit)
}

func (c *connection) getQueryURL() string {
	return fmt.Sprintf("%s://%s:%d/api/v3/sql", c.proto, c.hostname, c.port)
}

func (c *connection) post(url string, buf []byte) (*http.Response, error) {
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(buf))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "_dremio"+c.token)
	return http.DefaultClient.Do(req)
}

func (c *connection) get(url string) (*http.Response, error) {
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Authorization", "_dremio"+c.token)
	return http.DefaultClient.Do(req)
}

// Prepare returns a prepared statement, bound to this connection.
func (c *connection) Prepare(query string) (driver.Stmt, error) {
	return &statement{query, c, context.Background()}, nil
}

func (c *connection) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	return &statement{query, c, ctx}, nil
}

// Close invalidates and potentially stops any current
// prepared statements and transactions, marking this
// connection as no longer in use.
//
// Because the sql package maintains a free pool of
// connections and only calls Close when there's a surplus of
// idle connections, it shouldn't be necessary for drivers to
// do their own connection caching.
func (c *connection) Close() error {
	// don't shutdown the connection, we'll internally handle it
	return nil
}

// Begin starts and returns a new transaction.
//
// Deprecated: Drivers should implement ConnBeginTx instead (or additionally).
func (c *connection) Begin() (driver.Tx, error) {
	return nil, ErrTransactionNotSupported
}

// BeginTx starts and returns a new transaction.
// If the context is canceled by the user the sql package will
// call Tx.Rollback before discarding and closing the connection.
//
// This must check opts.Isolation to determine if there is a set
// isolation level. If the driver does not support a non-default
// level and one is set or if there is a non-default isolation level
// that is not supported, an error must be returned.
//
// This must also check opts.ReadOnly to determine if the read-only
// value is true to either set the read-only transaction property if supported
// or return an error if it is not supported.
func (c *connection) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	return nil, ErrTransactionNotSupported
}

// ResetSession is called while a connection is in the connection
// pool. No queries will run on this connection until this method returns.
//
// If the connection is bad this should return driver.ErrBadConn to prevent
// the connection from being returned to the connection pool. Any other
// error will be discarded.
func (c *connection) ResetSession(ctx context.Context) error {
	return nil
}

// Pinger is an optional interface that may be implemented by a Conn.
//
// If a Conn does not implement Pinger, the sql package's DB.Ping and
// DB.PingContext will check if there is at least one Conn available.
//
// If Conn.Ping returns ErrBadConn, DB.Ping and DB.PingContext will remove
// the Conn from pool.
func (c *connection) Ping(ctx context.Context) error {
	return nil
}

// Execer is an optional interface that may be implemented by a Conn.
//
// If a Conn implements neither ExecerContext nor Execer Execer,
// the sql package's DB.Exec will first prepare a query, execute the statement,
// and then close the statement.
//
// Exec may return ErrSkip.
//
// Deprecated: Drivers should implement ExecerContext instead.
func (c *connection) Exec(query string, args []driver.Value) (driver.Result, error) {
	return nil, ErrNonQueryNotSupported
}

// ExecerContext is an optional interface that may be implemented by a Conn.
//
// If a Conn does not implement ExecerContext, the sql package's DB.Exec
// will fall back to Execer; if the Conn does not implement Execer either,
// DB.Exec will first prepare a query, execute the statement, and then
// close the statement.
//
// ExecerContext may return ErrSkip.
//
// ExecerContext must honor the context timeout and return when the context is canceled.
func (c *connection) ExecContext(ctx context.Context, rawQuery string, nargs []driver.NamedValue) (driver.Result, error) {
	return nil, ErrNonQueryNotSupported
}

// Queryer is an optional interface that may be implemented by a Conn.
//
// If a Conn implements neither QueryerContext nor Queryer,
// the sql package's DB.Query will first prepare a query, execute the statement,
// and then close the statement.
//
// Query may return ErrSkip.
//
// Deprecated: Drivers should implement QueryerContext instead.
func (c *connection) Query(query string, args []driver.Value) (driver.Rows, error) {
	return nil, nil
}

// QueryerContext is an optional interface that may be implemented by a Conn.
//
// If a Conn does not implement QueryerContext, the sql package's DB.Query
// will fall back to Queryer; if the Conn does not implement Queryer either,
// DB.Query will first prepare a query, execute the statement, and then
// close the statement.
//
// QueryerContext may return ErrSkip.
//
// QueryerContext must honor the context timeout and return when the context is canceled.
func (c *connection) QueryContext(ctx context.Context, rawQuery string, args []driver.NamedValue) (driver.Rows, error) {
	q := query{rawQuery, c.context, nil}
	return q.queryNamed(ctx, c, args)
}

// Close closes the statement.
//
// As of Go 1.1, a Stmt will not be closed if it's in use
// by any queries.
func (s *statement) Close() error {
	return nil
}

// NumInput returns the number of placeholder parameters.
//
// If NumInput returns >= 0, the sql package will sanity check
// argument counts from callers and return errors to the caller
// before the statement's Exec or Query methods are called.
//
// NumInput may also return -1, if the driver doesn't know
// its number of placeholders. In that case, the sql package
// will not sanity check Exec or Query argument counts.
func (s *statement) NumInput() int {
	return -1
}

// Query executes a query that may return rows, such as a
// SELECT.
//
// Deprecated: Drivers should implement StmtQueryContext instead (or additionally).
func (s *statement) Query(args []driver.Value) (driver.Rows, error) {
	q := query{s.query, s.conn.context, nil}
	return q.query(context.Background(), s.conn, args)
}

// QueryContext executes a query that may return rows, such as a
// SELECT.
//
// QueryContext must honor the context timeout and return when it is canceled.
func (s *statement) QueryContext(ctx context.Context, nargs []driver.NamedValue) (driver.Rows, error) {
	q := query{s.query, s.conn.context, nil}
	return q.queryNamed(ctx, s.conn, nargs)
}

// Exec executes a query that doesn't return rows, such
// as an INSERT or UPDATE.
//
// Deprecated: Drivers should implement StmtExecContext instead (or additionally).
func (s *statement) Exec(args []driver.Value) (driver.Result, error) {
	return nil, ErrNonQueryNotSupported
}

// ExecContext executes a query that doesn't return rows, such
// as an INSERT or UPDATE.
//
// ExecContext must honor the context timeout and return when it is canceled.
func (s *statement) ExecContext(ctx context.Context, nargs []driver.NamedValue) (driver.Result, error) {
	return nil, ErrNonQueryNotSupported
}

// namedValueToValue is a helper function copied from the database/sql package
func namedValueToValue(named []driver.NamedValue) ([]driver.Value, error) {
	dargs := make([]driver.Value, len(named))
	for n, param := range named {
		if len(param.Name) > 0 {
			return nil, errors.New("sql: driver does not support the use of Named Parameters")
		}
		dargs[n] = param.Value
	}
	return dargs, nil
}

// DriverName is the public name of the driver
const DriverName = "dremio"

func init() {
	sql.Register(DriverName, &db{})
}
