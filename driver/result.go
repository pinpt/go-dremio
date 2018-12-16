package drill

import (
	"bytes"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"strings"
	"time"
)

type jobState struct {
	RowCount     int       `json:"rowCount"`
	State        string    `json:"jobState"`
	ErrorMessage *string   `json:"errorMessage,omitempty"`
	StartedAt    time.Time `json:"startedAt"`
	EndedAt      time.Time `json:"endedAt"`
}

type jobResults struct {
	RowCount int                      `json:"rowCount"`
	Schema   []schema                 `json:"schema"`
	Rows     []map[string]interface{} `json:"rows"`
}

func (r *jobResults) Read(in io.Reader) error {
	return json.NewDecoder(in).Decode(r)
}

type schema struct {
	Name string `json:"name"`
}

type jobid struct {
	ID string `json:"id"`
}

type columns []string

type result struct {
	jobid   string
	conn    *connection
	columns columns
	rows    *rows
	offset  int
	total   int
}

func fetchNextPage(conn *connection, jobid string, offset int, total int, res *result) error {
	resp, err := conn.get(conn.getResultURL(jobid, offset, conn.pagesize))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	var jr jobResults
	if err := jr.Read(resp.Body); err != nil {
		return err
	}
	res.total = total
	res.offset += len(jr.Rows)
	if res.columns == nil {
		res.columns = make(columns, 0)
		for _, e := range jr.Schema {
			res.columns = append(res.columns, e.Name)
		}
	}
	res.rows = &rows{
		parent: res,
		rows:   make([]map[string]interface{}, 0),
	}
	for _, row := range jr.Rows {
		res.rows.rows = append(res.rows.rows, row)
	}
	return nil
}

func newResult(conn *connection, jobid string, query *query) (driver.Rows, error) {
	jobResultURL := conn.getResultStatusURL(jobid)
	var state jobState
end:
	for {
		resp, err := conn.get(jobResultURL)
		if err != nil {
			return nil, err
		}
		if err := json.NewDecoder(resp.Body).Decode(&state); err != nil {
			resp.Body.Close()
			return nil, fmt.Errorf("error decoding JSON response. %v", err)
		}
		resp.Body.Close()
		switch state.State {
		case "FAILED":
			if strings.Contains(*state.ErrorMessage, "SCHEMA_CHANGE ERROR") {
				// this is a failure that needs to be restarted because the schema is in learning mode
				time.Sleep(time.Millisecond * 20)
				return query.send(conn, query.buf)
			}
			return nil, errors.New(*state.ErrorMessage)
		case "COMPLETED":
			break end
		default:
			time.Sleep(time.Millisecond)
		}
	}
	resp, err := conn.get(conn.getResultURL(jobid, 0, conn.pagesize))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var jr jobResults
	var result result
	buf, _ := ioutil.ReadAll(resp.Body)
	if bytes.HasPrefix(buf, []byte("{")) {
		if err := jr.Read(bytes.NewReader(buf)); err != nil {
			return nil, err
		}
		if err := fetchNextPage(conn, jobid, 0, jr.RowCount, &result); err != nil {
			return nil, err
		}
	} else {
		result.total = 0
		result.offset = 0
		if result.columns == nil {
			result.columns = make(columns, 0)
		}
		result.rows = &rows{
			parent: &result,
			rows:   make([]map[string]interface{}, 0),
		}
	}
	result.jobid = jobid
	result.conn = conn
	return result.rows, nil
}

type rows struct {
	parent *result
	rows   []map[string]interface{}
	index  int
}

var _ driver.Rows = (*rows)(nil)

// Columns returns the names of the columns. The number of
// columns of the result is inferred from the length of the
// slice. If a particular column name isn't known, an empty
// string should be returned for that entry.
func (r *rows) Columns() []string {
	return r.parent.columns
}

// Close closes the rows iterator.
func (r *rows) Close() error {
	return nil
}

// Next is called to populate the next row of data into
// the provided slice. The provided slice will be the same
// size as the Columns() are wide.
//
// Next should return io.EOF when there are no more rows.
//
// The dest should not be written to outside of Next. Care
// should be taken when closing Rows not to modify
// a buffer held in dest.
func (r *rows) Next(dest []driver.Value) error {
	if r.index >= len(r.rows) {
		if r.parent.offset < r.parent.total {
			if err := fetchNextPage(r.parent.conn, r.parent.jobid, r.parent.offset, r.parent.total, r.parent); err != nil {
				return err
			}
			r.index = r.parent.rows.index
			r.rows = r.parent.rows.rows
		} else {
			r.rows = nil
		}
	}
	if len(dest) != len(r.parent.columns) {
		return fmt.Errorf("invalid scan, expected %d arguments and received %d", len(r.parent.columns), len(dest))
	}
	if r.rows == nil || len(r.rows) == 0 {
		return sql.ErrNoRows
	}
	therow := r.rows[r.index]
	for i := 0; i < len(r.parent.columns); i++ {
		key := r.parent.columns[i]
		val := therow[key]
		dest[i] = val
	}
	r.index++
	return nil
}
