package driver

import (
	"database/sql/driver"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPlaceholderWithForIn(t *testing.T) {
	assert := assert.New(t)
	q := `SELECT
	dir1,
	"value",
	"metadata",
	"ref_id"
FROM
	 devdata.pinpoint."signal"
WHERE
	 dir2 = ?
	 AND dir1 like 'CycleTime%'
	 AND ref_type = 'team'
	 AND time_unit = 180
	 AND dir0 = ?
	  and ref_id IN (?,?,?,?)`
	args := []string{"1548460799000", "5500a5ba8135f296", "9000beafc6358579", "5b7adda6516daee7", "4fa4a5e4578444b5", "5b7adda6516daee7"}
	val := replacePlaceholders(q, func(index int) driver.Value {
		if index < len(args) {
			return args[index]
		}
		return nil
	})
	assert.Equal(`SELECT
	dir1,
	"value",
	"metadata",
	"ref_id"
FROM
	 devdata.pinpoint."signal"
WHERE
	 dir2 =  '1548460799000' 
	 AND dir1 like 'CycleTime%'
	 AND ref_type = 'team'
	 AND time_unit = 180
	 AND dir0 =  '5500a5ba8135f296' 
	  and ref_id IN ( '9000beafc6358579' , '5b7adda6516daee7' , '4fa4a5e4578444b5' , '5b7adda6516daee7' )`, val)
}

func TestPlaceholder(t *testing.T) {
	assert := assert.New(t)
	q := `SELECT * FROM "foo" WHERE id = ?`
	args := []string{"1548460799000"}
	val := replacePlaceholders(q, func(index int) driver.Value {
		if index < len(args) {
			return args[index]
		}
		return nil
	})
	assert.Equal(`SELECT * FROM "foo" WHERE id =  '1548460799000'`, val)
}

func TestMultiplePlaceholder(t *testing.T) {
	assert := assert.New(t)
	q := `SELECT * FROM "foo" WHERE id = ? AND foo=?`
	args := []string{"1548460799000", "1548460799000"}
	val := replacePlaceholders(q, func(index int) driver.Value {
		if index < len(args) {
			return args[index]
		}
		return nil
	})
	assert.Equal(`SELECT * FROM "foo" WHERE id =  '1548460799000'  AND foo= '1548460799000' `, val)
}

func TestPlaceholderWithNoArg(t *testing.T) {
	assert := assert.New(t)
	q := `SELECT * FROM "foo" WHERE id = ?`
	args := []string{}
	val := replacePlaceholders(q, func(index int) driver.Value {
		if index < len(args) {
			return args[index]
		}
		return nil
	})
	assert.Equal(`SELECT * FROM "foo" WHERE id = ?`, val)
}
