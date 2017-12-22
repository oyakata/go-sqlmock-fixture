package fixture

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
)

const query = `SELECT id, name, age, superuser, birthdady FROM person WHERE name != ''`

var jst = time.FixedZone("JST", 9*60*60)

func doSomething(db *sql.DB) ([]string, error) {
	xs := []string{}
	rows, err := db.Query(query)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var (
			id        int
			name      string
			age       uint8
			superuser bool
			birthday  time.Time
		)
		if err := rows.Scan(&id, &name, &age, &superuser, &birthday); err != nil {
			return nil, err
		}
		xs = append(xs, fmt.Sprintf("%d, %s, %d, %v, %v", id, name, age, superuser, birthday))
	}
	return xs, nil
}

func TestInspect(t *testing.T) {
	s := [][]struct {
		id   int
		name string
	}{
		{{1, "a"}, {2, "b"}},
		{},
	}

	type Field struct {
		name string
		kind reflect.Kind
	}
	helper := func(fields []reflect.StructField) []Field {
		f := make([]Field, len(fields))
		for i, x := range fields {
			f[i] = Field{x.Name, x.Type.Kind()}
		}
		return f
	}
	expectedFieldTypes := []Field{
		{"id", reflect.Int},
		{"name", reflect.String},
	}

	fields, items := Inspect(s[0])
	assert.Equal(t, expectedFieldTypes, helper(fields))
	assert.Equal(t, [][]driver.Value{{int64(1), "a"}, {int64(2), "b"}}, items)

	fields, items = Inspect(s[1])
	assert.Equal(t, expectedFieldTypes, helper(fields))
	assert.Equal(t, [][]driver.Value{}, items)

	assert.PanicsWithValue(t, ErrNotASlice, func() {
		Inspect(0)
	})
}

const text = `
- id: 1
  name: foo
  super: true
- id: 2
  name: bar
  super: false
`

func TestFromYAML(t *testing.T) {
	rows := FromYAML([]byte(text))
	expected := sqlmock.NewRows([]string{"id", "name", "super"}).AddRow(int64(1), "foo", true).AddRow(int64(2), "bar", false)

	assert.Equal(t, expected, rows)
}

func TestNewRows(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()

	mock.ExpectQuery(query).WillReturnRows(NewRows([]struct {
		id        int
		name      string
		age       uint8
		superuser bool
		birthdady time.Time
	}{
		{1, "imagawa", 13, false, time.Date(1560, 7, 10, 0, 0, 0, 0, jst)},
		{2, "yoshimot", 20, true, time.Date(1560, 7, 11, 0, 0, 0, 0, jst)},
		{3, "moyomot", 35, false, time.Time{}},
	}))

	mock.ExpectQuery(query).WillReturnRows(NewRows([]struct {
		id        int
		name      string
		age       uint8
		superuser bool
		birthdady time.Time
	}{
		{1, "imagawax", 13, false, time.Date(1560, 7, 10, 0, 0, 0, 0, jst)},
	}))

	// first call
	result, err := doSomething(db)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, []string{
		"1, imagawa, 13, false, 1560-07-10 00:00:00 +0900 JST",
		"2, yoshimot, 20, true, 1560-07-11 00:00:00 +0900 JST",
		"3, moyomot, 35, false, 0001-01-01 00:00:00 +0000 UTC",
	}, result)

	// second call
	result, err = doSomething(db)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, []string{
		"1, imagawax, 13, false, 1560-07-10 00:00:00 +0900 JST",
	}, result)

	// check mock expectations
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
