package fixture

import (
	"database/sql/driver"
	"errors"
	"reflect"
	"time"
	"unsafe"

	"github.com/DATA-DOG/go-sqlmock"
	"gopkg.in/yaml.v2"
)

var (
	timeType     = reflect.TypeOf(time.Time{})
	ErrNotASlice = errors.New("dbtest.Inspect: argument 'items' is not a slice.")
)

func NewRows(xs interface{}) *sqlmock.Rows {
	fields, items := Inspect(xs)
	names := make([]string, len(fields))
	for i, x := range fields {
		names[i] = x.Name
	}

	rows := sqlmock.NewRows(names)
	for _, values := range items {
		rows.AddRow(values...)
	}
	return rows
}

func FromYAML(text []byte) *sqlmock.Rows {
	items := []yaml.MapSlice{}
	yaml.Unmarshal(text, &items)
	if len(items) == 0 {
		return nil
	}

	first := items[0]
	names := make([]string, len(first))
	for i, x := range first {
		names[i] = x.Key.(string)
	}

	rows := sqlmock.NewRows(names)
	for _, m := range items {
		values := make([]driver.Value, len(names))
		for i, x := range m {
			v := reflect.ValueOf(x.Value)
			values[i] = asType(v)
		}
		rows.AddRow(values...)
	}
	return rows
}

func Inspect(items interface{}) ([]reflect.StructField, [][]driver.Value) {
	t := reflect.TypeOf(items)
	if t.Kind() != reflect.Slice {
		panic(ErrNotASlice)
	}

	el := t.Elem()
	fields := make([]reflect.StructField, el.NumField())
	for i := 0; i < el.NumField(); i++ {
		fields[i] = el.Field(i)
	}

	v := reflect.ValueOf(items)
	vs := make([]reflect.Value, v.Len())
	for i := 0; i < v.Len(); i++ {
		vs[i] = v.Index(i)
	}

	xs := make([][]driver.Value, v.Len())
	for i, x := range vs {
		values := make([]driver.Value, len(fields))
		for j, f := range fields {
			v := x.FieldByName(f.Name)
			values[j] = asType(v)
		}
		xs[i] = values
	}
	return fields, xs
}

func asType(v reflect.Value) driver.Value {
	var r driver.Value
	switch v.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		r = v.Int()
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		r = v.Uint()
	case reflect.Float32, reflect.Float64:
		r = v.Float()
	case reflect.String:
		r = v.String()
	case reflect.Bool:
		r = v.Bool()
	case reflect.Slice:
		r = v.Bytes()
	default:
		if v.Type().ConvertibleTo(timeType) {
			tt := (*time.Time)(unsafe.Pointer(v.Addr().Pointer()))
			r = *tt
		}
	}
	return r
}
