package sqlscan

import (
	"database/sql"
	"errors"
	"fmt"
	"reflect"
)

var (
	ErrRequiresPtr        = errors.New("sqlkit/encoding: non pointer passed to Decode")
	ErrMustNotBeNil       = errors.New("sqlkit/encoding: nil value passed to Decode")
	ErrMissingDestination = errors.New("sqlkit/encoding: missing destination")
	// ErrTooManyColumns is returned when too many columns are present to scan
	// into a value.
	ErrTooManyColumns = errors.New("sqlkit/encoding: too many columns to scan")
	// ErrNoRows is mirrored from the database/sql package.
	ErrNoRows = sql.ErrNoRows
)

var _scannerInterface = reflect.TypeOf((*sql.Scanner)(nil)).Elem()

// isScannable takes the reflect.Type and the actual dest value and returns
// whether or not it's Scannable.  Something is scannable if:
//   * it is not a struct
//   * it implements sql.Scanner
//   * it has no exported fields
func isScannable(t reflect.Type) bool {
	if reflect.PtrTo(t).Implements(_scannerInterface) {
		return true
	}
	if t.Kind() != reflect.Struct {
		return true
	}
	if len(DefaultMapper.TypeMap(t).Index) == 0 {
		return true
	}
	return false
}

type nilSafety struct {
	dest interface{}
}

func (n *nilSafety) Scan(src interface{}) error {
	if src == nil {
		return nil
	}
	return Assign(n.dest, src)
}

// fieldsByTraversal fills a list of value interfaces. It will also return an
func fieldsByTraversal(v reflect.Value, traversals [][]int, values []interface{}, unsafe bool) error {
	v = reflect.Indirect(v)
	if v.Kind() != reflect.Struct {
		return errors.New("argument not a struct")
	}

	for i, traversal := range traversals {
		if len(traversal) == 0 {
			if unsafe {
				values[i] = new(interface{})
				continue
			}
			return errors.New(fmt.Sprintf("sqlkit/encoding: missing destination, column index: %d", i))
		}
		f := v
		for _, i2 := range traversal {
			f = reflect.Indirect(f).Field(i2)
		}
		values[i] = &nilSafety{dest: f.Addr().Interface()}
	}
	return nil
}

// Unmarshal will run Decode with the default Decoder configuration.
func Unmarshal(dest interface{}, rows *sql.Rows) error {
	return Encoder{}.Decode(dest, rows)
}

func (e Encoder) Decode(dest interface{}, rows *sql.Rows) error {
	value := reflect.ValueOf(dest)
	if value.Kind() != reflect.Ptr {
		return ErrRequiresPtr
	}
	if value.IsNil() {
		return ErrRequiresPtr
	}
	var slice bool
	base := Deref(value.Type())
	if base.Kind() == reflect.Slice {
		slice = true
	}

	if slice {
		if err := e.scanAll(base, value, rows); err != nil {
			return err
		}
	} else {
		if err := e.scanRow(base, value, dest, rows); err != nil {
			return err
		}
	}
	return rows.Err()
}

func (e Encoder) scanAll(slice reflect.Type, value reflect.Value, rows *sql.Rows) error {
	direct := reflect.Indirect(value)
	isPtr := slice.Elem().Kind() == reflect.Ptr
	base := Deref(slice.Elem())
	scannable := isScannable(base)

	columns, err := rows.Columns()
	if err != nil {
		return err
	}

	m := DefaultMapper
	if e.mapper != nil {
		m = e.mapper
	}

	// if it's a base type make sure it only has 1 column;  if not return an error
	if scannable && len(columns) > 1 {
		return ErrTooManyColumns
	}

	if !scannable {
		var values []interface{}
		fields := m.TraversalsByName(base, columns)
		values = make([]interface{}, len(columns))

		for rows.Next() {
			// create a new struct type (which returns PtrTo) and indirect it
			vp := reflect.New(base)
			v := reflect.Indirect(vp)

			err = fieldsByTraversal(v, fields, values, e.unsafe)
			if err != nil {
				return err
			}

			// scan into the struct field pointers and append to our results
			err = rows.Scan(values...)
			if err != nil {
				return err
			}

			if isPtr {
				direct.Set(reflect.Append(direct, vp))
			} else {
				direct.Set(reflect.Append(direct, v))
			}
		}
	} else {
		for rows.Next() {
			vp := reflect.New(base)
			err = rows.Scan(vp.Interface())
			if err != nil {
				return err
			}
			// append
			if isPtr {
				direct.Set(reflect.Append(direct, vp))
			} else {
				direct.Set(reflect.Append(direct, reflect.Indirect(vp)))
			}
		}
	}
	return nil
}

func (e Encoder) scanRow(base reflect.Type, value reflect.Value, dest interface{}, rows *sql.Rows) error {
	m := DefaultMapper
	if e.mapper != nil {
		m = e.mapper
	}

	// Do this early so we don't have to waste type reflecting or traversing if
	// there isn't anything to scan.
	if !rows.Next() {
		return ErrNoRows
	}

	value = reflect.Indirect(value)
	scannable := isScannable(base)

	columns, err := rows.Columns()
	if err != nil {
		return err
	}

	// If it's scannable, like a scalar type, we can scan directly right here.
	if scannable {
		// Ensure that only one column to scan into.
		if len(columns) > 1 {
			return ErrTooManyColumns
		}
		return rows.Scan(dest)
	}

	fields := m.TraversalsByName(value.Type(), columns)
	values := make([]interface{}, len(columns))
	err = fieldsByTraversal(value, fields, values, e.unsafe)
	if err != nil {
		return err
	}

	// scan into the struct field pointers and append to our results
	err = rows.Scan(values...)
	return err
}
