package watchdog

import (
	"database/sql"
	"fmt"
	"reflect"
)

type fakeRow struct {
	values []any
}

func (r fakeRow) Scan(dest ...any) error {
	if len(dest) != len(r.values) {
		return fmt.Errorf("scan destination count %d does not match value count %d", len(dest), len(r.values))
	}
	for i, value := range r.values {
		target := reflect.ValueOf(dest[i])
		if target.Kind() != reflect.Pointer || target.IsNil() {
			return fmt.Errorf("scan destination %d is not a pointer", i)
		}
		if value == nil {
			switch typed := dest[i].(type) {
			case *sql.NullString:
				*typed = sql.NullString{}
			case *sql.NullInt64:
				*typed = sql.NullInt64{}
			case *sql.NullTime:
				*typed = sql.NullTime{}
			default:
				target.Elem().Set(reflect.Zero(target.Elem().Type()))
			}
			continue
		}
		source := reflect.ValueOf(value)
		if !source.Type().AssignableTo(target.Elem().Type()) {
			if source.Type().ConvertibleTo(target.Elem().Type()) {
				source = source.Convert(target.Elem().Type())
			} else {
				return fmt.Errorf("scan value %d type %T cannot assign to %T", i, value, dest[i])
			}
		}
		target.Elem().Set(source)
	}
	return nil
}
