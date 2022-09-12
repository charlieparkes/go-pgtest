package pgtest

import (
	"context"
	"fmt"
	"strings"

	"github.com/charlieparkes/go-structs"
	"github.com/iancoleman/strcase"
)

type model interface {
	TableName() string
}

// ValidateModels checks that the given structs are valid representations of a database tables.
//     1. Validate table name (using gorm-style TableName() or name-to-snake)
//     2. Validate columns exist.
func ValidateModels(ctx context.Context, f *Postgres, databaseName string, i ...interface{}) error {
	for _, iface := range i {
		if err := ValidateModel(ctx, f, databaseName, iface); err != nil {
			return err
		}
	}
	return nil
}

// ValidateModel checks that a given struct is a valid representation of a database table.
//     1. Validate table name (using gorm-style TableName() or name-to-snake)
//     2. Validate columns exist.
func ValidateModel(ctx context.Context, f *Postgres, databaseName string, i interface{}) error {
	var tableName string
	switch v := i.(type) {
	case model:
		tableName = strings.Trim(v.TableName(), "\"")
	default:
		tableName = strcase.ToSnake(structs.Name(v))
	}

	var schemaName string = "public"
	if s, t, found := strings.Cut(tableName, "."); found {
		schemaName = strings.Trim(s, "\"")
		tableName = strings.Trim(t, "\"")
	}

	exists, err := f.TableExists(ctx, databaseName, schemaName, tableName)
	if err != nil {
		return err
	}
	if !exists {
		return fmt.Errorf("table %v.%v does not exist", schemaName, tableName)
	}

	fieldNames := Columns(i)
	columnNames, err := f.TableColumns(ctx, databaseName, schemaName, tableName)
	if err != nil {
		return err
	}

	for _, f := range fieldNames {
		found := false
		for _, c := range columnNames {
			if f == c {
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("struct %v contains field %v which does not exist in table: %v.%v{%v}", structs.Name(i), f, schemaName, tableName, columnNames)
		}
	}
	return nil
}

// Given a struct, return the expected column names.
func Columns(i interface{}) []string {
	sf := structs.Fields(i)
	fields := []string{}
	for _, f := range sf {
		if tag := f.Tag.Get("db"); tag == "-" {
			continue
		} else if tag == "" {
			fields = append(fields, strcase.ToSnake(f.Name))
		} else {
			fields = append(fields, tag)
		}
	}
	return fields
}
