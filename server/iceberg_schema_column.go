package main

import (
	"fmt"
)

type IcebergSchemaColumn struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	Position int    `json:"position"`
	List     bool   `json:"list"`
	Required bool   `json:"required"`
}

func (column IcebergSchemaColumn) ToSql() string {
	sql := fmt.Sprintf(`"%s" %s`, column.Name, column.Type)

	if column.List {
		sql += "[]"
	}

	if column.Required {
		sql += " NOT NULL"
	}

	return sql
}
