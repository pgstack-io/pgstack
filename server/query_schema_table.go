package main

import (
	"strings"
)

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

type QuerySchemaTable struct {
	Schema string
	Table  string
	Alias  string
}

func NewQuerySchemaTableFromString(schemaTable string) QuerySchemaTable {
	parts := strings.Split(schemaTable, ".")

	qSchemaTable := QuerySchemaTable{
		Table: parts[len(parts)-1],
	}
	if len(parts) > 1 {
		qSchemaTable.Schema = parts[0]
	}

	if !StringContainsUpper(qSchemaTable.Schema) {
		qSchemaTable.Schema = strings.ReplaceAll(qSchemaTable.Schema, `"`, "")
	}
	if !StringContainsUpper(qSchemaTable.Table) {
		qSchemaTable.Table = strings.ReplaceAll(qSchemaTable.Table, `"`, "")
	}

	return qSchemaTable
}

func (qSchemaTable QuerySchemaTable) ToIcebergSchemaTable() IcebergSchemaTable {
	if qSchemaTable.Schema == "" {
		return IcebergSchemaTable{
			Schema: PG_SCHEMA_PUBLIC,
			Table:  qSchemaTable.Table,
		}
	}

	return IcebergSchemaTable{
		Schema: qSchemaTable.Schema,
		Table:  qSchemaTable.Table,
	}
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

type QuerySchemaFunction struct {
	Schema   string
	Function string
}
