package main

import (
	"fmt"
)

type IcebergSchemaTable struct {
	Schema string
	Table  string
}

func (schemaTable IcebergSchemaTable) String() string {
	return fmt.Sprintf(`"%s"."%s"`, schemaTable.Schema, schemaTable.Table)
}
