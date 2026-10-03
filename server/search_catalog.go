package main

import (
	"encoding/json"
	"fmt"
	"strings"
)

// searchCatalogBootQueries creates empty catalog tables for PostgreSQL schema
// discovery. Query remapping reads the corresponding tables from Lance.
func searchCatalogBootQueries(config *Config) []string {
	var tables []serverSearchTable
	if err := json.Unmarshal([]byte(config.SearchTablesJSON), &tables); err != nil {
		panic(fmt.Sprintf("invalid SEARCH_TABLES_JSON: %v", err))
	}

	queries := []string{"CREATE SCHEMA " + PG_SCHEMA_SEARCH}
	for _, table := range tables {
		columns := make([]string, 0, len(table.StoreColumns))
		for _, column := range table.StoreColumns {
			columns = append(columns, quoteVectorIdentifier(column)+" VARCHAR")
		}

		tableName := strings.ReplaceAll(table.Name, ".", "_")
		queries = append(queries, fmt.Sprintf(
			"CREATE TABLE %s.%s (%s)",
			PG_SCHEMA_SEARCH,
			quoteVectorIdentifier(tableName),
			strings.Join(columns, ", "),
		))
	}
	return queries
}
