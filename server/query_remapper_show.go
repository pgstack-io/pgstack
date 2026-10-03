package main

import (
	pgQuery "github.com/pganalyze/pg_query_go/v6"
)

type QueryRemapperShow struct {
	config     *Config
	parserShow *ParserShow
}

func NewQueryRemapperShow(config *Config) *QueryRemapperShow {
	return &QueryRemapperShow{
		config:     config,
		parserShow: NewParserShow(config),
	}
}

func (remapper *QueryRemapperShow) RemapShowStatement(stmt *pgQuery.RawStmt) *pgQuery.RawStmt {
	parser := remapper.parserShow
	variableName := parser.VariableName(stmt)

	// SHOW var -> SELECT value AS var FROM duckdb_settings() WHERE LOWER(name) = 'var';
	newStmt := parser.MakeSelectFromDuckdbSettings(variableName)

	// SELECT value AS search_path -> SELECT CONCAT('"$user", ', value) AS search_path
	if variableName == PG_VAR_SEARCH_PATH {
		parser.SetTargetListForSearchPath(newStmt)
	}

	return newStmt
}
