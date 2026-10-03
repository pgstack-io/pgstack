package main

import (
	pgQuery "github.com/pganalyze/pg_query_go/v6"
)

type ParserSelect struct {
	config *Config
	utils  *ParserUtils
}

func NewParserSelect(config *Config) *ParserSelect {
	return &ParserSelect{config: config, utils: NewParserUtils(config)}
}

func (parser *ParserSelect) SetTargetNameIfEmpty(targetNode *pgQuery.Node, name string) {
	target := targetNode.GetResTarget()

	if target.Name == "" {
		target.Name = name
	}
}

func (parser *ParserSelect) SetTargetName(targetNode *pgQuery.Node, name string) {
	target := targetNode.GetResTarget()
	target.Name = name
}

func (parser *ParserSelect) TargetName(targetNode *pgQuery.Node) string {
	return targetNode.GetResTarget().Name
}
