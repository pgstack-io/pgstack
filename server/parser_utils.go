package main

import (
	pgQuery "github.com/pganalyze/pg_query_go/v6"
)

type ParserUtils struct {
	config *Config
}

func NewParserUtils(config *Config) *ParserUtils {
	return &ParserUtils{config: config}
}

func (utils *ParserUtils) SchemaFunction(functionCall *pgQuery.FuncCall) *QuerySchemaFunction {
	switch len(functionCall.Funcname) {
	case 1:
		return &QuerySchemaFunction{
			Schema:   "",
			Function: functionCall.Funcname[0].GetString_().Sval,
		}
	case 2:
		return &QuerySchemaFunction{
			Schema:   functionCall.Funcname[0].GetString_().Sval,
			Function: functionCall.Funcname[1].GetString_().Sval,
		}
	default:
		Panic(utils.config, "Invalid function call")
		return nil
	}
}

func (utils *ParserUtils) MakeNullNode() *pgQuery.Node {
	return &pgQuery.Node{
		Node: &pgQuery.Node_AConst{
			AConst: &pgQuery.A_Const{
				Isnull: true,
			},
		},
	}
}
