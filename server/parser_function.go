package main

import (
	"strings"

	pgQuery "github.com/pganalyze/pg_query_go/v6"
)

type ParserFunction struct {
	config *Config
	utils  *ParserUtils
}

func NewParserFunction(config *Config) *ParserFunction {
	return &ParserFunction{config: config, utils: NewParserUtils(config)}
}

func (parser *ParserFunction) FunctionCall(targetNode *pgQuery.Node) *pgQuery.FuncCall {
	return targetNode.GetResTarget().Val.GetFuncCall()
}

func (parser *ParserFunction) FirstArgumentToString(functionCall *pgQuery.FuncCall) string {
	if len(functionCall.Args) < 1 {
		return ""
	}
	return functionCall.Args[0].GetAConst().GetSval().Sval
}

// n from (FUNCTION()).n
func (parser *ParserFunction) IndirectionName(targetNode *pgQuery.Node) string {
	indirection := targetNode.GetResTarget().Val.GetAIndirection()
	if indirection != nil && len(indirection.Indirection) == 1 && indirection.Indirection[0].GetString_() != nil {
		return indirection.Indirection[0].GetString_().Sval
	}

	return ""
}

func (parser *ParserFunction) NestedFunctionCalls(functionCall *pgQuery.FuncCall) []*pgQuery.FuncCall {
	nestedFunctionCalls := []*pgQuery.FuncCall{}

	for _, arg := range functionCall.Args {
		nestedFunctionCalls = append(nestedFunctionCalls, arg.GetFuncCall())
	}

	return nestedFunctionCalls
}

func (parser *ParserFunction) SchemaFunction(functionCall *pgQuery.FuncCall) *QuerySchemaFunction {
	return parser.utils.SchemaFunction(functionCall)
}

// pg_catalog.func() -> main.func()
func (parser *ParserFunction) RemapSchemaToMain(functionCall *pgQuery.FuncCall) {
	switch len(functionCall.Funcname) {
	case 1:
		functionCall.Funcname = append([]*pgQuery.Node{pgQuery.MakeStrNode(DUCKDB_SCHEMA_MAIN)}, functionCall.Funcname...)
	case 2:
		functionCall.Funcname[0] = pgQuery.MakeStrNode(DUCKDB_SCHEMA_MAIN)
	default:
		Panic(parser.config, "Unexpected function name length: "+IntToString(len(functionCall.Funcname)))
	}
}

// jsonb_agg(...) -> to_json(array_agg(...))
//
// DuckDB does not support jsonb_agg. We can't remap and fully support all features with just a macro:
// Function "jsonb_agg" is a Macro Function. "DISTINCT", "FILTER", and "ORDER BY" are only applicable to aggregate functions.
func (parser *ParserFunction) RemapJsonbAgg(functionCall *pgQuery.FuncCall) {
	originalArgs := functionCall.Args
	originalAggOrder := functionCall.AggOrder
	originalAggFilter := functionCall.AggFilter
	originalAggWithinGroup := functionCall.AggWithinGroup
	originalAggStar := functionCall.AggStar
	originalAggDistinct := functionCall.AggDistinct

	nestedFunctionCallNode := pgQuery.MakeFuncCallNode(
		[]*pgQuery.Node{pgQuery.MakeStrNode("array_agg")},
		originalArgs,
		0,
	)
	nestedFunctionCall := nestedFunctionCallNode.GetFuncCall()
	nestedFunctionCall.AggOrder = originalAggOrder
	nestedFunctionCall.AggFilter = originalAggFilter
	nestedFunctionCall.AggWithinGroup = originalAggWithinGroup
	nestedFunctionCall.AggStar = originalAggStar
	nestedFunctionCall.AggDistinct = originalAggDistinct

	functionCall.Funcname = []*pgQuery.Node{pgQuery.MakeStrNode("to_json")}
	functionCall.Args = []*pgQuery.Node{nestedFunctionCallNode}
	functionCall.AggOrder = nil
	functionCall.AggFilter = nil
	functionCall.AggWithinGroup = false
	functionCall.AggStar = false
	functionCall.AggDistinct = false
}

// format('%s %1$s', str) -> printf('%1$s %1$s', str)
func (parser *ParserFunction) RemapFormatToPrintf(functionCall *pgQuery.FuncCall) {
	format := parser.FirstArgumentToString(functionCall)
	for i := range functionCall.Args[1:] {
		format = strings.Replace(format, "%s", "%"+IntToString(i+1)+"$s", 1)
	}

	functionCall.Funcname = []*pgQuery.Node{pgQuery.MakeStrNode("printf")}
	functionCall.Args[0] = pgQuery.MakeAConstStrNode(format, 0)
}

// encode(sha256(...), 'hex') -> sha256(...)
func (parser *ParserFunction) RemoveEncode(functionCall *pgQuery.FuncCall) {
	if len(functionCall.Args) != 2 {
		return
	}

	firstArg := functionCall.Args[0]
	nestedFunctionCall := firstArg.GetFuncCall()
	schemaFunction := parser.utils.SchemaFunction(nestedFunctionCall)
	if schemaFunction.Function != "sha256" {
		return
	}

	secondArg := functionCall.Args[1]
	var format string
	if secondArg.GetAConst() != nil {
		format = secondArg.GetAConst().GetSval().Sval
	} else if secondArg.GetTypeCast() != nil {
		format = secondArg.GetTypeCast().Arg.GetAConst().GetSval().Sval
	}
	if format != "hex" {
		return
	}

	functionCall.Funcname = nestedFunctionCall.Funcname
	functionCall.Args = nestedFunctionCall.Args
}
