package main

import (
	"strings"

	pgQuery "github.com/pganalyze/pg_query_go/v6"
)

type ParserAExpr struct {
	config         *Config
	utils          *ParserUtils
	parserTypeCast *ParserTypeCast
}

func NewParserAExpr(config *Config) *ParserAExpr {
	return &ParserAExpr{
		config:         config,
		utils:          NewParserUtils(config),
		parserTypeCast: NewParserTypeCast(config),
	}
}

func (parser *ParserAExpr) AExprFromTargetNode(targetNode *pgQuery.Node) *pgQuery.A_Expr {
	return targetNode.GetResTarget().Val.GetAExpr()
}

func (parser *ParserAExpr) AExpr(node *pgQuery.Node) *pgQuery.A_Expr {
	return node.GetAExpr()
}

// [column]->>'value' -> json_extract_string([column], 'value')
//
// DuckDB bug with wrong precedence of ->> operator: it has lower precedence than AND, so make it a function call explicitly:
// SELECT * FROM postgres.test_table WHERE id = 1 ((   AND json_column   ))->>'key' IN ('value');
// ^ Could not convert string '...' to BOOL when casting from source column json_column
func (parser *ParserAExpr) RemappedJsonExtractString(node *pgQuery.Node) *pgQuery.Node {
	aExpr := parser.AExpr(node)
	if aExpr == nil || parser.OperatorName(aExpr) != "->>" {
		return node
	}

	return pgQuery.MakeFuncCallNode(
		[]*pgQuery.Node{pgQuery.MakeStrNode("json_extract_string")},
		[]*pgQuery.Node{aExpr.Lexpr, aExpr.Rexpr},
		0,
	)
}

// [column]->'value' -> json_extract([column], 'value')
//
// DuckDB bug with wrong precedence of -> operator: it has lower precedence than IS NOT NULL, so make it a function call explicitly:
// SELECT string_agg(jsonb_column->'key') FILTER (WHERE jsonb_column->   ((   'key' IS NOT NULL   ))   ) FROM postgres.test_table;
// ^ Binder Error: No function matches the given name and argument types 'json_extract(VARCHAR, BOOLEAN)'
func (parser *ParserAExpr) RemappedJsonExtract(node *pgQuery.Node) *pgQuery.Node {
	aExpr := parser.AExpr(node)
	if aExpr == nil || parser.OperatorName(aExpr) != "->" {
		return node
	}

	return pgQuery.MakeFuncCallNode(
		[]*pgQuery.Node{pgQuery.MakeStrNode("json_extract")},
		[]*pgQuery.Node{aExpr.Lexpr, aExpr.Rexpr},
		0,
	)
}

// [column] ? 'key' -> json_exists([column], 'key')
func (parser *ParserAExpr) RemappedJsonExists(node *pgQuery.Node) *pgQuery.Node {
	aExpr := parser.AExpr(node)
	if aExpr == nil || parser.OperatorName(aExpr) != "?" {
		return node
	}

	return pgQuery.MakeFuncCallNode(
		[]*pgQuery.Node{pgQuery.MakeStrNode("json_exists")},
		[]*pgQuery.Node{aExpr.Lexpr, aExpr.Rexpr},
		0,
	)
}

func (parser *ParserAExpr) OperatorName(aExpr *pgQuery.A_Expr) string {
	if aExpr.Kind != pgQuery.A_Expr_Kind_AEXPR_OP || len(aExpr.Name) != 1 {
		return ""
	}

	return aExpr.Name[0].GetString_().Sval
}

func (parser *ParserAExpr) LeftColumnRefName(aExpr *pgQuery.A_Expr) string {
	if aExpr.Lexpr == nil || aExpr.Lexpr.GetColumnRef() == nil || len(aExpr.Lexpr.GetColumnRef().Fields) != 1 {
		return ""
	}

	return aExpr.Lexpr.GetColumnRef().Fields[0].GetString_().Sval
}

func (parser *ParserAExpr) RightAConstValue(aExpr *pgQuery.A_Expr) string {
	if aExpr.Rexpr == nil || aExpr.Rexpr.GetAConst() == nil {
		return ""
	}

	return aExpr.Rexpr.GetAConst().GetSval().Sval
}

// = ANY('{information_schema, ...}') -> IN ('information_schema', ...)
//
// DuckDB error: UNNEST() for correlated expressions is not supported yet
func (parser *ParserAExpr) ConvertedRightAnyStringConstantToIn(node *pgQuery.Node) *pgQuery.Node {
	aExpr := parser.AExpr(node)

	if aExpr.Kind != pgQuery.A_Expr_Kind_AEXPR_OP_ANY {
		return node
	}

	if aExpr.Rexpr.GetAConst() == nil {
		return node
	}

	arrayStr := aExpr.Rexpr.GetAConst().GetSval().Sval
	arrayStr = strings.Trim(arrayStr, "{}")
	values := strings.Split(arrayStr, ",")

	items := make([]*pgQuery.Node, len(values))
	for i, value := range values {
		value = strings.Trim(value, " ")
		items[i] = &pgQuery.Node{
			Node: &pgQuery.Node_AConst{
				AConst: &pgQuery.A_Const{
					Val: &pgQuery.A_Const_Sval{
						Sval: &pgQuery.String{
							Sval: value,
						},
					},
				},
			},
		}
	}

	return &pgQuery.Node{
		Node: &pgQuery.Node_AExpr{
			AExpr: &pgQuery.A_Expr{
				Kind:  pgQuery.A_Expr_Kind_AEXPR_IN,
				Name:  []*pgQuery.Node{{Node: &pgQuery.Node_String_{String_: &pgQuery.String{Sval: "="}}}},
				Lexpr: aExpr.Lexpr,
				Rexpr: &pgQuery.Node{
					Node: &pgQuery.Node_List{
						List: &pgQuery.List{
							Items: items,
						},
					},
				},
				Location: aExpr.Location,
			},
		},
	}
}

// value = ANY($1::text[]) -> list_contains($1::text[], value)
//
// DuckDB cannot UNNEST a parameterized list in an ANY expression because it
// treats it as a correlated expression. list_contains has the same membership
// semantics for this common equality form without requiring UNNEST.
func (parser *ParserAExpr) RemappedAnyTextArrayParameter(node *pgQuery.Node) *pgQuery.Node {
	aExpr := parser.AExpr(node)
	if aExpr == nil || aExpr.Kind != pgQuery.A_Expr_Kind_AEXPR_OP_ANY || len(aExpr.Name) != 1 || aExpr.Name[0].GetString_().Sval != "=" {
		return node
	}

	if aExpr.Rexpr == nil {
		return node
	}
	typeCast := aExpr.Rexpr.GetTypeCast()
	if typeCast == nil || typeCast.Arg.GetParamRef() == nil {
		return node
	}

	typeName := parser.parserTypeCast.TypeName(typeCast)
	if typeName != "text[]" && typeName != "pg_catalog.text[]" {
		return node
	}

	return pgQuery.MakeFuncCallNode(
		[]*pgQuery.Node{pgQuery.MakeStrNode("list_contains")},
		[]*pgQuery.Node{aExpr.Rexpr, aExpr.Lexpr},
		0,
	)
}

// pg_catalog.[operator] -> [operator]
func (parser *ParserAExpr) RemovePgCatalog(node *pgQuery.Node) {
	aExpr := parser.AExpr(node)

	if aExpr == nil || aExpr.Kind != pgQuery.A_Expr_Kind_AEXPR_OP {
		return
	}

	if len(aExpr.Name) == 2 && aExpr.Name[0].GetString_().Sval == PG_SCHEMA_PG_CATALOG {
		aExpr.Name = aExpr.Name[1:] // Remove the first element
	}
}
