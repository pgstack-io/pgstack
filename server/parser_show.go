package main

import (
	pgQuery "github.com/pganalyze/pg_query_go/v6"
)

type ParserShow struct {
	config *Config
}

func NewParserShow(config *Config) *ParserShow {
	return &ParserShow{config: config}
}

func (parser *ParserShow) VariableName(stmt *pgQuery.RawStmt) string {
	return stmt.Stmt.GetVariableShowStmt().Name
}

// SHOW var -> SELECT value AS var FROM duckdb_settings() WHERE LOWER(name) = 'var';
func (parser *ParserShow) MakeSelectFromDuckdbSettings(variableName string) *pgQuery.RawStmt {
	return &pgQuery.RawStmt{
		Stmt: &pgQuery.Node{
			Node: &pgQuery.Node_SelectStmt{
				SelectStmt: &pgQuery.SelectStmt{
					TargetList: []*pgQuery.Node{
						pgQuery.MakeResTargetNodeWithNameAndVal(
							variableName,
							pgQuery.MakeColumnRefNode(
								[]*pgQuery.Node{pgQuery.MakeStrNode("value")},
								0,
							),
							0,
						),
					},
					FromClause: []*pgQuery.Node{
						pgQuery.MakeSimpleRangeFunctionNode(
							[]*pgQuery.Node{
								pgQuery.MakeListNode(
									[]*pgQuery.Node{
										pgQuery.MakeFuncCallNode(
											[]*pgQuery.Node{pgQuery.MakeStrNode("duckdb_settings")},
											nil,
											0,
										),
									},
								),
							},
						),
					},
					WhereClause: pgQuery.MakeAExprNode(
						pgQuery.A_Expr_Kind_AEXPR_OP,
						[]*pgQuery.Node{pgQuery.MakeStrNode("=")},
						pgQuery.MakeFuncCallNode(
							[]*pgQuery.Node{pgQuery.MakeStrNode("lower")},
							[]*pgQuery.Node{
								pgQuery.MakeColumnRefNode(
									[]*pgQuery.Node{pgQuery.MakeStrNode("name")},
									0,
								),
							},
							0,
						),
						pgQuery.MakeAConstStrNode(variableName, 0),
						0,
					),
				},
			},
		},
	}
}

// SELECT value AS search_path -> SELECT CONCAT('"$user", ', value) AS search_path
func (parser *ParserShow) SetTargetListForSearchPath(stmt *pgQuery.RawStmt) {
	stmt.Stmt.GetSelectStmt().TargetList = []*pgQuery.Node{
		pgQuery.MakeResTargetNodeWithNameAndVal(
			PG_VAR_SEARCH_PATH,
			pgQuery.MakeFuncCallNode(
				[]*pgQuery.Node{pgQuery.MakeStrNode("concat")},
				[]*pgQuery.Node{
					pgQuery.MakeAConstStrNode(`"$user", `, 0),
					pgQuery.MakeColumnRefNode(
						[]*pgQuery.Node{pgQuery.MakeStrNode("value")},
						0,
					),
				},
				0,
			),
			0,
		),
	}
}
