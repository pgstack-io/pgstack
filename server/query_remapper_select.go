package main

import (
	pgQuery "github.com/pganalyze/pg_query_go/v6"
)

type QueryRemapperSelect struct {
	parserSelect    *ParserSelect
	parserFunction  *ParserFunction
	parserAExpr     *ParserAExpr
	parserColumnRef *ParserColumnRef
	config          *Config
}

func NewQueryRemapperSelect(config *Config) *QueryRemapperSelect {
	return &QueryRemapperSelect{
		parserSelect:    NewParserSelect(config),
		parserFunction:  NewParserFunction(config),
		parserAExpr:     NewParserAExpr(config),
		parserColumnRef: NewParserColumnRef(config),
		config:          config,
	}
}

// table.column AS table -> table.column AS table_
//
// DuckDB can't execute GROUP BY table, thinking it's a table name instead of an alias:
// column "name" must appear in the GROUP BY clause or must be part of an aggregate function.
// Either add it to the GROUP BY list, or use "ANY_VALUE(name)" if the exact value of "name" is not important.
//
// So we rename the alias to table_ and use it in GROUP BY and ORDER BY clauses.
func (remapper *QueryRemapperSelect) RemapTargetName(targetNode *pgQuery.Node) (*pgQuery.Node, map[string]string) {
	remappedColumnRefs := make(map[string]string)

	columnRef := remapper.parserColumnRef.ColumnRefFromTargetNode(targetNode)
	if columnRef != nil {
		fieldNames := remapper.parserColumnRef.FieldNames(columnRef)
		targetName := remapper.parserSelect.TargetName(targetNode)

		if len(fieldNames) == 2 && fieldNames[0] == targetName {
			newTargetName := targetName + "_"
			remapper.parserSelect.SetTargetName(targetNode, newTargetName)
			remappedColumnRefs[targetName] = newTargetName
		}
	}

	return targetNode, remappedColumnRefs
}

// SELECT ... -> SELECT ... AS ...
func (remapper *QueryRemapperSelect) SetTargetNameIfEmpty(targetNode *pgQuery.Node) *pgQuery.Node {
	aExpr := remapper.parserAExpr.AExprFromTargetNode(targetNode)
	if aExpr != nil {
		operatorName := remapper.parserAExpr.OperatorName(aExpr)

		// [column]->'value' or [column]->>'value' -> column_value
		if operatorName == "->" || operatorName == "->>" {
			columnName := remapper.parserAExpr.LeftColumnRefName(aExpr)
			valueName := remapper.parserAExpr.RightAConstValue(aExpr)

			if columnName != "" && valueName != "" {
				remapper.parserSelect.SetTargetNameIfEmpty(targetNode, columnName+"_"+valueName)
				return targetNode
			}
		}
	}

	functionCall := remapper.parserFunction.FunctionCall(targetNode)
	if functionCall != nil {
		schemaFunction := remapper.parserFunction.SchemaFunction(functionCall)
		functionName := schemaFunction.Function

		// FUNCTION(...) -> FUNCTION(...) AS FUNCTION
		remapper.parserSelect.SetTargetNameIfEmpty(targetNode, functionName)
		return targetNode
	}

	indirectionName := remapper.parserFunction.IndirectionName(targetNode)
	if indirectionName != "" {
		// (FUNCTION()).n -> (FUNCTION()).n AS n
		remapper.parserSelect.SetTargetNameIfEmpty(targetNode, indirectionName)
		return targetNode
	}

	return targetNode
}
