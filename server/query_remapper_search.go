package main

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	pgQuery "github.com/pganalyze/pg_query_go/v6"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

const (
	VECTOR_SEARCH_OVERSAMPLE   = 10
	HYBRID_RRF_CONSTANT        = 60
	PGSTACK_CHUNK_INDEX_COLUMN = "__pgstack_chunk_index"
	PGSTACK_SOURCE_KEY_COLUMN  = "__pgstack_source_key"
	PGSTACK_VECTOR_RANK_COLUMN = "__pgstack_rank"
)

type serverSearchTable struct {
	Name         string   `json:"name"`
	IndexColumns []string `json:"indexColumns"`
	StoreColumns []string `json:"storeColumns"`
	KeywordOnly  bool     `json:"keywordOnly,omitempty"`
}

type vectorQueryOperand struct {
	key string
	sql string
}

type SearchParameterSpec struct {
	Dimensions      int
	RequireNonZero  bool
	LiteralText     *string
	SourceParameter int
	TextOnly        bool
	RowCountTable   string
}

func (remapper *QueryRemapper) remapSearchRank(selectStatement *pgQuery.SelectStmt, vectorParameters map[int]SearchParameterSpec) error {
	hasRank := false
	visitSearchMessages(selectStatement, func(message protoreflect.Message) {
		if call, ok := message.Interface().(*pgQuery.FuncCall); ok && isSearchRankFunction(call) {
			hasRank = true
		}
	})
	if !hasRank {
		return nil
	}
	if len(selectStatement.FromClause) != 1 || len(selectStatement.SortClause) != 1 {
		return fmt.Errorf("search rank requires one Search table and one ORDER BY rank function")
	}
	rangeVar := selectStatement.FromClause[0].GetRangeVar()
	if rangeVar == nil || rangeVar.Schemaname != PG_SCHEMA_SEARCH || len(rangeVar.Alias.GetColnames()) != 0 {
		return fmt.Errorf("search rank requires a single Search table without column aliases")
	}
	if len(selectStatement.GroupClause) != 0 || selectStatement.HavingClause != nil || len(selectStatement.DistinctClause) != 0 || len(selectStatement.WindowClause) != 0 {
		return fmt.Errorf("search rank does not support grouping, DISTINCT, or window clauses")
	}
	sortBy := selectStatement.SortClause[0].GetSortBy()
	if sortBy == nil || sortBy.Node == nil {
		return nil
	}
	functionCall := sortBy.Node.GetFuncCall()
	if !isSearchRankFunction(functionCall) {
		return fmt.Errorf("search rank must be the ORDER BY expression")
	}
	if functionCall.AggDistinct || functionCall.AggStar || len(functionCall.AggOrder) != 0 || functionCall.AggFilter != nil || functionCall.Over != nil || functionCall.AggWithinGroup {
		return fmt.Errorf("search rank functions do not support aggregate or window syntax")
	}
	if sortBy.SortbyDir != pgQuery.SortByDir_SORTBY_DEFAULT && sortBy.SortbyDir != pgQuery.SortByDir_SORTBY_ASC {
		return fmt.Errorf("search rank queries must use ascending order")
	}
	limit := int32(10)
	if selectStatement.LimitCount != nil {
		if selectStatement.LimitCount.GetAConst() == nil || selectStatement.LimitCount.GetAConst().GetIval() == nil {
			return fmt.Errorf("search rank LIMIT must be an integer")
		}
		limit = selectStatement.LimitCount.GetAConst().GetIval().Ival
	}
	if limit <= 0 {
		return fmt.Errorf("search rank LIMIT must be positive")
	}
	if selectStatement.LimitCount == nil {
		selectStatement.LimitCount = pgQuery.MakeAConstIntNode(int64(limit), 0)
		selectStatement.LimitOption = pgQuery.LimitOption_LIMIT_OPTION_COUNT
	}

	offset := int32(0)
	if selectStatement.LimitOffset != nil {
		constant := selectStatement.LimitOffset.GetAConst()
		if constant == nil || constant.GetIval() == nil || constant.GetIval().Ival < 0 {
			return fmt.Errorf("search rank OFFSET must be a non-negative integer")
		}
		offset = constant.GetIval().Ival
	}
	requiredDocuments := int64(limit) + int64(offset)

	embeddingName := "__pgstack_embedding"
	var configuredTables []serverSearchTable
	if err := json.Unmarshal([]byte(remapper.config.SearchTablesJSON), &configuredTables); err != nil {
		return fmt.Errorf("invalid Search server configuration: %w", err)
	}
	var configuredTable *serverSearchTable
	for _, table := range configuredTables {
		if strings.ReplaceAll(table.Name, ".", "_") != rangeVar.Relname {
			continue
		}
		configuredTable = &table
		break
	}
	if configuredTable == nil || len(configuredTable.IndexColumns) == 0 {
		return fmt.Errorf("unknown or unindexed Search table: search.%s", rangeVar.Relname)
	}

	rankFunction := functionCall.Funcname[0].GetString_().Sval
	if configuredTable.KeywordOnly && rankFunction != "keyword_rank" {
		return fmt.Errorf("%s requires semantic indexing; restart the local image with OPENAI_API_KEY", rankFunction)
	}
	dimensions := OPENAI_EMBEDDING_DIMENSIONS
	if rankFunction == "keyword_rank" {
		dimensions = 0
	}
	nextParameterNumber := maxVectorParameterNumber(selectStatement) + 1
	queryOperand, err := parseSearchRankArgument(functionCall, dimensions, vectorParameters, &nextParameterNumber)
	if err != nil {
		return err
	}
	distanceExpression := fmt.Sprintf(
		"%s(%s, %s)",
		"array_cosine_distance",
		quoteVectorIdentifier(embeddingName),
		queryOperand.sql,
	)
	replaceSearchRankTargets(selectStatement, queryOperand.key, rankFunction)
	// Keep search text validation/binding separate from ordinary SQL parameters.
	validationCopy := proto.Clone(selectStatement).(*pgQuery.SelectStmt)
	validationCopy.SortClause = nil
	var validationError error
	visitSearchMessages(validationCopy, func(message protoreflect.Message) {
		if call, ok := message.Interface().(*pgQuery.FuncCall); ok && isSearchRankFunction(call) {
			validationError = fmt.Errorf("selected rank functions must match the ORDER BY query")
		}
		if parameter, ok := message.Interface().(*pgQuery.ParamRef); ok && queryOperand.key == "parameter:"+strconv.Itoa(int(parameter.Number)) {
			validationError = fmt.Errorf("search rank parameter $%d cannot also be used outside the rank function; use a separate parameter", parameter.Number)
		}
	})
	if validationError != nil {
		return validationError
	}

	path := fmt.Sprintf("s3://%s/%s/%s.lance", remapper.config.AwsS3Bucket, strings.Trim(remapper.config.SearchBasePath, "/"), rangeVar.Relname)
	alias := rangeVar.Relname
	if rangeVar.Alias != nil {
		alias = rangeVar.Alias.Aliasname
	}
	projectedColumns := make([]string, 0, len(configuredTable.StoreColumns)+1)
	for _, column := range configuredTable.StoreColumns {
		projectedColumns = append(projectedColumns, quoteVectorIdentifier(column))
	}
	publicColumns := strings.Join(projectedColumns, ", ")
	projectedColumns = append(projectedColumns, quoteVectorIdentifier(PGSTACK_SOURCE_KEY_COLUMN))
	searchLimit := requiredDocuments * VECTOR_SEARCH_OVERSAMPLE
	projected := strings.Join(projectedColumns, ", ")
	projectedWithDistance := projected + ", " + quoteVectorIdentifier("__pgstack_distance")
	physicalTable := "search_store.main." + quoteVectorIdentifier(rangeVar.Relname)
	// Apply filters directly to each raw retrieval before chunk deduplication.
	// Conservative prefilter eligibility avoids rejecting valid DuckDB expressions
	// that Lance cannot evaluate. Those still retain filtered exhaustive fallbacks.
	prefilter := isSearchPrefilter(selectStatement.WhereClause)
	vectorSource := func(k string) string {
		return fmt.Sprintf("SELECT %s, %s AS __pgstack_distance FROM lance_vector_search('%s', '%s', %s, k => %s, prefilter => %t)",
			projected, distanceExpression, strings.ReplaceAll(path, "'", "''"), embeddingName, queryOperand.sql, k, prefilter)
	}
	textOperand := queryOperand.sql
	if rankFunction == "hybrid_rank" {
		textParameter := nextParameterNumber
		nextParameterNumber++
		textSpec := SearchParameterSpec{TextOnly: true}
		if parameter := functionCall.Args[0].GetParamRef(); parameter != nil {
			textSpec.SourceParameter = int(parameter.Number)
		} else {
			text := functionCall.Args[0].GetAConst().GetSval().Sval
			textSpec.LiteralText = &text
		}
		vectorParameters[textParameter] = textSpec
		textOperand = searchOperandSQL(textParameter, 0)
	}
	keywordSource := func(k string) string {
		return fmt.Sprintf("SELECT %s, -_score AS __pgstack_distance FROM lance_fts('%s', '__pgstack_text', %s, k => %s, prefilter => %t)",
			projected, strings.ReplaceAll(path, "'", "''"), textOperand, k, prefilter)
	}
	if rankFunction == "keyword_rank" && (selectStatement.WhereClause == nil || prefilter) {
		// Only the first chunk has searchable text, so FTS already returns unique
		// documents. With no postfilter, LIMIT + OFFSET is the entire retrieval budget.
		source, err := filterSearchSource(keywordSource(strconv.FormatInt(requiredDocuments, 10)), selectStatement.WhereClause, alias)
		if err != nil {
			return err
		}
		template := fmt.Sprintf("SELECT %s FROM (%s) AS %s ORDER BY __pgstack_distance, __pgstack_source_key",
			projectedWithDistance, source, quoteVectorIdentifier(alias))
		return remapSearchSource(selectStatement, template, configuredTable.StoreColumns, alias)
	}
	vectorFallback := fmt.Sprintf("SELECT %s, %s AS __pgstack_distance FROM %s WHERE %s IS NOT NULL",
		projected, distanceExpression, physicalTable, quoteVectorIdentifier(embeddingName))
	allChunks := ""
	if rankFunction != "semantic_rank" {
		vectorParameters[nextParameterNumber] = SearchParameterSpec{RowCountTable: physicalTable}
		allChunks = fmt.Sprintf("CAST($%d AS BIGINT)", nextParameterNumber)
	}
	filteredDocuments := func(source string) (string, error) {
		filtered, err := filterSearchSource(source, selectStatement.WhereClause, alias)
		if err != nil {
			return "", err
		}
		return buildVectorDocumentsQuery(filtered, projectedWithDistance, alias), nil
	}
	buildDocuments := func(fallback bool) (string, error) {
		k := strconv.FormatInt(searchLimit, 10)
		if fallback {
			k = allChunks
		}
		if rankFunction == "keyword_rank" {
			return filteredDocuments(keywordSource(k))
		}
		vectorSQL := vectorFallback
		if !fallback {
			vectorSQL = vectorSource(k)
		}
		vectorDocuments, err := filteredDocuments(vectorSQL)
		if err != nil || rankFunction == "semantic_rank" {
			return vectorDocuments, err
		}
		keywordDocuments, err := filteredDocuments(keywordSource(k))
		if err != nil {
			return "", err
		}
		return buildHybridDocumentsQuery(vectorDocuments, keywordDocuments, projected), nil
	}
	candidateDocuments, err := buildDocuments(false)
	if err != nil {
		return err
	}
	fallbackDocuments, err := buildDocuments(true)
	if err != nil {
		return err
	}
	template := fmt.Sprintf(
		`SELECT * FROM (
			WITH __pgstack_candidate_docs AS MATERIALIZED (%s),
			__pgstack_fallback_docs AS (%s),
			__pgstack_search_docs AS (
				SELECT %s FROM __pgstack_candidate_docs
				WHERE (SELECT count(*) FROM __pgstack_candidate_docs) >= %d
				UNION ALL
				SELECT %s FROM __pgstack_fallback_docs
				WHERE (SELECT count(*) FROM __pgstack_candidate_docs) < %d
			)
			SELECT %s FROM __pgstack_search_docs
		) AS %s ORDER BY %s, %s`,
		candidateDocuments,
		fallbackDocuments,
		projectedWithDistance,
		requiredDocuments,
		projectedWithDistance,
		requiredDocuments,
		publicColumns+", "+quoteVectorIdentifier("__pgstack_distance")+", "+quoteVectorIdentifier(PGSTACK_SOURCE_KEY_COLUMN),
		quoteVectorIdentifier(alias),
		quoteVectorIdentifier("__pgstack_distance"),
		quoteVectorIdentifier(PGSTACK_SOURCE_KEY_COLUMN),
	)
	return remapSearchSource(selectStatement, template, configuredTable.StoreColumns, alias)
}

func remapSearchSource(selectStatement *pgQuery.SelectStmt, template string, columns []string, alias string) error {
	parsed, err := pgQuery.Parse(template)
	if err != nil {
		return fmt.Errorf("failed to build search: %w", err)
	}
	templateSelect := parsed.Stmts[0].Stmt.GetSelectStmt()
	expandSearchStars(selectStatement, columns, alias)
	selectStatement.FromClause = templateSelect.FromClause
	selectStatement.SortClause = templateSelect.SortClause
	selectStatement.WhereClause = nil
	return nil
}

func buildVectorDocumentsQuery(source string, projectedColumns string, alias string) string {
	return fmt.Sprintf(
		`SELECT %s FROM (
			SELECT %s, row_number() OVER (PARTITION BY %s ORDER BY %s) AS %s
			FROM (%s) AS __pgstack_chunks
		) AS %s WHERE %s = 1`,
		projectedColumns,
		projectedColumns,
		quoteVectorIdentifier(PGSTACK_SOURCE_KEY_COLUMN),
		quoteVectorIdentifier("__pgstack_distance"),
		quoteVectorIdentifier(PGSTACK_VECTOR_RANK_COLUMN),
		source,
		quoteVectorIdentifier(alias),
		quoteVectorIdentifier(PGSTACK_VECTOR_RANK_COLUMN),
	)
}

// Both inputs contain one row per source document. Rank each retrieval after
// deduplication so extra vector chunks cannot boost a document's fusion score.
func buildHybridDocumentsQuery(vectorDocuments, keywordDocuments, columns string) string {
	return fmt.Sprintf(`WITH __pgstack_vector_docs AS (%s), __pgstack_keyword_docs AS (%s),
		__pgstack_signals AS (
			SELECT %s, -1.0 / (%d + rank() OVER (ORDER BY __pgstack_distance)) AS __pgstack_distance FROM __pgstack_vector_docs
			UNION ALL
			SELECT %s, -1.0 / (%d + rank() OVER (ORDER BY __pgstack_distance)) AS __pgstack_distance FROM __pgstack_keyword_docs
		)
		SELECT %s, sum(__pgstack_distance) AS __pgstack_distance FROM __pgstack_signals GROUP BY %s`,
		vectorDocuments, keywordDocuments, columns, HYBRID_RRF_CONSTANT, columns, HYBRID_RRF_CONSTANT, columns, columns)
}

func filterSearchSource(source string, filter *pgQuery.Node, alias string) (string, error) {
	parsed, err := pgQuery.Parse(source)
	if err != nil {
		return "", fmt.Errorf("failed to build search source: %w", err)
	}
	statement := parsed.Stmts[0].Stmt.GetSelectStmt()
	if function := statement.FromClause[0].GetRangeFunction(); function != nil {
		function.Alias = &pgQuery.Alias{Aliasname: alias}
	} else {
		statement.FromClause[0].GetRangeVar().Alias = &pgQuery.Alias{Aliasname: alias}
	}
	if filter != nil {
		condition := proto.Clone(filter).(*pgQuery.Node)
		if statement.WhereClause == nil {
			statement.WhereClause = condition
		} else {
			statement.WhereClause = &pgQuery.Node{Node: &pgQuery.Node_BoolExpr{BoolExpr: &pgQuery.BoolExpr{
				Boolop: pgQuery.BoolExprType_AND_EXPR, Args: []*pgQuery.Node{statement.WhereClause, condition},
			}}}
		}
	}
	return pgQuery.Deparse(parsed)
}

// DuckDB translates simple column comparisons and conjunctions into Lance
// filters. Other expressions remain valid SQL with best-effort pushdown.
func isSearchPrefilter(filter *pgQuery.Node) bool {
	if filter == nil {
		return false
	}
	if boolean := filter.GetBoolExpr(); boolean != nil {
		if boolean.Boolop != pgQuery.BoolExprType_AND_EXPR || len(boolean.Args) == 0 {
			return false
		}
		for _, argument := range boolean.Args {
			if !isSearchPrefilter(argument) {
				return false
			}
		}
		return true
	}
	if null := filter.GetNullTest(); null != nil {
		return null.Arg.GetColumnRef() != nil && !null.Argisrow
	}
	expression := filter.GetAExpr()
	if expression == nil || expression.Kind != pgQuery.A_Expr_Kind_AEXPR_OP || len(expression.Name) != 1 {
		return false
	}
	switch expression.Name[0].GetString_().GetSval() {
	case "=", "<>", "!=", "<", ">", "<=", ">=":
	default:
		return false
	}
	// Stored columns are VARCHAR. Numeric literals may cast the column, turning
	// a comparison into an expression that cannot be guaranteed to prefilter.
	isValue := func(node *pgQuery.Node) bool {
		return node.GetParamRef() != nil || node.GetAConst().GetSval() != nil
	}
	return expression.Lexpr.GetColumnRef() != nil && isValue(expression.Rexpr) || expression.Rexpr.GetColumnRef() != nil && isValue(expression.Lexpr)
}

func parseSearchRankArgument(functionCall *pgQuery.FuncCall, dimensions int, vectorParameters map[int]SearchParameterSpec, nextParameterNumber *int) (*vectorQueryOperand, error) {
	if len(functionCall.Args) != 1 {
		return nil, fmt.Errorf("search rank functions require exactly one text argument")
	}
	argument := functionCall.Args[0]
	if parameter := argument.GetParamRef(); parameter != nil {
		parameterNumber := int(parameter.Number)
		vectorParameters[parameterNumber] = SearchParameterSpec{Dimensions: dimensions, RequireNonZero: dimensions > 0, TextOnly: dimensions == 0}
		return &vectorQueryOperand{
			key: "parameter:" + strconv.Itoa(parameterNumber),
			sql: searchOperandSQL(parameterNumber, dimensions),
		}, nil
	}
	constant := argument.GetAConst()
	if constant == nil || constant.GetSval() == nil {
		return nil, fmt.Errorf("search rank argument must be a text literal or bind parameter")
	}
	text := constant.GetSval().Sval
	if err := validateEmbeddingInput(text); err != nil {
		return nil, err
	}
	parameterNumber := *nextParameterNumber
	*nextParameterNumber = parameterNumber + 1
	vectorParameters[parameterNumber] = SearchParameterSpec{Dimensions: dimensions, RequireNonZero: dimensions > 0, LiteralText: &text, TextOnly: dimensions == 0}
	return &vectorQueryOperand{
		key: "literal:" + text,
		sql: searchOperandSQL(parameterNumber, dimensions),
	}, nil
}

func searchOperandSQL(parameter, dimensions int) string {
	if dimensions == 0 {
		return fmt.Sprintf("CAST($%d AS VARCHAR)", parameter)
	}
	return fmt.Sprintf("CAST($%d AS REAL[%d])", parameter, dimensions)
}

func isSearchRankFunction(functionCall *pgQuery.FuncCall) bool {
	if functionCall == nil || len(functionCall.Funcname) != 1 || functionCall.Funcname[0].GetString_() == nil {
		return false
	}
	switch functionCall.Funcname[0].GetString_().Sval {
	case "semantic_rank", "hybrid_rank", "keyword_rank":
		return true
	default:
		return false
	}
}

func maxVectorParameterNumber(message proto.Message) int {
	maximum := 0
	visitSearchMessages(message, func(current protoreflect.Message) {
		if parameter, ok := current.Interface().(*pgQuery.ParamRef); ok {
			maximum = max(maximum, int(parameter.Number))
		}
	})
	return maximum
}

func visitSearchMessages(message proto.Message, visitor func(protoreflect.Message)) {
	var visit func(protoreflect.Message)
	visit = func(current protoreflect.Message) {
		visitor(current)
		current.Range(func(field protoreflect.FieldDescriptor, value protoreflect.Value) bool {
			if field.IsList() && field.Kind() == protoreflect.MessageKind {
				list := value.List()
				for i := 0; i < list.Len(); i++ {
					visit(list.Get(i).Message())
				}
			} else if field.Kind() == protoreflect.MessageKind {
				visit(value.Message())
			}
			return true
		})
	}
	visit(message.ProtoReflect())
}

func replaceSearchRankTargets(selectStatement *pgQuery.SelectStmt, operandKey string, rankFunction string) {
	for _, targetNode := range selectStatement.TargetList {
		target := targetNode.GetResTarget()
		if target == nil || target.Val == nil {
			continue
		}
		functionCall := target.Val.GetFuncCall()
		if !isSearchRankFunction(functionCall) || functionCall.Funcname[0].GetString_().Sval != rankFunction || len(functionCall.Args) != 1 {
			continue
		}
		argument := functionCall.Args[0]
		matches := false
		if parameter := argument.GetParamRef(); parameter != nil {
			matches = operandKey == "parameter:"+strconv.Itoa(int(parameter.Number))
		}
		if constant := argument.GetAConst(); constant != nil && constant.GetSval() != nil {
			matches = operandKey == "literal:"+constant.GetSval().Sval
		}
		if matches {
			if target.Name == "" {
				target.Name = functionCall.Funcname[0].GetString_().Sval
			}
			target.Val = pgQuery.MakeColumnRefNode([]*pgQuery.Node{pgQuery.MakeStrNode("__pgstack_distance")}, 0)
		}
	}
}

// The ranking column is available to ORDER BY but must not leak through SELECT *.
func expandSearchStars(statement *pgQuery.SelectStmt, columns []string, alias string) {
	var targets []*pgQuery.Node
	for _, node := range statement.TargetList {
		target := node.GetResTarget()
		column := target.GetVal().GetColumnRef()
		if column == nil || len(column.Fields) == 0 || column.Fields[len(column.Fields)-1].GetAStar() == nil ||
			(len(column.Fields) > 1 && (len(column.Fields) != 2 || column.Fields[0].GetString_().GetSval() != alias)) {
			targets = append(targets, node)
			continue
		}
		for _, name := range columns {
			targets = append(targets, &pgQuery.Node{Node: &pgQuery.Node_ResTarget{ResTarget: &pgQuery.ResTarget{
				Val: pgQuery.MakeColumnRefNode([]*pgQuery.Node{pgQuery.MakeStrNode(alias), pgQuery.MakeStrNode(name)}, 0),
			}}})
		}
	}
	statement.TargetList = targets
}

func quoteVectorIdentifier(value string) string {
	return `"` + strings.ReplaceAll(value, `"`, `""`) + `"`
}
