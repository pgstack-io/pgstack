package main

import (
	"context"
	"database/sql"
	"encoding/binary"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"
)

const (
	FALLBACK_SQL_QUERY        = "SELECT 1"
	PREPARE_STATEMENT_TIMEOUT = 30 * time.Second
)

type QueryHandler struct {
	Config             *Config
	ServerDuckdbClient *DuckdbClient
	QueryRemapper      *QueryRemapper
	ResponseHandler    *ResponseHandler
	EmbeddingClient    *EmbeddingClient
}

type PreparedStatement struct {
	// Parse
	Name          string
	OriginalQuery string
	Query         string
	Statement     *sql.Stmt
	ParameterOIDs []uint32
	// VectorParameters maps one-based PostgreSQL bind parameter numbers to
	// search text/vector validation and generated search bindings.
	VectorParameters map[int]SearchParameterSpec
	// TextArrayParameters identifies PostgreSQL text[] values that need decoding
	// from PostgreSQL's wire representation before DuckDB can bind them.
	TextArrayParameters map[int]bool

	// Bind
	Bound     bool
	Variables []interface{}
	Portal    string

	// Describe
	Described bool

	// Describe/Execute
	Rows *sql.Rows
}

func NewQueryHandler(config *Config, serverDuckdbClient *DuckdbClient) (*QueryHandler, error) {
	s3Client, err := NewS3Client(config)
	if err != nil {
		return nil, err
	}
	icebergReader := NewIcebergReader(config, s3Client)

	queryHandler := &QueryHandler{
		Config:             config,
		ServerDuckdbClient: serverDuckdbClient,
		QueryRemapper:      NewQueryRemapper(config, icebergReader, serverDuckdbClient),
		ResponseHandler:    NewResponseHandler(config),
		EmbeddingClient:    NewEmbeddingClient(config),
	}

	return queryHandler, nil
}

func (queryHandler *QueryHandler) HandleSimpleQuery(originalQuery string) ([]pgproto3.Message, error) {
	remappedQuery, err := queryHandler.QueryRemapper.ParseAndRemapQuery(originalQuery)
	if err != nil {
		return nil, err
	}
	if len(remappedQuery.Statements) == 0 {
		return []pgproto3.Message{&pgproto3.EmptyQueryResponse{}}, nil
	}
	if len(sortedGeneratedVectorParameterNumbers(remappedQuery.VectorParameters)) > 0 && len(remappedQuery.Statements) != 1 {
		err := fmt.Errorf("search rank is only supported in a single simple-query statement")
		return nil, NewClientError(PG_ERROR_SEVERITY_ERROR, PG_ERROR_CODE_FEATURE_NOT_SUPPORTED, err.Error(), err)
	}
	variables, err := queryHandler.resolveEmbeddedLiteralParameters(context.Background(), remappedQuery.VectorParameters)
	if err != nil {
		return nil, NewClientError(PG_ERROR_SEVERITY_ERROR, PG_ERROR_CODE_INVALID_PARAMETER_VALUE, err.Error(), err)
	}

	var queriesMessages []pgproto3.Message

	for i, queryStatement := range remappedQuery.Statements {
		LogInfo(queryHandler.Config, "Original query:", remappedQuery.OriginalStatements[i])
		LogInfo(queryHandler.Config, "Remapped query:", queryStatement)
		rows, err := queryHandler.ServerDuckdbClient.QueryContext(context.Background(), queryStatement, variables...)
		if err != nil {
			errorMessage := err.Error()
			LogError(queryHandler.Config, "Query execution error:", errorMessage)
			if errorMessage == "Binder Error: UNNEST requires a single list as input" {
				// https://github.com/duckdbClient/duckdb/issues/11693
				LogWarn(queryHandler.Config, "Couldn't handle query via DuckDB:", queryStatement+"\n"+err.Error())
				queriesMsgs, err := queryHandler.HandleSimpleQuery(FALLBACK_SQL_QUERY) // self-recursion
				if err != nil {
					return nil, err
				}
				queriesMessages = append(queriesMessages, queriesMsgs...)
				continue
			} else {
				return nil, err
			}
		}
		defer rows.Close()

		var queryMessages []pgproto3.Message
		descriptionMessages, err := queryHandler.rowsToDescriptionMessages(rows, remappedQuery.OriginalStatements[i])
		if err != nil {
			return nil, err
		}
		queryMessages = append(queryMessages, descriptionMessages...)
		dataMessages, err := queryHandler.rowsToDataMessages(rows, remappedQuery.OriginalStatements[i])
		if err != nil {
			return nil, err
		}
		queryMessages = append(queryMessages, dataMessages...)

		queriesMessages = append(queriesMessages, queryMessages...)
	}

	return queriesMessages, nil
}

func (queryHandler *QueryHandler) HandleParseQuery(message *pgproto3.Parse) ([]pgproto3.Message, *PreparedStatement, error) {
	originalQuery := string(message.Query)
	remappedQuery, err := queryHandler.QueryRemapper.ParseAndRemapQuery(originalQuery)
	if err != nil {
		return nil, nil, err
	}
	if len(remappedQuery.Statements) > 1 {
		err := fmt.Errorf("multiple queries in a single parse message are not supported: %s", originalQuery)
		return nil, nil, NewClientError(
			PG_ERROR_SEVERITY_ERROR,
			PG_ERROR_CODE_SYNTAX_ERROR,
			"cannot insert multiple commands into a prepared statement",
			err,
		)
	}

	preparedStatement := &PreparedStatement{
		Name:                message.Name,
		OriginalQuery:       originalQuery,
		ParameterOIDs:       message.ParameterOIDs,
		VectorParameters:    remappedQuery.VectorParameters,
		TextArrayParameters: remappedQuery.TextArrayParameters,
	}
	if len(remappedQuery.Statements) == 0 {
		return []pgproto3.Message{&pgproto3.ParseComplete{}}, preparedStatement, nil
	}

	query := remappedQuery.Statements[0]
	preparedStatement.Query = query
	statement, err := queryHandler.prepareContextWithTimeout(query)
	preparedStatement.Statement = statement
	if err != nil {
		return nil, nil, err
	}

	return []pgproto3.Message{&pgproto3.ParseComplete{}}, preparedStatement, nil
}

func (queryHandler *QueryHandler) prepareContextWithTimeout(query string) (*sql.Stmt, error) {
	ctx := context.Background()
	timeoutCtx, cancel := context.WithTimeout(ctx, PREPARE_STATEMENT_TIMEOUT)
	defer cancel()

	type result struct {
		statement *sql.Stmt
		err       error
	}
	resultChan := make(chan result, 1)

	go func() {
		statement, err := queryHandler.ServerDuckdbClient.PrepareContext(timeoutCtx, query)
		resultChan <- result{statement, err}
	}()

	select {
	case res := <-resultChan:
		return res.statement, res.err
	case <-timeoutCtx.Done():
		LogWarn(queryHandler.Config, "PrepareContext timed out, recreating DuckDB connection")
		if err := queryHandler.ServerDuckdbClient.RecreateDb(); err != nil {
			return nil, fmt.Errorf("failed to recreate DuckDB after prepare timeout: %w", err)
		}
		queryHandler.QueryRemapper.remapperTable.Reset()
		return queryHandler.ServerDuckdbClient.PrepareContext(ctx, query)
	}
}

func (queryHandler *QueryHandler) HandleBindQuery(message *pgproto3.Bind, preparedStatement *PreparedStatement) ([]pgproto3.Message, *PreparedStatement, error) {
	if message.PreparedStatement != preparedStatement.Name {
		err := fmt.Errorf("prepared statement mismatch, %s instead of %s: %s", message.PreparedStatement, preparedStatement.Name, preparedStatement.OriginalQuery)
		return nil, nil, NewClientError(PG_ERROR_SEVERITY_ERROR, PG_ERROR_CODE_PROTOCOL_VIOLATION, "bind message references an unexpected prepared statement", err)
	}

	var variables []interface{}
	paramFormatCodes := message.ParameterFormatCodes
	embeddingCache := make(map[string][]float32)

	for i, param := range message.Parameters {
		parameterNumber := i + 1
		if param == nil {
			if _, isVector := preparedStatement.VectorParameters[parameterNumber]; isVector {
				err := fmt.Errorf("search rank parameter $%d cannot be NULL", parameterNumber)
				return nil, nil, NewClientError(PG_ERROR_SEVERITY_ERROR, PG_ERROR_CODE_INVALID_PARAMETER_VALUE, err.Error(), err)
			}
			variables = append(variables, nil)
			continue
		}

		textFormat := true
		if len(paramFormatCodes) == 1 {
			textFormat = paramFormatCodes[0] == 0
		} else if len(paramFormatCodes) > 1 {
			textFormat = paramFormatCodes[i] == 0
		}

		if vectorSpec, isVector := preparedStatement.VectorParameters[parameterNumber]; isVector {
			if vectorSpec.LiteralText != nil || vectorSpec.SourceParameter != 0 || vectorSpec.RowCountTable != "" {
				err := fmt.Errorf("bind message supplied a hidden search rank parameter")
				return nil, nil, NewClientError(PG_ERROR_SEVERITY_ERROR, PG_ERROR_CODE_PROTOCOL_VIOLATION, err.Error(), err)
			}
			value, err := queryHandler.resolveSearchParameter(context.Background(), string(param), vectorSpec, embeddingCache)
			if err != nil {
				return nil, nil, NewClientError(PG_ERROR_SEVERITY_ERROR, PG_ERROR_CODE_INVALID_PARAMETER_VALUE, err.Error(), err)
			}
			variables = append(variables, value)
		} else if preparedStatement.TextArrayParameters[parameterNumber] {
			value, err := parseTextArrayBindParameter(param, textFormat)
			if err != nil {
				return nil, nil, NewClientError(PG_ERROR_SEVERITY_ERROR, PG_ERROR_CODE_INVALID_PARAMETER_VALUE, err.Error(), err)
			}
			variables = append(variables, value)
		} else if textFormat {
			variables = append(variables, string(param))
		} else if len(param) == 4 {
			variables = append(variables, int32(binary.BigEndian.Uint32(param)))
		} else if len(param) == 8 {
			variables = append(variables, int64(binary.BigEndian.Uint64(param)))
		} else if len(param) == 16 {
			variables = append(variables, uuid.UUID(param).String())
		} else {
			err := fmt.Errorf("unsupported parameter format: %v (length %d). Original query: %s", param, len(param), preparedStatement.OriginalQuery)
			return nil, nil, NewClientError(PG_ERROR_SEVERITY_ERROR, PG_ERROR_CODE_PROTOCOL_VIOLATION, "unsupported bind parameter format", err)
		}
	}
	generatedParameterNumbers := sortedGeneratedVectorParameterNumbers(preparedStatement.VectorParameters)
	for _, parameterNumber := range generatedParameterNumbers {
		if parameterNumber != len(variables)+1 {
			err := fmt.Errorf("hidden search rank parameter $%d is out of order", parameterNumber)
			return nil, nil, NewClientError(PG_ERROR_SEVERITY_ERROR, PG_ERROR_CODE_PROTOCOL_VIOLATION, err.Error(), err)
		}
		spec := preparedStatement.VectorParameters[parameterNumber]
		text := ""
		if spec.SourceParameter != 0 {
			if spec.SourceParameter > len(message.Parameters) || message.Parameters[spec.SourceParameter-1] == nil {
				return nil, nil, fmt.Errorf("unbound search text parameter $%d", spec.SourceParameter)
			}
			text = string(message.Parameters[spec.SourceParameter-1])
		} else if spec.LiteralText != nil {
			text = *spec.LiteralText
		}
		value, err := queryHandler.resolveSearchParameter(context.Background(), text, spec, embeddingCache)
		if err != nil {
			return nil, nil, NewClientError(PG_ERROR_SEVERITY_ERROR, PG_ERROR_CODE_INVALID_PARAMETER_VALUE, err.Error(), err)
		}
		variables = append(variables, value)
	}

	LogDebug(queryHandler.Config, "Bound variables:", redactVectorVariables(variables, preparedStatement.VectorParameters))
	preparedStatement.Bound = true
	preparedStatement.Variables = variables
	preparedStatement.Portal = message.DestinationPortal

	messages := []pgproto3.Message{&pgproto3.BindComplete{}}

	return messages, preparedStatement, nil
}

func parseTextArrayBindParameter(param []byte, textFormat bool) ([]string, error) {
	formatCode := int16(pgtype.BinaryFormatCode)
	if textFormat {
		formatCode = pgtype.TextFormatCode
	}

	var value []string
	if err := pgtype.NewMap().Scan(pgtype.TextArrayOID, formatCode, param, &value); err != nil {
		return nil, fmt.Errorf("invalid text[] bind parameter: %w", err)
	}
	return value, nil
}

func (queryHandler *QueryHandler) resolveEmbeddedLiteralParameters(ctx context.Context, specs map[int]SearchParameterSpec) ([]interface{}, error) {
	parameterNumbers := sortedGeneratedVectorParameterNumbers(specs)
	if len(parameterNumbers) == 0 {
		for parameterNumber, spec := range specs {
			if spec.LiteralText == nil {
				return nil, fmt.Errorf("search rank($%d) requires the extended query protocol", parameterNumber)
			}
		}
		return nil, nil
	}
	variables := make([]interface{}, 0, len(parameterNumbers))
	cache := make(map[string][]float32)
	for index, parameterNumber := range parameterNumbers {
		if parameterNumber != index+1 {
			return nil, fmt.Errorf("unbound parameter precedes search rank literal")
		}
		spec := specs[parameterNumber]
		if spec.LiteralText == nil && spec.RowCountTable == "" {
			return nil, fmt.Errorf("search rank requires the extended query protocol")
		}
		value, err := queryHandler.resolveSearchParameter(ctx, searchLiteralText(spec), spec, cache)
		if err != nil {
			return nil, err
		}
		variables = append(variables, value)
	}
	return variables, nil
}

func sortedGeneratedVectorParameterNumbers(specs map[int]SearchParameterSpec) []int {
	parameterNumbers := make([]int, 0)
	for parameterNumber, spec := range specs {
		if spec.LiteralText != nil || spec.SourceParameter != 0 || spec.RowCountTable != "" {
			parameterNumbers = append(parameterNumbers, parameterNumber)
		}
	}
	sort.Ints(parameterNumbers)
	return parameterNumbers
}

func searchLiteralText(spec SearchParameterSpec) string {
	if spec.LiteralText != nil {
		return *spec.LiteralText
	}
	return ""
}

func (queryHandler *QueryHandler) resolveSearchParameter(ctx context.Context, text string, spec SearchParameterSpec, cache map[string][]float32) (interface{}, error) {
	if spec.RowCountTable != "" {
		var count int64
		rows, err := queryHandler.ServerDuckdbClient.QueryContext(ctx, "SELECT count(*) FROM "+spec.RowCountTable)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		if !rows.Next() {
			return nil, fmt.Errorf("failed to count Search chunks: %v", rows.Err())
		}
		err = rows.Scan(&count)
		return max(int64(1), count), err
	}
	if spec.TextOnly {
		if err := validateEmbeddingInput(text); err != nil {
			return nil, err
		}
		return text, nil
	}
	return queryHandler.embedVectorParameter(ctx, text, spec, cache)
}

func (queryHandler *QueryHandler) embedVectorParameter(ctx context.Context, text string, spec SearchParameterSpec, cache map[string][]float32) ([]float32, error) {
	if err := validateEmbeddingInput(text); err != nil {
		return nil, err
	}
	if cache != nil {
		if vector, ok := cache[text]; ok {
			return vector, nil
		}
	}
	vector, err := queryHandler.EmbeddingClient.Embed(ctx, text)
	if err != nil {
		return nil, queryHandler.hiddenEmbeddingError(err)
	}
	if len(vector) != spec.Dimensions {
		return nil, queryHandler.hiddenEmbeddingError(fmt.Errorf("embedding response must have %d dimensions, got %d", spec.Dimensions, len(vector)))
	}
	normSquared := 0.0
	for _, component := range vector {
		if math.IsNaN(float64(component)) || math.IsInf(float64(component), 0) {
			return nil, queryHandler.hiddenEmbeddingError(fmt.Errorf("embedding response components must be finite REAL values"))
		}
		normSquared += float64(component) * float64(component)
	}
	if spec.RequireNonZero && normSquared == 0 {
		return nil, queryHandler.hiddenEmbeddingError(fmt.Errorf("cosine distance requires a non-zero embedding response"))
	}
	if cache != nil {
		cache[text] = vector
	}
	return vector, nil
}

func (queryHandler *QueryHandler) hiddenEmbeddingError(err error) error {
	LogError(queryHandler.Config, "search rank failed:", err)
	return fmt.Errorf("search rank failed")
}

func (queryHandler *QueryHandler) HandleDescribeQuery(message *pgproto3.Describe, preparedStatement *PreparedStatement) ([]pgproto3.Message, *PreparedStatement, error) {
	switch message.ObjectType {
	case 'S': // Statement
		if message.Name != preparedStatement.Name {
			err := fmt.Errorf("statement mismatch, %s instead of %s: %s", message.Name, preparedStatement.Name, preparedStatement.OriginalQuery)
			return nil, nil, NewClientError(PG_ERROR_SEVERITY_ERROR, PG_ERROR_CODE_PROTOCOL_VIOLATION, "describe message references an unexpected prepared statement", err)
		}
	case 'P': // Portal
		if message.Name != preparedStatement.Portal {
			err := fmt.Errorf("portal mismatch, %s instead of %s: %s", message.Name, preparedStatement.Portal, preparedStatement.OriginalQuery)
			return nil, nil, NewClientError(PG_ERROR_SEVERITY_ERROR, PG_ERROR_CODE_PROTOCOL_VIOLATION, "describe message references an unexpected portal", err)
		}
	default:
		err := fmt.Errorf("unsupported describe object type: %c. Original query: %s", message.ObjectType, preparedStatement.OriginalQuery)
		return nil, nil, NewClientError(PG_ERROR_SEVERITY_ERROR, PG_ERROR_CODE_PROTOCOL_VIOLATION, "unsupported describe object type", err)
	}

	preparedStatement.Described = true
	if preparedStatement.Query == "" || !preparedStatement.Bound { // Empty query or Parse->[No Bind]->Describe
		return []pgproto3.Message{&pgproto3.NoData{}}, preparedStatement, nil
	}

	rows, err := preparedStatement.Statement.QueryContext(context.Background(), preparedStatement.Variables...)
	if err != nil {
		return nil, nil, fmt.Errorf("couldn't execute statement: %w. Original query: %s", err, preparedStatement.OriginalQuery)
	}
	preparedStatement.Rows = rows

	messages, err := queryHandler.rowsToDescriptionMessages(preparedStatement.Rows, preparedStatement.OriginalQuery)
	if err != nil {
		return nil, nil, err
	}
	return messages, preparedStatement, nil
}

func (queryHandler *QueryHandler) HandleExecuteQuery(message *pgproto3.Execute, preparedStatement *PreparedStatement) ([]pgproto3.Message, error) {
	if message.Portal != preparedStatement.Portal {
		err := fmt.Errorf("portal mismatch, %s instead of %s: %s", message.Portal, preparedStatement.Portal, preparedStatement.OriginalQuery)
		return nil, NewClientError(PG_ERROR_SEVERITY_ERROR, PG_ERROR_CODE_PROTOCOL_VIOLATION, "execute message references an unexpected portal", err)
	}

	if preparedStatement.Query == "" {
		return []pgproto3.Message{&pgproto3.EmptyQueryResponse{}}, nil
	}

	if preparedStatement.Rows == nil { // Parse->[No Bind]->Describe->Execute or Parse->Bind->[No Describe]->Execute
		rows, err := preparedStatement.Statement.QueryContext(context.Background(), preparedStatement.Variables...)
		if err != nil {
			return nil, err
		}
		preparedStatement.Rows = rows
	}

	defer preparedStatement.Rows.Close()

	return queryHandler.rowsToDataMessages(preparedStatement.Rows, preparedStatement.OriginalQuery)
}

func (queryHandler *QueryHandler) rowsToDescriptionMessages(rows *sql.Rows, originalQuery string) ([]pgproto3.Message, error) {
	cols, err := rows.ColumnTypes()
	if err != nil {
		return nil, fmt.Errorf("couldn't get column types: %w. Original query: %s", err, originalQuery)
	}

	var messages []pgproto3.Message

	rowDescription := queryHandler.generateRowDescription(cols)
	if rowDescription != nil {
		messages = append(messages, rowDescription)
	}

	return messages, nil
}

func (queryHandler *QueryHandler) rowsToDataMessages(rows *sql.Rows, originalQuery string) ([]pgproto3.Message, error) {
	cols, err := rows.ColumnTypes()
	if err != nil {
		return nil, fmt.Errorf("couldn't get column types: %w. Original query: %s", err, originalQuery)
	}

	var messages []pgproto3.Message
	for rows.Next() {
		dataRow, err := queryHandler.generateDataRow(rows, cols)
		if err != nil {
			return nil, fmt.Errorf("couldn't get data row: %w. Original query: %s", err, originalQuery)
		}
		messages = append(messages, dataRow)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("couldn't read query results: %w. Original query: %s", err, originalQuery)
	}

	commandTag := FALLBACK_SQL_QUERY
	upperOriginalQueryStatement := strings.ToUpper(originalQuery)
	switch {
	case strings.HasPrefix(upperOriginalQueryStatement, "SET "):
		commandTag = "SET"
	case strings.HasPrefix(upperOriginalQueryStatement, "SHOW "):
		commandTag = "SHOW"
	case strings.HasPrefix(upperOriginalQueryStatement, "DISCARD ALL"):
		commandTag = "DISCARD ALL"
	case strings.HasPrefix(upperOriginalQueryStatement, "BEGIN"):
		commandTag = "BEGIN"
	case strings.HasPrefix(upperOriginalQueryStatement, "COMMIT"):
		commandTag = "COMMIT"
	case strings.HasPrefix(upperOriginalQueryStatement, "CREATE MATERIALIZED VIEW "):
		commandTag = "CREATE MATERIALIZED VIEW"
	case strings.HasPrefix(upperOriginalQueryStatement, "DROP MATERIALIZED VIEW "):
		commandTag = "DROP MATERIALIZED VIEW"
	case strings.HasPrefix(upperOriginalQueryStatement, "REFRESH MATERIALIZED VIEW "):
		commandTag = "REFRESH MATERIALIZED VIEW"
	default:
		// Fallback to SELECT from FALLBACK_SQL_QUERY
	}

	messages = append(messages, &pgproto3.CommandComplete{CommandTag: []byte(commandTag)})
	return messages, nil
}

func (queryHandler *QueryHandler) generateRowDescription(cols []*sql.ColumnType) *pgproto3.RowDescription {
	description := pgproto3.RowDescription{Fields: []pgproto3.FieldDescription{}}

	for _, col := range cols {
		typeIod := queryHandler.ResponseHandler.ColumnDescriptionTypeOid(col)

		if col.Name() == "Success" && typeIod == pgtype.BoolOID && len(cols) == 1 {
			// Skip the "Success" DuckDBClient column returned from SET ... commands
			return nil
		}

		description.Fields = append(description.Fields, pgproto3.FieldDescription{
			Name:                 []byte(col.Name()),
			TableOID:             0,
			TableAttributeNumber: 0,
			DataTypeOID:          typeIod,
			DataTypeSize:         -1,
			TypeModifier:         -1,
			Format:               0,
		})
	}
	return &description
}

func (queryHandler *QueryHandler) generateDataRow(rows *sql.Rows, cols []*sql.ColumnType) (*pgproto3.DataRow, error) {
	valuePointers := make([]interface{}, len(cols))
	for i, col := range cols {
		valuePointers[i] = queryHandler.ResponseHandler.RowValuePointer(col)
	}

	err := rows.Scan(valuePointers...)
	if err != nil {
		return nil, err
	}

	var values [][]byte
	for i, valuePointer := range valuePointers {
		value := queryHandler.ResponseHandler.RowValueBytes(valuePointer, cols[i])
		values = append(values, value)
	}
	dataRow := pgproto3.DataRow{Values: values}

	return &dataRow, nil
}
