package main

import (
	"context"
	"database/sql"
	"encoding/binary"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/BemiHQ/BemiDB/src/common"
)

const (
	FALLBACK_SQL_QUERY        = "SELECT 1"
	PREPARE_STATEMENT_TIMEOUT = 30 * time.Second
)

type QueryHandler struct {
	Config             *Config
	ServerDuckdbClient *common.DuckdbClient
	QueryRemapper      *QueryRemapper
	ResponseHandler    *ResponseHandler
}

type PreparedStatement struct {
	// Parse
	Name          string
	OriginalQuery string
	Query         string
	Statement     *sql.Stmt
	ParameterOIDs []uint32

	// Bind
	Bound     bool
	Variables []interface{}
	Portal    string

	// Describe
	Described bool

	// Describe/Execute
	Rows *sql.Rows
}

func NewQueryHandler(config *Config, serverDuckdbClient *common.DuckdbClient) *QueryHandler {
	storageS3 := common.NewStorageS3(config.CommonConfig)
	icebergCatalog := common.NewIcebergCatalog(config.CommonConfig)
	icebergReader := NewIcebergReader(config, icebergCatalog)
	icebergWriter := NewIcebergWriter(config, storageS3, serverDuckdbClient, icebergCatalog)

	queryHandler := &QueryHandler{
		Config:             config,
		ServerDuckdbClient: serverDuckdbClient,
		QueryRemapper:      NewQueryRemapper(config, icebergReader, icebergWriter, serverDuckdbClient),
		ResponseHandler:    NewResponseHandler(config),
	}

	return queryHandler
}

func (queryHandler *QueryHandler) HandleSimpleQuery(originalQuery string) ([]pgproto3.Message, error) {
	queryStatements, originalQueryStatements, err := queryHandler.QueryRemapper.ParseAndRemapQuery(originalQuery)
	if err != nil {
		return nil, err
	}
	if len(queryStatements) == 0 {
		return []pgproto3.Message{&pgproto3.EmptyQueryResponse{}}, nil
	}

	var queriesMessages []pgproto3.Message

	for i, queryStatement := range queryStatements {
		rows, err := queryHandler.ServerDuckdbClient.QueryContext(context.Background(), queryStatement)
		if err != nil {
			errorMessage := err.Error()
			if errorMessage == "Binder Error: UNNEST requires a single list as input" {
				// https://github.com/duckdbClient/duckdb/issues/11693
				common.LogWarn(queryHandler.Config.CommonConfig, "Couldn't handle query via DuckDB:", queryStatement+"\n"+err.Error())
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
		descriptionMessages, err := queryHandler.rowsToDescriptionMessages(rows, originalQueryStatements[i])
		if err != nil {
			return nil, err
		}
		queryMessages = append(queryMessages, descriptionMessages...)
		dataMessages, err := queryHandler.rowsToDataMessages(rows, originalQueryStatements[i])
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
	queryStatements, _, err := queryHandler.QueryRemapper.ParseAndRemapQuery(originalQuery)
	if err != nil {
		return nil, nil, err
	}
	if len(queryStatements) > 1 {
		return nil, nil, fmt.Errorf("multiple queries in a single parse message are not supported: %s", originalQuery)
	}

	preparedStatement := &PreparedStatement{
		Name:          message.Name,
		OriginalQuery: originalQuery,
		ParameterOIDs: message.ParameterOIDs,
	}
	if len(queryStatements) == 0 {
		return []pgproto3.Message{&pgproto3.ParseComplete{}}, preparedStatement, nil
	}

	query := queryStatements[0]
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
		common.LogWarn(queryHandler.Config.CommonConfig, "PrepareContext timed out, recreating DuckDB connection")
		queryHandler.ServerDuckdbClient.RecreateDb()
		queryHandler.QueryRemapper.remapperTable.Reset()
		return queryHandler.ServerDuckdbClient.PrepareContext(ctx, query)
	}
}

func (queryHandler *QueryHandler) HandleBindQuery(message *pgproto3.Bind, preparedStatement *PreparedStatement) ([]pgproto3.Message, *PreparedStatement, error) {
	if message.PreparedStatement != preparedStatement.Name {
		return nil, nil, fmt.Errorf("prepared statement mismatch, %s instead of %s: %s", message.PreparedStatement, preparedStatement.Name, preparedStatement.OriginalQuery)
	}

	var variables []interface{}
	paramFormatCodes := message.ParameterFormatCodes

	for i, param := range message.Parameters {
		if param == nil {
			continue
		}

		textFormat := true
		if len(paramFormatCodes) == 1 {
			textFormat = paramFormatCodes[0] == 0
		} else if len(paramFormatCodes) > 1 {
			textFormat = paramFormatCodes[i] == 0
		}

		if textFormat {
			variables = append(variables, string(param))
		} else if len(param) == 4 {
			variables = append(variables, int32(binary.BigEndian.Uint32(param)))
		} else if len(param) == 8 {
			variables = append(variables, int64(binary.BigEndian.Uint64(param)))
		} else if len(param) == 16 {
			variables = append(variables, uuid.UUID(param).String())
		} else {
			return nil, nil, fmt.Errorf("unsupported parameter format: %v (length %d). Original query: %s", param, len(param), preparedStatement.OriginalQuery)
		}
	}

	common.LogDebug(queryHandler.Config.CommonConfig, "Bound variables:", variables)
	preparedStatement.Bound = true
	preparedStatement.Variables = variables
	preparedStatement.Portal = message.DestinationPortal

	messages := []pgproto3.Message{&pgproto3.BindComplete{}}

	return messages, preparedStatement, nil
}

func (queryHandler *QueryHandler) HandleDescribeQuery(message *pgproto3.Describe, preparedStatement *PreparedStatement) ([]pgproto3.Message, *PreparedStatement, error) {
	switch message.ObjectType {
	case 'S': // Statement
		if message.Name != preparedStatement.Name {
			return nil, nil, fmt.Errorf("statement mismatch, %s instead of %s: %s", message.Name, preparedStatement.Name, preparedStatement.OriginalQuery)
		}
	case 'P': // Portal
		if message.Name != preparedStatement.Portal {
			return nil, nil, fmt.Errorf("portal mismatch, %s instead of %s: %s", message.Name, preparedStatement.Portal, preparedStatement.OriginalQuery)
		}
	default:
		return nil, nil, fmt.Errorf("unsupported describe object type: %c. Original query: %s", message.ObjectType, preparedStatement.OriginalQuery)
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
		return nil, fmt.Errorf("portal mismatch, %s instead of %s: %s", message.Portal, preparedStatement.Portal, preparedStatement.OriginalQuery)
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
