package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
	pgQuery "github.com/pganalyze/pg_query_go/v6"
)

const (
	PG_WRITE_BUFFER_SIZE     = 64 * 1024
	PG_VERSION               = "17.0"
	PG_ENCODING              = "UTF8"
	PG_TX_STATUS_IDLE        = 'I'
	PG_TX_STATUS_TRANSACTION = 'T'
	PG_TX_STATUS_FAILED      = 'E'

	RETRY_SIMPLE_QUERY_TIMEOUT = 5 * time.Second
	CLIENT_ERROR_MESSAGE       = "internal server error"
)

type PostgresServer struct {
	backend           *pgproto3.Backend
	conn              *net.Conn
	config            *Config
	writer            *bufio.Writer
	writeErr          error
	txStatus          byte
	statements        map[string]*PreparedStatement
	portals           map[string]*Portal
	cursorIdleTimeout time.Duration
}

func NewPostgresServer(config *Config, conn *net.Conn) *PostgresServer {
	return &PostgresServer{
		conn:              conn,
		backend:           pgproto3.NewBackend(*conn, *conn),
		writer:            bufio.NewWriterSize(*conn, PG_WRITE_BUFFER_SIZE),
		config:            config,
		txStatus:          PG_TX_STATUS_IDLE,
		statements:        make(map[string]*PreparedStatement),
		portals:           make(map[string]*Portal),
		cursorIdleTimeout: PG_CURSOR_IDLE_TIMEOUT,
	}
}

func NewTcpListener(config *Config) net.Listener {
	parsedIp := net.ParseIP(config.Host)
	if parsedIp == nil {
		PrintErrorAndExit(config, "Invalid host: "+config.Host+".")
	}

	var network, host string
	if parsedIp.To4() == nil {
		network = "tcp6"
		host = "[" + config.Host + "]"
	} else {
		network = "tcp4"
		host = config.Host
	}

	tcpListener, err := net.Listen(network, host+":"+config.Port)
	PanicIfError(config, err)
	return tcpListener
}

func AcceptConnection(config *Config, listener net.Listener) net.Conn {
	conn, err := listener.Accept()
	PanicIfError(config, err)
	return conn
}

func (server *PostgresServer) Run(runtimeProvider func(*Config) (*QueryHandler, func(), error)) {
	if err := server.handleStartup(); err != nil {
		LogError(server.config, "Error handling startup:", err)
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	messages, readerDone := server.receiveMessages(ctx, cancel)
	defer func() { cancel(); (*server.conn).Close(); <-readerDone }()
	var queryHandler *QueryHandler
	var releaseRuntime func()
	defer func() {
		server.closeStatements()
		if releaseRuntime != nil {
			releaseRuntime()
		}
	}()
	releaseIfIdle := func() {
		if releaseRuntime == nil {
			return
		}
		for _, portal := range server.portals {
			if portal.Reader != nil {
				return
			}
		}
		releaseRuntime()
		releaseRuntime, queryHandler = nil, nil
	}
	skipUntilSync := false
	for {
		if server.writeErr != nil {
			return
		}
		var idle <-chan time.Time
		var timer *time.Timer
		if server.hasOpenCursor() {
			timer = time.NewTimer(server.cursorIdleTimeout)
			idle = timer.C
		}
		var message pgproto3.FrontendMessage
		select {
		case <-ctx.Done():
			if timer != nil {
				timer.Stop()
			}
			return
		case <-idle:
			LogInfo(server.config, "Closing connection with idle cursor")
			cancel()
			return
		case next, ok := <-messages:
			if timer != nil {
				timer.Stop()
			}
			if !ok {
				return
			}
			message = next
		}
		var err error
		if _, ok := message.(*pgproto3.Sync); ok {
			skipUntilSync = false
			if server.txStatus == PG_TX_STATUS_IDLE {
				server.closePortals()
			}
			server.writeMessages(&pgproto3.ReadyForQuery{TxStatus: server.txStatus})
			releaseIfIdle()
			continue
		}
		if skipUntilSync {
			continue
		}
		if _, ok := message.(*pgproto3.Flush); ok {
			continue
		}
		if queryHandler == nil {
			queryHandler, releaseRuntime, err = runtimeProvider(server.config)
			if err != nil {
				_, simple := message.(*pgproto3.Query)
				server.writeErrorResponse(fmt.Errorf("failed to initialize database runtime: %w", err), simple)
				skipUntilSync = !simple
				continue
			}
		}
		if query, ok := message.(*pgproto3.Query); ok {
			// A simple query destroys the unnamed prepared statement and portal.
			server.closeStatement("")
			server.closePortal("")
			server.handleSimpleQuery(ctx, queryHandler, query)
			releaseIfIdle()
			continue
		}
		if err := server.handleExtendedMessage(ctx, queryHandler, message); err != nil {
			server.abortTransaction()
			server.writeErrorResponse(err, false)
			skipUntilSync = true
		}
	}
}

func (server *PostgresServer) handleExtendedMessage(ctx context.Context, handler *QueryHandler, message pgproto3.FrontendMessage) error {
	switch message := message.(type) {
	case *pgproto3.Parse:
		messages, statement, err := handler.HandleParseQuery(ctx, message)
		if err != nil {
			return err
		}
		server.closeStatement(message.Name)
		server.statements[message.Name] = statement
		server.writeMessages(messages...)
	case *pgproto3.Bind:
		statement, ok := server.statements[message.PreparedStatement]
		if !ok {
			return fmt.Errorf("unknown prepared statement %q", message.PreparedStatement)
		}
		messages, bound, err := handler.HandleBindQuery(ctx, message, statement)
		if err != nil {
			return err
		}
		server.closePortal(message.DestinationPortal)
		server.portals[message.DestinationPortal] = bound
		server.writeMessages(messages...)
	case *pgproto3.Describe:
		var messages []pgproto3.Message
		var err error
		switch message.ObjectType {
		case 'S':
			statement := server.statements[message.Name]
			if statement == nil {
				return fmt.Errorf("unknown statement %q", message.Name)
			}
			messages, err = handler.DescribeStatement(ctx, message, statement)
		case 'P':
			portal := server.portals[message.Name]
			if portal == nil {
				return fmt.Errorf("unknown portal %q", message.Name)
			}
			messages, err = handler.DescribePortal(ctx, message, portal)
		default:
			return fmt.Errorf("unsupported describe target %c", message.ObjectType)
		}
		if err != nil {
			return err
		}
		server.writeMessages(messages...)
	case *pgproto3.Execute:
		statement := server.portals[message.Portal]
		if statement == nil {
			return fmt.Errorf("unknown portal %q", message.Portal)
		}
		if statement.Terminal != nil {
			server.writeMessages(statement.Terminal)
			return nil
		}
		if err := server.transactionError(statement.QueryClassification); err != nil {
			return err
		}
		if messages := server.transactionMessages(statement.QueryClassification); messages != nil {
			statement.Terminal = messages[len(messages)-1]
			server.writeMessages(messages...)
			server.applyTransactionStatus(statement.QueryClassification)
			return nil
		}
		if err := server.executePortal(ctx, handler, message, statement); err != nil {
			return err
		}
		server.applyTransactionStatus(statement.QueryClassification)
	case *pgproto3.Close:
		switch message.ObjectType {
		case 'S':
			server.closeStatement(message.Name)
		case 'P':
			server.closePortal(message.Name)
		default:
			return fmt.Errorf("unsupported close target %c", message.ObjectType)
		}
		server.writeMessages(&pgproto3.CloseComplete{})
	default:
		return fmt.Errorf("received unexpected message type from client: %T", message)
	}
	return nil
}

func (server *PostgresServer) executePortal(ctx context.Context, handler *QueryHandler, execute *pgproto3.Execute, statement *Portal) error {
	remaining := execute.MaxRows
	// Internal batch limits must not turn an ordinary fetch-all into a cursor.
	cursorExecution := statement.IsCursor(execute.MaxRows)
	for {
		batch := *execute
		if cursorExecution {
			batch.MaxRows = QUERY_RESULT_BATCH_SIZE
			if remaining > 0 && remaining < batch.MaxRows {
				batch.MaxRows = remaining
			}
		}
		messages, err := handler.HandleExecuteQuery(ctx, &batch, statement)
		if err != nil {
			return err
		}
		suspended := false
		if len(messages) > 0 {
			_, suspended = messages[len(messages)-1].(*pgproto3.PortalSuspended)
		}
		if !suspended {
			server.writeMessages(messages...)
			return nil
		}
		rows := uint32(len(messages) - 1)
		if execute.MaxRows != 0 {
			remaining -= rows
			if remaining == 0 {
				server.writeMessages(messages...)
				return nil
			}
		}
		server.writeMessages(messages[:len(messages)-1]...)
		if server.writeErr != nil {
			return server.writeErr
		}
	}
}

func (server *PostgresServer) closePortal(name string) {
	if portal := server.portals[name]; portal != nil {
		if err := portal.Close(); err != nil {
			LogError(server.config, "Error closing portal:", err)
		}
		delete(server.portals, name)
	}
}

func (server *PostgresServer) closePortals() {
	for name := range server.portals {
		server.closePortal(name)
	}
}

func (server *PostgresServer) closeStatement(name string) {
	if statement := server.statements[name]; statement != nil {
		// Closing a statement also drops portals created from it.
		for portalName, portal := range server.portals {
			if portal.PreparedStatement == statement {
				server.closePortal(portalName)
			}
		}
		delete(server.statements, name)
	}
}

func (server *PostgresServer) closeStatements() {
	server.closePortals()
	for name := range server.statements {
		server.closeStatement(name)
	}
}

func (server *PostgresServer) hasOpenCursor() bool {
	for _, portal := range server.portals {
		if portal.Reader != nil {
			return true
		}
	}
	return false
}

func (server *PostgresServer) applyTransactionStatus(query QueryClassification) {
	switch query.Kind {
	case QUERY_BEGIN:
		server.txStatus = PG_TX_STATUS_TRANSACTION
	case QUERY_COMMIT, QUERY_ROLLBACK:
		server.txStatus = PG_TX_STATUS_IDLE
		if query.Chain {
			server.txStatus = PG_TX_STATUS_TRANSACTION
		}
		server.closePortals()
	case QUERY_DISCARD:
		server.closeStatements()
	}
}

func (server *PostgresServer) transactionMessages(query QueryClassification) []pgproto3.Message {
	tag := ""
	switch query.Kind {
	case QUERY_BEGIN:
		tag = "BEGIN"
	case QUERY_COMMIT:
		tag = "COMMIT"
		if server.txStatus == PG_TX_STATUS_FAILED {
			tag = "ROLLBACK"
		}
	case QUERY_ROLLBACK:
		tag = "ROLLBACK"
	}
	if tag == "" {
		return nil
	}
	return []pgproto3.Message{&pgproto3.CommandComplete{CommandTag: []byte(tag)}}
}

func (server *PostgresServer) transactionError(query QueryClassification) error {
	if query.Chain && query.EndsTransaction() && server.txStatus == PG_TX_STATUS_IDLE {
		return NewClientError(PG_ERROR_SEVERITY_ERROR, "25P01", "AND CHAIN requires an active transaction", nil)
	}
	if query.Kind == QUERY_DISCARD && server.txStatus == PG_TX_STATUS_TRANSACTION {
		return NewClientError(PG_ERROR_SEVERITY_ERROR, "25001", "DISCARD cannot run inside a transaction block", nil)
	}

	if server.txStatus == PG_TX_STATUS_FAILED && !query.EndsTransaction() {
		return NewClientError(PG_ERROR_SEVERITY_ERROR, "25P02", "current transaction is aborted, commands ignored until end of transaction block", nil)
	}
	if query.Kind == QUERY_UNSUPPORTED_TRANSACTION {
		return NewClientError(PG_ERROR_SEVERITY_ERROR, PG_ERROR_CODE_FEATURE_NOT_SUPPORTED, "this transaction command is not supported", nil)
	}
	return nil
}

func (server *PostgresServer) Close() error {
	return (*server.conn).Close()
}

func (server *PostgresServer) handleSimpleQuery(ctx context.Context, handler *QueryHandler, message *pgproto3.Query) {
	// Parse the entire packet first, then execute and update state per statement.
	tree, err := pgQuery.Parse(message.String)
	if err != nil {
		server.failSimpleQuery(NewClientError(PG_ERROR_SEVERITY_ERROR, PG_ERROR_CODE_SYNTAX_ERROR, err.Error(), err))
		return
	}
	if len(tree.Stmts) == 0 {
		server.writeMessages(&pgproto3.EmptyQueryResponse{})
	}
	for _, raw := range tree.Stmts {
		classification := classifyQuery(raw.Stmt)
		if err := server.transactionError(classification); err != nil {
			server.failSimpleQuery(err)
			return
		}
		original, err := pgQuery.Deparse(&pgQuery.ParseResult{Stmts: []*pgQuery.RawStmt{raw}})
		if err != nil {
			server.failSimpleQuery(err)
			return
		}
		messages := server.transactionMessages(classification)
		if messages == nil {
			messages, err = handler.HandleSimpleQuery(ctx, original)
		}
		if err != nil && server.isS3NotFoundError(err) {
			timer := time.NewTimer(RETRY_SIMPLE_QUERY_TIMEOUT)
			select {
			case <-ctx.Done():
				timer.Stop()
				err = ctx.Err()
			case <-timer.C:
				messages, err = handler.HandleSimpleQuery(ctx, original)
			}
		}
		if err != nil {
			server.failSimpleQuery(err)
			return
		}
		server.applyTransactionStatus(classification)
		server.writeMessages(messages...)
		if server.writeErr != nil {
			return
		}
	}
	server.writeMessages(&pgproto3.ReadyForQuery{TxStatus: server.txStatus})
}

func (server *PostgresServer) abortTransaction() {
	if server.txStatus == PG_TX_STATUS_TRANSACTION {
		server.txStatus = PG_TX_STATUS_FAILED
	}
	// Failed transactions cannot resume portals; release their readers promptly.
	server.closePortals()
}

func (server *PostgresServer) failSimpleQuery(err error) {
	server.abortTransaction()
	server.writeError(err)
}

func (server *PostgresServer) isS3NotFoundError(err error) bool {
	return strings.Contains(err.Error(), "v1.metadata.json\": 404 (Not Found)") ||
		strings.Contains(err.Error(), "v1.metadata.json': (HTTP 404)") ||
		strings.Contains(err.Error(), ".parquet' (HTTP 404)") ||
		strings.Contains(err.Error(), ".avro' (HTTP 404)")
}

func (server *PostgresServer) writeMessages(messages ...pgproto3.Message) {
	if server.writeErr != nil {
		return
	}
	for _, message := range messages {
		buf, err := message.Encode(nil)
		if err != nil {
			server.writeErr = err
			return
		}
		if _, err := server.writer.Write(buf); err != nil {
			server.writeErr = err
			return
		}
	}
	server.writeErr = server.writer.Flush()
}

func (server *PostgresServer) writeError(err error) {
	server.writeErrorResponse(err, true)
}

func (server *PostgresServer) writeErrorResponse(err error, ready bool) {
	LogError(server.config, err.Error())
	severity := PG_ERROR_SEVERITY_ERROR
	code := PG_ERROR_CODE_INTERNAL_ERROR
	message := CLIENT_ERROR_MESSAGE
	var clientError *ClientError
	if errors.As(err, &clientError) {
		severity = clientError.Severity
		code = clientError.Code
		message = clientError.Message
	}

	messages := []pgproto3.Message{
		&pgproto3.ErrorResponse{
			Severity: severity,
			Code:     code,
			Message:  message,
		},
	}
	if ready && severity != PG_ERROR_SEVERITY_FATAL {
		messages = append(messages, &pgproto3.ReadyForQuery{TxStatus: server.txStatus})
	}
	server.writeMessages(messages...)
}

func (server *PostgresServer) handleStartup() error {
	startupMessage, err := server.backend.ReceiveStartupMessage()
	if err != nil {
		return err
	}

	switch startupMessage := startupMessage.(type) {
	case *pgproto3.StartupMessage:
		params := startupMessage.Parameters
		LogDebug(server.config, "PgStack: startup message", params)

		connectionConfig, err := server.configForStartup(params["database"], params["user"])
		if err != nil {
			server.writeError(err)
			return err
		}
		server.config = connectionConfig
		if err := server.authenticateStartup(params["user"]); err != nil {
			server.writeError(err)
			return err
		}

		server.writeMessages(
			&pgproto3.AuthenticationOk{},
			&pgproto3.ParameterStatus{Name: "client_encoding", Value: PG_ENCODING},
			&pgproto3.ParameterStatus{Name: "server_version", Value: PG_VERSION},
			&pgproto3.ReadyForQuery{TxStatus: PG_TX_STATUS_IDLE},
		)
		return nil
	case *pgproto3.SSLRequest:
		_, err = (*server.conn).Write([]byte("N"))
		if err != nil {
			return err
		}
		return server.handleStartup()
	default:
		return errors.New("unknown startup message")
	}
}

func (server *PostgresServer) configForStartup(database string, user string) (*Config, error) {
	if database != server.config.Database {
		message := "database \"" + database + "\" does not exist"
		return nil, NewClientError(
			PG_ERROR_SEVERITY_FATAL,
			PG_ERROR_CODE_INVALID_CATALOG_NAME,
			message,
			errors.New(message),
		)
	}
	if !startupUserAllowed(server.config, user) {
		message := "role \"" + user + "\" does not exist"
		return nil, NewClientError(
			PG_ERROR_SEVERITY_FATAL,
			PG_ERROR_CODE_INVALID_AUTHORIZATION_SPECIFICATION,
			message,
			errors.New(message),
		)
	}

	return server.config, nil
}
