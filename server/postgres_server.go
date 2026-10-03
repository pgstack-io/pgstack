package main

import (
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
)

const (
	PG_VERSION        = "17.0"
	PG_ENCODING       = "UTF8"
	PG_TX_STATUS_IDLE = 'I'

	RETRY_SIMPLE_QUERY_TIMEOUT = 5 * time.Second
	CLIENT_ERROR_MESSAGE       = "internal server error"
)

type PostgresServer struct {
	backend *pgproto3.Backend
	conn    *net.Conn
	config  *Config
}

func NewPostgresServer(config *Config, conn *net.Conn) *PostgresServer {
	return &PostgresServer{
		conn:    conn,
		backend: pgproto3.NewBackend(*conn, *conn),
		config:  config,
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
	err := server.handleStartup()
	if err != nil {
		LogError(server.config, "Error handling startup:", err)
		return // Terminate connection
	}
	for {
		message, err := server.backend.Receive()
		if err != nil {
			return // Terminate connection
		}

		switch message := message.(type) {
		case *pgproto3.Query:
			queryHandler, releaseRuntime, err := runtimeProvider(server.config)
			if err != nil {
				server.writeError(fmt.Errorf("failed to initialize database runtime: %w", err))
				continue
			}
			server.handleSimpleQuery(queryHandler, message)
			releaseRuntime()
		case *pgproto3.Parse:
			queryHandler, releaseRuntime, err := runtimeProvider(server.config)
			if err != nil {
				server.writeError(fmt.Errorf("failed to initialize database runtime: %w", err))
				continue
			}
			err = server.handleExtendedQuery(queryHandler, message) // recursion
			releaseRuntime()
			if err != nil {
				return // Terminate connection
			}
		case *pgproto3.Terminate:
			LogDebug(server.config, "Client terminated connection")
			return
		default:
			LogError(server.config, "Received message other than Query from client:", message)
			return // Terminate connection
		}
	}
}

func (server *PostgresServer) Close() error {
	return (*server.conn).Close()
}

func (server *PostgresServer) handleSimpleQuery(queryHandler *QueryHandler, queryMessage *pgproto3.Query) {
	LogDebug(server.config, "Received query:", queryMessage.String)
	messages, err := queryHandler.HandleSimpleQuery(queryMessage.String)
	if err != nil {
		if server.isS3NotFoundError(err) {
			LogDebug(server.config, "Retrying query due to resynced table")
			time.Sleep(RETRY_SIMPLE_QUERY_TIMEOUT)
			messages, err = queryHandler.HandleSimpleQuery(queryMessage.String)
		}
		if err != nil {
			server.writeError(err)
			return
		}
	}
	messages = append(messages, &pgproto3.ReadyForQuery{TxStatus: PG_TX_STATUS_IDLE})
	server.writeMessages(messages...)
}

func (server *PostgresServer) isS3NotFoundError(err error) bool {
	return strings.Contains(err.Error(), "v1.metadata.json\": 404 (Not Found)") ||
		strings.Contains(err.Error(), "v1.metadata.json': (HTTP 404)") ||
		strings.Contains(err.Error(), ".parquet' (HTTP 404)") ||
		strings.Contains(err.Error(), ".avro' (HTTP 404)")
}

func (server *PostgresServer) handleExtendedQuery(queryHandler *QueryHandler, parseMessage *pgproto3.Parse) error {
	LogDebug(server.config, "Parsing query", parseMessage.Query)
	messages, preparedStatement, err := queryHandler.HandleParseQuery(parseMessage)
	if err != nil {
		server.writeError(err)
		return nil
	}
	server.writeMessages(messages...)

	var previousErr error
	for {
		message, err := server.backend.Receive()
		if err != nil {
			return err
		}

		switch message := message.(type) {
		case *pgproto3.Bind:
			if previousErr != nil { // Skip processing the next message if there was an error in the previous message
				continue
			}

			LogDebug(server.config, "Binding query", message.PreparedStatement)
			messages, preparedStatement, err = queryHandler.HandleBindQuery(message, preparedStatement)
			if err != nil {
				server.writeError(err)
				previousErr = err
			}
			server.writeMessages(messages...)
		case *pgproto3.Describe:
			if previousErr != nil { // Skip processing the next message if there was an error in the previous message
				continue
			}

			LogDebug(server.config, "Describing query", message.Name, "("+string(message.ObjectType)+")")
			var messages []pgproto3.Message
			messages, preparedStatement, err = queryHandler.HandleDescribeQuery(message, preparedStatement)
			if err != nil {
				server.writeError(err)
				previousErr = err
			}
			server.writeMessages(messages...)
		case *pgproto3.Execute:
			if previousErr != nil { // Skip processing the next message if there was an error in the previous message
				continue
			}

			LogDebug(server.config, "Executing query", message.Portal)
			messages, err := queryHandler.HandleExecuteQuery(message, preparedStatement)
			if err != nil {
				server.writeError(err)
				previousErr = err
			}
			server.writeMessages(messages...)
		case *pgproto3.Sync:
			LogDebug(server.config, "Syncing query")
			server.writeMessages(
				&pgproto3.ReadyForQuery{TxStatus: PG_TX_STATUS_IDLE},
			)

			// If there was an error or Parse->Bind->Sync (...) or Parse->Describe->Sync (e.g., Metabase)
			// it means that sync is the last message in the extended query protocol, we can exit handleExtendedQuery
			if previousErr != nil || preparedStatement.Bound || preparedStatement.Described {
				return nil
			}
			// Otherwise, wait for Bind/Describe/Execute/Sync.
			// For example, psycopg sends Parse->[extra Sync]->Bind->Describe->Execute->Sync
		case *pgproto3.Parse:
			server.handleExtendedQuery(queryHandler, message) // self-recursion
			return nil
		case *pgproto3.Flush:
			// Ignore Flush messages, as we are sending responses immediately.
		case *pgproto3.Close:
			LogDebug(server.config, "Closing prepared statement", message.Name)
			server.writeMessages(&pgproto3.CloseComplete{})
		default:
			LogError(server.config, fmt.Sprintf("Received unexpected message type from client: %T", message))
			return fmt.Errorf("received unexpected message type from client: %T", message)
		}
	}
}

func (server *PostgresServer) writeMessages(messages ...pgproto3.Message) {
	var buf []byte
	for _, message := range messages {
		buf, _ = message.Encode(buf)
	}
	(*server.conn).Write(buf)
}

func (server *PostgresServer) writeError(err error) {
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
	if severity != PG_ERROR_SEVERITY_FATAL {
		messages = append(messages, &pgproto3.ReadyForQuery{TxStatus: PG_TX_STATUS_IDLE})
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
