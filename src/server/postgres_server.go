package main

import (
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"

	"github.com/BemiHQ/BemiDB/src/common"
)

const (
	PG_VERSION        = "17.0"
	PG_ENCODING       = "UTF8"
	PG_TX_STATUS_IDLE = 'I'

	SYSTEM_AUTH_USER = "bemidb"

	RETRY_SIMPLE_QUERY_TIMEOUT = 5 * time.Second
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
		common.PrintErrorAndExit(config.CommonConfig, "Invalid host: "+config.Host+".")
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
	common.PanicIfError(config.CommonConfig, err)
	return tcpListener
}

func AcceptConnection(config *Config, listener net.Listener) net.Conn {
	conn, err := listener.Accept()
	common.PanicIfError(config.CommonConfig, err)
	return conn
}

func (server *PostgresServer) Run(queryHandler *QueryHandler) {
	err := server.handleStartup()
	if err != nil {
		common.LogError(server.config.CommonConfig, "Error handling startup:", err)
		return // Terminate connection
	}

	for {
		message, err := server.backend.Receive()
		if err != nil {
			return // Terminate connection
		}

		switch message := message.(type) {
		case *pgproto3.Query:
			server.handleSimpleQuery(queryHandler, message)
		case *pgproto3.Parse:
			err = server.handleExtendedQuery(queryHandler, message) // recursion
			if err != nil {
				return // Terminate connection
			}
		case *pgproto3.Terminate:
			common.LogDebug(server.config.CommonConfig, "Client terminated connection")
			return
		default:
			common.LogError(server.config.CommonConfig, "Received message other than Query from client:", message)
			return // Terminate connection
		}
	}
}

func (server *PostgresServer) Close() error {
	return (*server.conn).Close()
}

func (server *PostgresServer) handleSimpleQuery(queryHandler *QueryHandler, queryMessage *pgproto3.Query) {
	common.LogDebug(server.config.CommonConfig, "Received query:", queryMessage.String)
	messages, err := queryHandler.HandleSimpleQuery(queryMessage.String)
	if err != nil {
		if server.isS3NotFoundError(err) {
			common.LogDebug(server.config.CommonConfig, "Retrying query due to resynced table")
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
		strings.Contains(err.Error(), ".parquet' (HTTP 404)") ||
		strings.Contains(err.Error(), ".avro' (HTTP 404)")
}

func (server *PostgresServer) handleExtendedQuery(queryHandler *QueryHandler, parseMessage *pgproto3.Parse) error {
	common.LogDebug(server.config.CommonConfig, "Parsing query", parseMessage.Query)
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

			common.LogDebug(server.config.CommonConfig, "Binding query", message.PreparedStatement)
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

			common.LogDebug(server.config.CommonConfig, "Describing query", message.Name, "("+string(message.ObjectType)+")")
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

			common.LogDebug(server.config.CommonConfig, "Executing query", message.Portal)
			messages, err := queryHandler.HandleExecuteQuery(message, preparedStatement)
			if err != nil {
				server.writeError(err)
				previousErr = err
			}
			server.writeMessages(messages...)
		case *pgproto3.Sync:
			common.LogDebug(server.config.CommonConfig, "Syncing query")
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
			common.LogDebug(server.config.CommonConfig, "Closing prepared statement", message.Name)
			server.writeMessages(&pgproto3.CloseComplete{})
		default:
			common.LogError(server.config.CommonConfig, fmt.Sprintf("Received unexpected message type from client: %T", message))
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
	common.LogError(server.config.CommonConfig, err.Error())

	server.writeMessages(
		&pgproto3.ErrorResponse{
			Severity: "ERROR",
			Message:  err.Error(),
		},
		&pgproto3.ReadyForQuery{TxStatus: PG_TX_STATUS_IDLE},
	)
}

func (server *PostgresServer) handleStartup() error {
	startupMessage, err := server.backend.ReceiveStartupMessage()
	if err != nil {
		return err
	}

	switch startupMessage := startupMessage.(type) {
	case *pgproto3.StartupMessage:
		params := startupMessage.Parameters
		common.LogDebug(server.config.CommonConfig, "BemiDB: startup message", params)

		if params["database"] != server.config.Database {
			server.writeError(errors.New("database " + params["database"] + " does not exist"))
			return errors.New("database does not exist")
		}

		if server.config.User != "" && params["user"] != server.config.User && params["user"] != SYSTEM_AUTH_USER {
			server.writeError(errors.New("role \"" + params["user"] + "\" does not exist"))
			return errors.New("role does not exist")
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
		server.handleStartup()
		return nil
	default:
		return errors.New("unknown startup message")
	}
}
