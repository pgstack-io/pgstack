package main

import (
	"bytes"
	"errors"
	"github.com/jackc/pgx/v5/pgproto3"
	"net"
	"testing"
)

func TestConfigForStartupPreservesDatabaseError(t *testing.T) {
	server := &PostgresServer{config: &Config{Database: "db_expected", User: "u_expected"}}

	_, err := server.configForStartup("db_missing", "u_expected")
	if err == nil || err.Error() != `database "db_missing" does not exist` {
		t.Fatalf("unexpected error: %v", err)
	}
	assertClientError(t, err, PG_ERROR_SEVERITY_FATAL, PG_ERROR_CODE_INVALID_CATALOG_NAME, `database "db_missing" does not exist`)
}

func TestConfigForStartupPreservesRoleError(t *testing.T) {
	server := &PostgresServer{config: &Config{Database: "db_expected", User: "u_expected"}}

	_, err := server.configForStartup("db_expected", "u_missing")
	if err == nil || err.Error() != `role "u_missing" does not exist` {
		t.Fatalf("unexpected error: %v", err)
	}
	assertClientError(t, err, PG_ERROR_SEVERITY_FATAL, PG_ERROR_CODE_INVALID_AUTHORIZATION_SPECIFICATION, `role "u_missing" does not exist`)
}

func assertClientError(t *testing.T, err error, severity, code, message string) {
	t.Helper()
	var clientError *ClientError
	if !errors.As(err, &clientError) {
		t.Fatalf("error type = %T, want *ClientError", err)
	}
	if clientError.Severity != severity || clientError.Code != code || clientError.Message != message {
		t.Fatalf("client error = %+v, want severity=%s code=%s message=%q", clientError, severity, code, message)
	}
}

type bufferedTestConn struct {
	net.Conn
	bytes.Buffer
	writes  int
	failure error
}

func (conn *bufferedTestConn) Read(data []byte) (int, error) { return conn.Buffer.Read(data) }

func (conn *bufferedTestConn) Write(data []byte) (int, error) {
	conn.writes++
	if conn.failure != nil {
		return 0, conn.failure
	}
	return conn.Buffer.Write(data)
}

func TestWriteMessagesBuffersAndFlushes(t *testing.T) {
	conn := &bufferedTestConn{}
	var network net.Conn = conn
	server := NewPostgresServer(&Config{}, &network)
	var messages []pgproto3.Message
	var expected []byte
	for i := 0; i < 1000; i++ {
		message := &pgproto3.DataRow{Values: [][]byte{[]byte("test row")}}
		messages = append(messages, message)
		var err error
		expected, err = message.Encode(expected)
		if err != nil {
			t.Fatal(err)
		}
	}
	server.writeMessages(messages...)
	if server.writeErr != nil {
		t.Fatal(server.writeErr)
	}
	if !bytes.Equal(conn.Bytes(), expected) {
		t.Fatal("wire bytes changed or were not flushed")
	}
	if conn.writes != 1 {
		t.Fatalf("small response used %d socket writes", conn.writes)
	}
}

func TestWriteMessagesStopsAfterWriteError(t *testing.T) {
	failure := errors.New("connection closed")
	conn := &bufferedTestConn{failure: failure}
	var network net.Conn = conn
	server := NewPostgresServer(&Config{}, &network)
	server.writeMessages(&pgproto3.ParseComplete{})
	server.writeMessages(&pgproto3.BindComplete{})
	if !errors.Is(server.writeErr, failure) {
		t.Fatalf("write error = %v", server.writeErr)
	}
	if conn.writes != 1 {
		t.Fatalf("writes after failure = %d", conn.writes)
	}
}
