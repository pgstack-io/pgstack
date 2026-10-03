package main

import (
	"errors"
	"net"
	"testing"

	"github.com/jackc/pgx/v5/pgproto3"
)

func TestClientErrorKeepsPrivateCauseForLogging(t *testing.T) {
	cause := errors.New("private s3://bucket/path/file.parquet error")
	err := NewClientError(PG_ERROR_SEVERITY_ERROR, PG_ERROR_CODE_INTERNAL_ERROR, "safe message", cause)

	if err.Error() != cause.Error() {
		t.Fatalf("logged error = %q, want %q", err.Error(), cause.Error())
	}
	if !errors.Is(err, cause) {
		t.Fatal("client error does not unwrap to its private cause")
	}
}

func TestWriteErrorUsesExplicitClientError(t *testing.T) {
	clientError := NewClientError(
		PG_ERROR_SEVERITY_FATAL,
		PG_ERROR_CODE_INVALID_AUTHORIZATION_SPECIFICATION,
		`role "postgres" does not exist`,
		errors.New("private authentication details"),
	)

	response := writeAndReadErrorResponse(t, clientError, false)
	if response.Severity != clientError.Severity || response.Code != clientError.Code || response.Message != clientError.Message {
		t.Fatalf("response = %+v, want severity=%s code=%s message=%q", response, clientError.Severity, clientError.Code, clientError.Message)
	}
}

func TestWriteErrorHidesInternalError(t *testing.T) {
	response := writeAndReadErrorResponse(t, errors.New("private s3://bucket/path/file.parquet error"), true)
	if response.Severity != PG_ERROR_SEVERITY_ERROR || response.Code != PG_ERROR_CODE_INTERNAL_ERROR || response.Message != CLIENT_ERROR_MESSAGE {
		t.Fatalf("response = %+v, want generic internal error", response)
	}
}

func writeAndReadErrorResponse(t *testing.T, err error, expectReadyForQuery bool) *pgproto3.ErrorResponse {
	t.Helper()
	serverConn, clientConn := net.Pipe()
	t.Cleanup(func() {
		serverConn.Close()
		clientConn.Close()
	})
	server := NewPostgresServer(&Config{}, &serverConn)
	written := make(chan struct{})
	go func() {
		server.writeError(err)
		close(written)
	}()

	frontend := pgproto3.NewFrontend(clientConn, clientConn)
	message, receiveErr := frontend.Receive()
	if receiveErr != nil {
		t.Fatalf("receive error response: %v", receiveErr)
	}
	response, ok := message.(*pgproto3.ErrorResponse)
	if !ok {
		t.Fatalf("message type = %T, want *pgproto3.ErrorResponse", message)
	}
	if expectReadyForQuery {
		if _, receiveErr := frontend.Receive(); receiveErr != nil {
			t.Fatalf("receive ready-for-query response: %v", receiveErr)
		}
	}
	<-written
	return response
}
