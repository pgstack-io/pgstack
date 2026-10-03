package main

import (
	"errors"
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
