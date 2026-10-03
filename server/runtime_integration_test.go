package main

import (
	"strings"
	"testing"
)

func configureStandaloneTest(t *testing.T) {
	t.Setenv("PGSTACK_AUTH_MODE", "proxy")
}

func TestStandaloneLocalPasswordOptional(t *testing.T) {
	configureStandaloneTest(t)
	validateRuntimeConfig(&Config{})
	server := &PostgresServer{config: &Config{}}
	if err := server.authenticateStartup("reader"); err != nil {
		t.Fatal(err)
	}
}

func TestStandaloneCatalogContainsNoPassword(t *testing.T) {
	query := passwordCatalogQuery(&Config{User: "reader", EncryptedPassword: "secret-verifier"})
	if strings.Contains(query, "secret-verifier") || !strings.Contains(query, "WHERE FALSE") {
		t.Fatal("password catalog must be empty")
	}
}
