package main

import (
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestLoadConfigDefaultsLogLevel(t *testing.T) {
	t.Setenv("LOG_LEVEL", "")

	config := LoadConfig()

	if config.LogLevel != LOG_LEVEL_INFO {
		t.Fatalf("expected default log level %q, got %q", LOG_LEVEL_INFO, config.LogLevel)
	}
}

func TestLoadConfigReadsLogLevelFromEnv(t *testing.T) {
	t.Setenv("LOG_LEVEL", LOG_LEVEL_DEBUG)

	config := LoadConfig()

	if config.LogLevel != LOG_LEVEL_DEBUG {
		t.Fatalf("expected env log level %q, got %q", LOG_LEVEL_DEBUG, config.LogLevel)
	}
}

func TestNormalizeDatabaseURLEscapesUserInfo(t *testing.T) {
	raw := "postgres://user:go12^$@db-prod.slug.us-east-1.rds.amazonaws.com:5432/database"

	got := normalizeDatabaseURL(raw)
	want := "postgres://user:go12%5E$@db-prod.slug.us-east-1.rds.amazonaws.com:5432/database"

	if got != want {
		t.Fatalf("normalized URL mismatch:\nwant: %s\ngot:  %s", want, got)
	}

	if _, err := pgconn.ParseConfig(got); err != nil {
		t.Fatalf("normalized URL should parse: %v", err)
	}
}

func TestNormalizeDatabaseURLLeavesKeywordDSNUntouched(t *testing.T) {
	raw := "host=localhost port=5432 user=postgres password=go$1^3..."

	got := normalizeDatabaseURL(raw)

	if got != raw {
		t.Fatalf("expected keyword DSN to remain unchanged:\nwant: %s\ngot:  %s", raw, got)
	}
}

func TestShouldProcessUnionOfAuditAndSearchTables(t *testing.T) {
	config := &Config{
		AuditConfigured: true,
		IncludedTables:  []string{"public.audit_events"},
		SearchTables:    []string{"public.documents"},
	}

	if !config.ShouldProcessTable("public", "audit_events") {
		t.Fatal("expected Audit table to be processed")
	}
	if !config.ShouldProcessTable("public", "documents") {
		t.Fatal("expected Search table to be processed")
	}
	if config.ShouldProcessTable("public", "unused") {
		t.Fatal("expected unused table to be discarded")
	}
}

func TestSearchTablesStillProcessWhenAuditDisabled(t *testing.T) {
	config := &Config{AuditConfigured: false, SearchTables: []string{"public.documents"}}
	if config.ShouldAuditTable("public", "documents") {
		t.Fatal("expected Audit routing to be disabled")
	}
	if !config.ShouldProcessTable("public", "documents") {
		t.Fatal("expected Search routing to remain enabled")
	}
}

func TestLoadConfigDerivesAuditFromFilterPresence(t *testing.T) {
	t.Setenv("AUDIT_EXCLUDED_TABLES", "")
	config := LoadConfig()
	if !config.AuditConfigured {
		t.Fatal("expected an explicitly empty exclusion list to enable Audit for all tables")
	}
	if !config.ShouldAuditTable("public", "documents") {
		t.Fatal("expected Audit to include tables when no exclusions are configured")
	}
}
