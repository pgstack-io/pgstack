package main

import (
	"slices"
	"testing"
)

func TestDuckdbBootQueriesEnableExternalFileCaches(t *testing.T) {
	queries := duckdbBootQueris(&Config{})

	for _, query := range []string{
		"SET parquet_metadata_cache=true",
		"SET enable_external_file_cache=true",
	} {
		if !slices.Contains(queries, query) {
			t.Fatalf("boot queries do not contain %q", query)
		}
	}
}

func TestDuckdbBootQueriesLoadIcebergOnlyForAudit(t *testing.T) {
	withoutAudit := duckdbBootQueris(&Config{AuditEnabled: false})
	for _, query := range []string{"INSTALL iceberg", "LOAD iceberg"} {
		if slices.Contains(withoutAudit, query) {
			t.Fatalf("Search-only boot queries contain %q", query)
		}
	}

	withAudit := duckdbBootQueris(&Config{AuditEnabled: true})
	for _, query := range []string{"INSTALL iceberg", "LOAD iceberg"} {
		if !slices.Contains(withAudit, query) {
			t.Fatalf("Audit boot queries do not contain %q", query)
		}
	}
}

func TestDuckdbBootQueriesAttachSearchReadWrite(t *testing.T) {
	queries := duckdbBootQueris(&Config{
		AwsS3Bucket:      "bucket",
		SearchBasePath:   "project/search",
		SearchTablesJSON: `[{"name":"public.documents","storeColumns":["id","body"],"indexColumns":["body"]}]`,
	})
	for _, query := range []string{
		"ATTACH 's3://bucket/project/search' AS search_store (TYPE LANCE, READ_WRITE)",
		"CREATE SCHEMA search",
		`CREATE TABLE search."public_documents" ("id" VARCHAR, "body" VARCHAR)`,
	} {
		if !slices.Contains(queries, query) {
			t.Fatalf("Search boot queries do not contain %q", query)
		}
	}
}
