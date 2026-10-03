package main

import (
	"errors"
	"fmt"
	"slices"
	"testing"

	"github.com/duckdb/duckdb-go/v2"
)

func TestIsDuckDBOutOfMemory(t *testing.T) {
	err := fmt.Errorf("failed to write parquet: %w", &duckdb.Error{
		Type: duckdb.ErrorTypeOutOfMemory,
		Msg:  "Out of Memory Error: failed to allocate data",
	})

	if !isDuckDBOutOfMemory(err) {
		t.Fatal("expected wrapped DuckDB out-of-memory error to be detected")
	}
}

func TestIsDuckDBOutOfMemoryRejectsOtherErrors(t *testing.T) {
	tests := []error{
		errors.New("Out of Memory Error: text alone should not match"),
		&duckdb.Error{Type: duckdb.ErrorTypeIO, Msg: "IO Error"},
	}

	for _, err := range tests {
		if isDuckDBOutOfMemory(err) {
			t.Fatalf("did not expect %T to be detected as DuckDB out-of-memory", err)
		}
	}
}

func TestDuckDBBootQueriesAttachSearchReadWrite(t *testing.T) {
	client := &DuckDBClient{config: &Config{
		S3Bucket:         "bucket",
		SearchBasePath:   "project/search",
		SearchTablesJSON: `[{"name":"public.documents"}]`,
	}}
	query := "ATTACH 's3://bucket/project/search' AS search_store (TYPE LANCE, READ_WRITE)"
	if !slices.Contains(client.buildBootQueries(), query) {
		t.Fatalf("Search boot queries do not contain %q", query)
	}
}
