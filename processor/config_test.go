package main

import (
	"testing"
	"time"
)

func TestLoadConfigUsesResolvedStorage(t *testing.T) {
	t.Setenv("AWS_S3_BUCKET", "selected")
	t.Setenv("AUDIT_BASE_PATH", "org/project/audit")
	t.Setenv("SEARCH_BASE_PATH", "")
	t.Setenv("MAX_HOT_PARQUET_SIZE_MB", "96")
	t.Setenv("DUCKDB_MEMORY_LIMIT", "1GB")
	t.Setenv("NATS_BATCH_INTERVAL_SEC", "300")
	config := LoadConfig()
	if config.S3Bucket != "selected" || config.AuditBasePath != "org/project/audit" || config.SearchBasePath != "org/project/search" {
		t.Fatalf("incorrect resolved storage: %#v", config)
	}
	if config.NatsBatchInterval != 300*time.Second {
		t.Fatal("batch interval not preserved")
	}
}
