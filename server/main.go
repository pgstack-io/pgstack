package main

import (
	"slices"
	"sync/atomic"
)

const (
	COMMAND_START   = "start"
	COMMAND_VERSION = "version"

	DUCKDB_SCHEMA_MAIN = "main"
)

func main() {
	config := LoadConfig()
	defer HandleUnexpectedPanic(config)

	tcpListener := NewTcpListener(config)
	LogInfo(config, "PgStack: Listening on", tcpListener.Addr())
	runtimeManager := &RuntimeManager{}

	var connectionCount int64 = 0
	for {
		conn := AcceptConnection(config, tcpListener)
		atomic.AddInt64(&connectionCount, 1)
		LogInfo(config, "PgStack: Accepted", Int64ToString(atomic.LoadInt64(&connectionCount))+"th", "connection from", conn.RemoteAddr())
		server := NewPostgresServer(config, &conn)

		go func() {
			server.Run(runtimeManager.Acquire)
			defer server.Close()
			LogInfo(config, "PgStack: Closed", Int64ToString(atomic.LoadInt64(&connectionCount))+"th", "connection from", conn.RemoteAddr())
			atomic.AddInt64(&connectionCount, -1)
		}()
	}
}

func duckdbBootQueris(config *Config) []string {
	setupQueries := []string{
		"INSTALL httpfs",
		"LOAD httpfs",
	}
	if config.AuditEnabled {
		setupQueries = append(setupQueries,
			"INSTALL iceberg",
			"LOAD iceberg",
		)
	}
	setupQueries = append(setupQueries,
		// Set up schemas
		"SELECT oid FROM pg_catalog.pg_namespace",
		"CREATE SCHEMA "+PG_SCHEMA_PUBLIC,

		// Configure DuckDB
		"SET memory_limit='"+config.DuckDBMemoryLimit+"'",
		"SET threads=2",
		"SET preserve_insertion_order=false",
		"SET scalar_subquery_error_on_multiple_rows=false",
		// Cache
		"SET parquet_metadata_cache=true",
		"SET enable_external_file_cache=true",
		"SET validate_external_file_cache=VALIDATE_REMOTE",
	)
	queries := slices.Concat(
		setupQueries,

		// Create pg-compatible functions
		CreatePgCatalogMacroQueries(config),
		CreateInformationSchemaMacroQueries(config),

		// Create pg-compatible tables and views
		CreatePgCatalogTableQueries(config),
		CreateInformationSchemaTableQueries(config),

		// Use the public schema
		[]string{"USE " + PG_SCHEMA_PUBLIC},
	)
	if config.SearchTablesJSON != "" && config.SearchTablesJSON != "[]" {
		queries = append(queries,
			"INSTALL lance",
			"LOAD lance",
			duckdbLanceSecretQuery(config),
			"ATTACH 's3://"+config.AwsS3Bucket+"/"+config.SearchBasePath+"' AS search_store (TYPE LANCE, READ_WRITE)",
		)
		queries = append(queries, searchCatalogBootQueries(config)...)
	}
	return queries
}
