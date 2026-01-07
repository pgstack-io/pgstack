package main

import (
	"log"
	"net/http"
	_ "net/http/pprof"
	"slices"
	"sync/atomic"

	"github.com/BemiHQ/BemiDB/src/common"
)

const (
	COMMAND_START   = "start"
	COMMAND_VERSION = "version"

	DUCKDB_SCHEMA_MAIN = "main"
)

func main() {
	config := LoadConfig()
	defer common.HandleUnexpectedPanic(config.CommonConfig)

	if config.CommonConfig.LogLevel == common.LOG_LEVEL_TRACE {
		go enableProfiling()
	}

	tcpListener := NewTcpListener(config)
	common.LogInfo(config.CommonConfig, "BemiDB: Listening on", tcpListener.Addr())

	duckdbClient := common.NewDuckdbClient(config.CommonConfig, duckdbBootQueris(config))
	common.LogInfo(config.CommonConfig, "DuckDB: Connected")
	defer duckdbClient.Close()

	queryHandler := NewQueryHandler(config, duckdbClient)

	var connectionCount int64 = 0
	for {
		conn := AcceptConnection(config, tcpListener)
		atomic.AddInt64(&connectionCount, 1)
		common.LogInfo(config.CommonConfig, "BemiDB: Accepted", common.Int64ToString(atomic.LoadInt64(&connectionCount))+"th", "connection from", conn.RemoteAddr())
		server := NewPostgresServer(config, &conn)

		go func() {
			server.Run(queryHandler)
			defer server.Close()
			common.LogInfo(config.CommonConfig, "BemiDB: Closed", common.Int64ToString(atomic.LoadInt64(&connectionCount))+"th", "connection from", conn.RemoteAddr())
			atomic.AddInt64(&connectionCount, -1)
		}()
	}
}

func duckdbBootQueris(config *Config) []string {
	return slices.Concat(
		[]string{
			// Set up Iceberg
			"INSTALL iceberg",
			"LOAD iceberg",
			// Set up vector similarity search
			"INSTALL vss",
			"LOAD vss",

			// Set up schemas
			"SELECT oid FROM pg_catalog.pg_namespace",
			"CREATE SCHEMA " + PG_SCHEMA_PUBLIC,

			// Configure DuckDB
			"SET memory_limit='3GB'",
			"SET threads=2",
			"SET scalar_subquery_error_on_multiple_rows=false",
		},

		// Create pg-compatible functions
		CreatePgCatalogMacroQueries(config),
		CreateInformationSchemaMacroQueries(config),

		// Create pg-compatible tables and views
		CreatePgCatalogTableQueries(config),
		CreateInformationSchemaTableQueries(config),

		// Use the public schema
		[]string{"USE " + PG_SCHEMA_PUBLIC},
	)
}

func enableProfiling() {
	func() { log.Println(http.ListenAndServe(":6060", nil)) }()
}
