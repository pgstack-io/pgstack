package main

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"strings"
	"sync/atomic"

	"github.com/duckdb/duckdb-go/v2"
)

type DuckdbClient struct {
	Config      *Config
	Db          *sql.DB
	Connector   *duckdb.Connector
	BootQueries []string
}

func NewDuckdbClient(config *Config, bootQueries ...[]string) (*DuckdbClient, error) {
	client := &DuckdbClient{Config: config}

	client.BootQueries = []string{
		"SET timezone='UTC'",
	}
	if bootQueries != nil {
		client.BootQueries = append(client.BootQueries, bootQueries[0]...)
	}

	client.BootQueries = append(
		client.BootQueries,
		duckdbS3SecretQuery(config),
	)
	if config.LogLevel == LOG_LEVEL_TRACE {
		client.BootQueries = append(client.BootQueries, "CALL enable_logging('HTTP')", "SET logging_storage = 'stdout'")
	}

	db, connector, err := client.openDatabase()
	if err != nil {
		return nil, err
	}
	client.Db = db
	client.Connector = connector
	return client, nil
}

// openDatabase boots a shared in-memory database, then initializes session
// settings on each new connection. Schemas and secrets are created only once.
func (client *DuckdbClient) openDatabase() (*sql.DB, *duckdb.Connector, error) {
	ctx := context.Background()
	var sessionQueries []string
	for _, query := range client.BootQueries {
		if strings.HasPrefix(query, "SET ") || strings.HasPrefix(query, "USE ") {
			sessionQueries = append(sessionQueries, query)
		}
	}
	var booted atomic.Bool
	connector, err := duckdb.NewConnector("", func(execer driver.ExecerContext) error {
		// The boot connection runs all queries in order. Replaying USE before
		// boot has created its schema would fail.
		if !booted.Load() {
			return nil
		}
		for _, query := range sessionQueries {
			if _, err := execer.ExecContext(ctx, query, nil); err != nil {
				return fmt.Errorf("failed to initialize DuckDB session: %w", err)
			}
		}
		return nil
	})
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create DuckDB connector: %w", err)
	}
	db := sql.OpenDB(connector)
	for _, query := range client.BootQueries {
		if _, err := db.ExecContext(ctx, query); err != nil {
			db.Close()
			connector.Close()
			return nil, nil, fmt.Errorf("failed to execute DuckDB boot query: %w", err)
		}
	}
	booted.Store(true)

	if client.Config.SearchTablesJSON != "" && client.Config.SearchTablesJSON != "[]" {
		// Lance caches dataset handles per connection. The processor commits
		// through a separate process; an early read can pin an empty snapshot
		// even after rows arrive. Closing connections when their results are
		// released makes the next query open the latest committed version.
		// The shared database, catalog, and secrets remain alive.
		db.SetMaxIdleConns(0)
	}
	return db, connector, nil
}

func duckdbS3SecretQuery(config *Config) string {
	// DuckDB adds the protocol to S3 secret endpoints.
	s3Endpoint := strings.TrimPrefix(config.AwsS3Endpoint, "http://")
	s3Endpoint = strings.TrimPrefix(s3Endpoint, "https://")

	options := []string{
		"TYPE S3",
		"KEY_ID '" + config.AwsAccessKeyId + "'",
		"SECRET '" + config.AwsSecretAccessKey + "'",
		"REGION '" + config.AwsRegion + "'",
		"ENDPOINT '" + s3Endpoint + "'",
		"SCOPE 's3://" + config.AwsS3Bucket + "'",
	}
	if IsLocalHost(config.AwsS3Endpoint) {
		options = append(options, "USE_SSL false")
	}
	if config.AwsS3Endpoint != DEFAULT_AWS_S3_ENDPOINT {
		// Use endpoint/bucket/key instead of bucket.endpoint/key.
		options = append(options, "URL_STYLE 'path'")
	}

	return "CREATE OR REPLACE SECRET aws_s3_secret (" + strings.Join(options, ", ") + ")"
}

func duckdbLanceSecretQuery(config *Config) string {
	options := []string{
		"TYPE LANCE",
		"PROVIDER config",
		"SCOPE 's3://" + config.AwsS3Bucket + "/'",
		"ACCESS_KEY_ID '" + config.AwsAccessKeyId + "'",
		"SECRET_ACCESS_KEY '" + config.AwsSecretAccessKey + "'",
		"REGION '" + config.AwsRegion + "'",
	}
	if IsLocalHost(config.AwsS3Endpoint) {
		options = append(options,
			"ENDPOINT '"+config.AwsS3Endpoint+"'",
			"VIRTUAL_HOSTED_STYLE_REQUEST false",
			"ALLOW_HTTP true",
		)
	}
	return "CREATE OR REPLACE SECRET pgstack_lance (" + strings.Join(options, ", ") + ")"
}

func (client *DuckdbClient) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	LogDebug(client.Config, "Querying DuckDB:", query)
	return client.Db.QueryContext(ctx, query, args...)
}

func (client *DuckdbClient) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	LogDebug(client.Config, "Preparing DuckDB statement:", query)
	return client.Db.PrepareContext(ctx, query)
}

func (client *DuckdbClient) ExecContext(ctx context.Context, query string, args ...map[string]string) (sql.Result, error) {
	LogDebug(client.Config, "Executing DuckDB:", query)
	if len(args) == 0 {
		return client.Db.ExecContext(ctx, query)
	}

	return client.Db.ExecContext(ctx, replaceNamedStringArgs(query, args[0]))
}

func (client *DuckdbClient) ExecTransactionContext(ctx context.Context, queries []string, args ...[]map[string]string) error {
	tx, err := client.Db.Begin()
	LogDebug(client.Config, "Executing DuckDB: BEGIN")
	if err != nil {
		return err
	}

	for i, query := range queries {
		LogDebug(client.Config, "Executing DuckDB in transaction:", query)
		var err error
		if len(args) == 0 {
			_, err = tx.ExecContext(ctx, query)
		} else {
			_, err = tx.ExecContext(ctx, replaceNamedStringArgs(query, args[0][i]))
		}
		if err != nil {
			tx.Rollback()
			return err
		}
	}

	LogDebug(client.Config, "Executing DuckDB: COMMIT")
	return tx.Commit()
}

func (client *DuckdbClient) Close() {
	client.Db.Close()
	client.Connector.Close()
}

func (client *DuckdbClient) RecreateDb() error {
	db, connector, err := client.openDatabase()
	if err != nil {
		return fmt.Errorf("failed to recreate DuckDB: %w", err)
	}

	client.Db.Close()
	client.Connector.Close()
	client.Db = db
	client.Connector = connector
	return nil
}

func replaceNamedStringArgs(query string, args map[string]string) string {
	for key, value := range args {
		query = strings.ReplaceAll(
			query,
			"$"+key,
			strings.ReplaceAll(value, "'", "''"),
		)
	}
	return query
}
