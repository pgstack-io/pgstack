package common

import (
	"context"
	"database/sql"
	"strings"

	"github.com/marcboeker/go-duckdb/v2"
)

var SYNCER_DUCKDB_BOOT_QUERIES = []string{
	"SET memory_limit='2GB'",
	"SET threads=2",
}

type DuckdbClient struct {
	Config      *CommonConfig
	Db          *sql.DB
	Connector   *duckdb.Connector
	BootQueries []string
}

func NewDuckdbClient(config *CommonConfig, bootQueries ...[]string) *DuckdbClient {
	ctx := context.Background()
	connector, err := duckdb.NewConnector("", nil)
	PanicIfError(config, err)
	db := sql.OpenDB(connector)

	client := &DuckdbClient{
		Config:    config,
		Db:        db,
		Connector: connector,
	}

	client.BootQueries = []string{
		"SET timezone='UTC'",
	}
	if bootQueries != nil {
		client.BootQueries = append(client.BootQueries, bootQueries[0]...)
	}
	client.BootQueries = append(
		client.BootQueries,
		"CREATE OR REPLACE SECRET aws_s3_secret (TYPE S3, KEY_ID '"+config.Aws.AccessKeyId+"', SECRET '"+config.Aws.SecretAccessKey+"', REGION '"+config.Aws.Region+"', ENDPOINT '"+config.Aws.S3Endpoint+"', SCOPE 's3://"+config.Aws.S3Bucket+"')",
	)
	if IsLocalHost(config.Aws.S3Endpoint) {
		client.BootQueries = append(client.BootQueries, "SET s3_use_ssl=false")
	}
	if config.Aws.S3Endpoint != DEFAULT_AWS_S3_ENDPOINT {
		client.BootQueries = append(client.BootQueries, "SET s3_url_style='path'") // Use endpoint/bucket/key (path, deprecated on AWS) instead of bucket.endpoint/key (vhost)
	}
	if config.LogLevel == LOG_LEVEL_TRACE {
		client.BootQueries = append(client.BootQueries, "PRAGMA enable_logging('HTTP')", "SET logging_storage = 'stdout'")
	}

	for _, query := range client.BootQueries {
		_, err := client.ExecContext(ctx, query)
		PanicIfError(config, err)
	}

	return client
}

func (client *DuckdbClient) QueryContext(ctx context.Context, query string) (*sql.Rows, error) {
	LogDebug(client.Config, "Querying DuckDB:", query)
	return client.Db.QueryContext(ctx, query)
}

func (client *DuckdbClient) QueryRowContext(ctx context.Context, query string, args ...map[string]string) *sql.Row {
	LogDebug(client.Config, "Querying DuckDB row:", query)
	if len(args) == 0 {
		return client.Db.QueryRowContext(ctx, query)
	}
	return client.Db.QueryRowContext(ctx, replaceNamedStringArgs(query, args[0]))
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

func (client *DuckdbClient) Appender(schema string, table string) (*duckdb.Appender, error) {
	conn, err := client.Connector.Connect(context.Background())
	if err != nil {
		return nil, err
	}
	return duckdb.NewAppenderFromConn(conn, schema, table)
}

func (client *DuckdbClient) Close() {
	client.Db.Close()
}

func (client *DuckdbClient) RecreateDb() {
	ctx := context.Background()

	client.Db.Close()

	connector, err := duckdb.NewConnector("", nil)
	PanicIfError(client.Config, err)
	db := sql.OpenDB(connector)
	client.Db = db
	client.Connector = connector

	for _, query := range client.BootQueries {
		_, err := client.Db.ExecContext(ctx, query)
		PanicIfError(client.Config, err)
	}

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
