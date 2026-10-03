package main

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"strings"

	"github.com/duckdb/duckdb-go/v2"
)

type DuckDBClient struct {
	db          *sql.DB
	connector   *duckdb.Connector
	config      *Config
	bootQueries []string
}

func NewDuckDBClient(config *Config) (*DuckDBClient, error) {
	client := &DuckDBClient{
		config: config,
	}
	client.bootQueries = client.buildBootQueries()

	connector, err := duckdb.NewConnector("", func(execer driver.ExecerContext) error {
		return client.executeBootQueries(context.Background(), execer)
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create duckdb connector: %w", err)
	}

	client.connector = connector
	client.db = sql.OpenDB(connector)
	client.db.SetMaxOpenConns(1)

	if err := client.db.PingContext(context.Background()); err != nil {
		client.db.Close()
		client.connector.Close()
		return nil, fmt.Errorf("failed to ping duckdb: %w", err)
	}

	return client, nil
}

func (d *DuckDBClient) buildBootQueries() []string {
	// Strip protocol from endpoint for DuckDB
	endpoint := d.config.S3Endpoint
	endpoint = strings.TrimPrefix(endpoint, "http://")
	endpoint = strings.TrimPrefix(endpoint, "https://")

	queries := []string{
		"INSTALL httpfs",
		"LOAD httpfs",
		"SET timezone='UTC'",
		fmt.Sprintf("SET memory_limit='%s'", d.config.DuckDBMemoryLimit),
		"SET threads=1",
		"SET preserve_insertion_order=false",
		fmt.Sprintf("SET s3_endpoint='%s'", endpoint),
		fmt.Sprintf("SET s3_access_key_id='%s'", d.config.S3AccessKey),
		fmt.Sprintf("SET s3_secret_access_key='%s'", d.config.S3SecretKey),
		fmt.Sprintf("SET s3_region='%s'", d.config.S3Region),
		"SET s3_use_ssl=false",
		"SET s3_url_style='path'",
	}
	if d.config.SearchTablesJSON != "" && d.config.SearchTablesJSON != "[]" {
		lanceSecretOptions := []string{
			"TYPE LANCE",
			"PROVIDER config",
			fmt.Sprintf("SCOPE 's3://%s/'", escapeSQLString(d.config.S3Bucket)),
			fmt.Sprintf("ACCESS_KEY_ID '%s'", escapeSQLString(d.config.S3AccessKey)),
			fmt.Sprintf("SECRET_ACCESS_KEY '%s'", escapeSQLString(d.config.S3SecretKey)),
			fmt.Sprintf("REGION '%s'", escapeSQLString(d.config.S3Region)),
		}
		if strings.HasPrefix(d.config.S3Endpoint, "http://") || strings.HasPrefix(d.config.S3Endpoint, "https://") {
			lanceSecretOptions = append(lanceSecretOptions,
				fmt.Sprintf("ENDPOINT '%s'", escapeSQLString(d.config.S3Endpoint)),
				"VIRTUAL_HOSTED_STYLE_REQUEST false",
			)
			if strings.HasPrefix(d.config.S3Endpoint, "http://") {
				lanceSecretOptions = append(lanceSecretOptions, "ALLOW_HTTP true")
			}
		}
		queries = append(queries,
			"INSTALL lance",
			"LOAD lance",
			"CREATE OR REPLACE SECRET pgstack_lance ("+strings.Join(lanceSecretOptions, ", ")+")",
			fmt.Sprintf("ATTACH 's3://%s/%s' AS search_store (TYPE LANCE, READ_WRITE)", escapeSQLString(d.config.S3Bucket), escapeSQLString(d.config.SearchBasePath)),
		)
	}
	return queries
}

func escapeSQLString(value string) string {
	return strings.ReplaceAll(value, "'", "''")
}

func (d *DuckDBClient) executeBootQueries(ctx context.Context, execer driver.ExecerContext) error {
	for _, query := range d.bootQueries {
		if _, err := execer.ExecContext(ctx, query, nil); err != nil {
			return fmt.Errorf("failed to execute %q: %w", query, err)
		}
	}

	return nil
}

func (d *DuckDBClient) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	return d.db.ExecContext(ctx, query, args...)
}

func (d *DuckDBClient) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	return d.db.QueryContext(ctx, query, args...)
}

func (d *DuckDBClient) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	return d.db.QueryRowContext(ctx, query, args...)
}

func (d *DuckDBClient) Close() error {
	dbErr := d.db.Close()
	connectorErr := d.connector.Close()
	return errors.Join(dbErr, connectorErr)
}

func isDuckDBOutOfMemory(err error) bool {
	var duckDBError *duckdb.Error
	return errors.As(err, &duckDBError) && duckDBError.Type == duckdb.ErrorTypeOutOfMemory
}
