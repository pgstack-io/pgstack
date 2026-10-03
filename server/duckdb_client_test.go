package main

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
)

func TestSearchReaderSeesExternalCommits(t *testing.T) {
	ctx := context.Background()
	writer, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	if _, err := writer.ExecContext(ctx, "LOAD lance"); err != nil {
		t.Skipf("Lance extension unavailable: %v", err)
	}
	root := strings.ReplaceAll(t.TempDir(), "'", "''")
	for _, query := range []string{
		fmt.Sprintf("ATTACH '%s' AS search_store (TYPE LANCE, READ_WRITE)", root),
		"CREATE TABLE search_store.main.products AS SELECT 'before' AS name",
	} {
		if _, err := writer.ExecContext(ctx, query); err != nil {
			t.Fatal(err)
		}
	}
	reader, err := NewDuckdbClient(&Config{
		AwsRegion: "us-east-1", AwsS3Bucket: "test", AwsS3Endpoint: DEFAULT_AWS_S3_ENDPOINT,
		SearchTablesJSON: `[{"name":"public.products","indexColumns":["name"],"storeColumns":["name"]}]`,
	}, []string{"LOAD lance", "CREATE SCHEMA public", "USE public"})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	check := func(want string) {
		t.Helper()
		var name, schema, timezone string
		query := fmt.Sprintf("SELECT name, current_schema(), current_setting('TimeZone') FROM '%s/products.lance'", root)
		if err := reader.Db.QueryRowContext(ctx, query).Scan(&name, &schema, &timezone); err != nil {
			t.Fatal(err)
		}
		if name != want || schema != "public" || timezone != "UTC" {
			t.Fatalf("got (%q, %q, %q), want (%q, public, UTC)", name, schema, timezone, want)
		}
	}
	check("before")
	if _, err := writer.ExecContext(ctx, "UPDATE search_store.main.products SET name = 'after'"); err != nil {
		t.Fatal(err)
	}
	check("after")
	if err := reader.RecreateDb(); err != nil {
		t.Fatal(err)
	}
	check("after")
	if _, err := writer.ExecContext(ctx, "UPDATE search_store.main.products SET name = 'latest'"); err != nil {
		t.Fatal(err)
	}
	check("latest")
}

func TestDuckdbS3SecretQueryConfiguresLocalEndpoint(t *testing.T) {
	query := duckdbS3SecretQuery(&Config{
		AwsRegion:          "us-east-1",
		AwsS3Endpoint:      "http://127.0.0.1:9000",
		AwsS3Bucket:        "pgstack",
		AwsAccessKeyId:     "pgstack",
		AwsSecretAccessKey: "pgstack",
	})

	for _, expected := range []string{
		"ENDPOINT '127.0.0.1:9000'",
		"USE_SSL false",
		"URL_STYLE 'path'",
	} {
		if !strings.Contains(query, expected) {
			t.Errorf("local S3 secret query does not contain %q: %s", expected, query)
		}
	}
}

func TestDuckdbS3SecretQueryUsesAwsDefaults(t *testing.T) {
	query := duckdbS3SecretQuery(&Config{
		AwsRegion:      "us-east-1",
		AwsS3Endpoint:  DEFAULT_AWS_S3_ENDPOINT,
		AwsS3Bucket:    "pgstack",
		AwsAccessKeyId: "key",
	})

	for _, unexpected := range []string{
		"USE_SSL",
		"URL_STYLE",
	} {
		if strings.Contains(query, unexpected) {
			t.Errorf("AWS S3 secret query unexpectedly contains %q: %s", unexpected, query)
		}
	}
}
