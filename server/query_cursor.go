package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/uuid"
)

const QUERY_RESULT_BATCH_SIZE = 1024
const QUERY_CLEANUP_TIMEOUT = 5 * time.Second

// QueryCursor stores one execution on disk. The database/sql DuckDB driver
// materializes query results, so keeping sql.Rows open does not bound memory.
// CTAS writes through DuckDB's buffer manager; only bounded reads are returned
// through the driver. An ordinal preserves order and permits range scans.
type QueryCursor struct {
	conn      *sql.Conn
	db        *sql.DB
	attached  bool
	directory string
	alias     string
	ordinal   string
	position  int64
}

func (queryHandler *QueryHandler) newQueryCursor(ctx context.Context, statement *Portal) (_ *QueryCursor, err error) {
	directory, err := os.MkdirTemp("", "pgstack-cursor-")
	if err != nil {
		return nil, err
	}
	cursor := &QueryCursor{db: queryHandler.ServerDuckdbClient.Db, directory: directory, alias: "cursor_" + uuid.NewString(), ordinal: "ordinal_" + uuid.NewString()}
	defer func() {
		if err != nil {
			err = errors.Join(err, cursor.Close())
		}
	}()
	cursor.conn, err = queryHandler.ServerDuckdbClient.Db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	path := filepath.Join(directory, "result.duckdb")
	cursor.attached = true // ATTACH may commit just before its context is canceled.
	_, err = cursor.conn.ExecContext(ctx, "ATTACH '"+strings.ReplaceAll(path, "'", "''")+"' AS "+quoteCursorIdentifier(cursor.alias)+" (ROW_GROUP_SIZE 2048)")
	if err != nil {
		return nil, err
	}
	query := "CREATE TABLE " + cursor.table() + " AS SELECT row_number() OVER () AS " + quoteCursorIdentifier(cursor.ordinal) + ", * FROM (" + statement.Query + ") AS source"
	if _, err = cursor.conn.ExecContext(ctx, query, statement.Variables...); err != nil {
		return nil, err
	}
	return cursor, nil
}

func (cursor *QueryCursor) table() string {
	return quoteCursorIdentifier(cursor.alias) + ".main.result"
}

func (cursor *QueryCursor) ReadBatch(ctx context.Context, count uint32) (*sql.Rows, error) {
	query := fmt.Sprintf("SELECT * EXCLUDE (%s) FROM %s WHERE %s > $1 AND %s <= $2 ORDER BY %s",
		quoteCursorIdentifier(cursor.ordinal), cursor.table(), quoteCursorIdentifier(cursor.ordinal), quoteCursorIdentifier(cursor.ordinal), quoteCursorIdentifier(cursor.ordinal))
	rows, err := cursor.conn.QueryContext(ctx, query, cursor.position, cursor.position+int64(count))
	if err == nil {
		cursor.position += int64(count)
	}
	return rows, err
}

func (cursor *QueryCursor) Close() error {
	// Cleanup must survive cancellation of the request that owned the cursor.
	ctx, cancel := context.WithTimeout(context.Background(), QUERY_CLEANUP_TIMEOUT)
	defer cancel()
	var err error
	if cursor.attached {
		queryRow := cursor.db.QueryRowContext
		execute := cursor.db.ExecContext
		if cursor.conn != nil {
			queryRow = cursor.conn.QueryRowContext
			execute = cursor.conn.ExecContext
		}
		var exists bool
		err = queryRow(ctx, "SELECT EXISTS (SELECT 1 FROM duckdb_databases() WHERE database_name = $1)", cursor.alias).Scan(&exists)
		if err == nil && exists {
			_, err = execute(ctx, "DETACH "+quoteCursorIdentifier(cursor.alias))
		}

		if err == nil {
			cursor.attached = false
		} else {
			err = fmt.Errorf("detach cursor database: %w", err)
		}
	}
	if cursor.conn != nil {
		err = errors.Join(err, cursor.conn.Close())
		cursor.conn = nil
	}
	// Do not unlink a database that DuckDB still has attached.
	if !cursor.attached && cursor.directory != "" {
		if removeErr := os.RemoveAll(cursor.directory); removeErr != nil {
			err = errors.Join(err, fmt.Errorf("remove cursor directory: %w", removeErr))
		} else {
			cursor.directory = ""
		}
	}
	return err
}

func quoteCursorIdentifier(value string) string {
	return `"` + strings.ReplaceAll(value, `"`, `""`) + `"`
}
