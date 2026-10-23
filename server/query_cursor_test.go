package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/jackc/pgx/v5/pgproto3"
)

func TestCursorKeepsSnapshotAndOrder(t *testing.T) {
	handler := initQueryHandlerForTest(t)
	ctx := context.Background()
	if _, err := handler.ServerDuckdbClient.ExecContext(ctx, "CREATE TABLE cursor_source AS SELECT i FROM range(8000) t(i)"); err != nil {
		t.Fatal(err)
	}
	_, statement, err := handler.HandleParseQuery(context.Background(), &pgproto3.Parse{Query: "SELECT i, i::HUGEINT AS huge, NULL::text AS empty FROM cursor_source ORDER BY i DESC"})
	if err != nil {
		t.Fatal(err)
	}

	_, portal1, err := handler.HandleBindQuery(context.Background(), &pgproto3.Bind{DestinationPortal: "C_1"}, statement)
	defer portal1.Close()
	if err != nil {
		t.Fatal(err)
	}
	// Metadata inspection must not evaluate the result or create its disk spool.
	_, err = handler.DescribePortal(context.Background(), &pgproto3.Describe{ObjectType: 'P', Name: "C_1"}, portal1)
	if err != nil {
		t.Fatal(err)
	}
	if portal1.Reader != nil {
		t.Fatal("Describe executed query")
	}
	var directory string
	for batch, expected := range []int{3677, 3677, 646} {
		messages, err := handler.HandleExecuteQuery(context.Background(), &pgproto3.Execute{Portal: "C_1", MaxRows: 3677}, portal1)
		if err != nil {
			t.Fatal(err)
		}
		if len(messages) != expected+1 {
			t.Fatalf("batch %d: %d messages", batch, len(messages))
		}
		for i := 0; i < expected; i++ {
			value := fmt.Sprint(7999 - batch*3677 - i)
			row := messages[i].(*pgproto3.DataRow)
			if string(row.Values[0]) != value || string(row.Values[1]) != value || row.Values[2] != nil {
				t.Fatalf("unexpected row: %q", row.Values)
			}
		}
		if batch < 2 {
			if _, ok := messages[expected].(*pgproto3.PortalSuspended); !ok {
				t.Fatalf("expected suspension, got %T", messages[expected])
			}
			directory = portal1.Reader.(*QueryCursor).directory
		} else {
			tag := messages[expected].(*pgproto3.CommandComplete)
			if string(tag.CommandTag) != "SELECT 8000" {
				t.Fatalf("tag %q", tag.CommandTag)
			}
		}
		if batch == 0 {
			if _, err := handler.ServerDuckdbClient.ExecContext(ctx, "DELETE FROM cursor_source"); err != nil {
				t.Fatal(err)
			}
		}
	}
	if _, err := os.Stat(directory); !os.IsNotExist(err) {
		t.Fatalf("cursor directory retained: %v", err)
	}
}

func TestCursorDescriptionDoesNotEvaluateRows(t *testing.T) {
	handler := initQueryHandlerForTest(t)
	_, statement, err := handler.HandleParseQuery(context.Background(), &pgproto3.Parse{Query: "SELECT CAST('bad' AS INTEGER) AS invalid"})
	if err != nil {
		t.Fatal(err)
	}
	_, portal2, err := handler.HandleBindQuery(context.Background(), &pgproto3.Bind{}, statement)
	defer portal2.Close()
	if err != nil {
		t.Fatal(err)
	}
	if _, err = handler.DescribePortal(context.Background(), &pgproto3.Describe{ObjectType: 'P'}, portal2); err != nil {
		t.Fatal(err)
	}
	if _, err = handler.HandleExecuteQuery(context.Background(), &pgproto3.Execute{MaxRows: 1}, portal2); err == nil {
		t.Fatal("execution should fail")
	}
}

func TestOrdinaryPreparedSelectDoesNotSpool(t *testing.T) {
	handler := initQueryHandlerForTest(t)
	// An unwritable/nonexistent TMPDIR would make cursor creation fail.
	t.Setenv("TMPDIR", t.TempDir()+"/missing")
	_, statement, err := handler.HandleParseQuery(context.Background(), &pgproto3.Parse{Query: "SELECT i FROM generate_series(0,2047) t(i) ORDER BY i"})
	if err != nil {
		t.Fatal(err)
	}
	_, portal3, err := handler.HandleBindQuery(context.Background(), &pgproto3.Bind{}, statement)
	defer portal3.Close()
	if err != nil {
		t.Fatal(err)
	}
	messages, err := handler.HandleExecuteQuery(context.Background(), &pgproto3.Execute{}, portal3)
	if err != nil {
		t.Fatal(err)
	}
	if portal3.Reader != nil {
		t.Fatal("ordinary query created a cursor")
	}
	if len(messages) != 2049 {
		t.Fatalf("messages = %d", len(messages))
	}
	if _, ok := messages[2048].(*pgproto3.CommandComplete); !ok {
		t.Fatalf("unexpected terminal message %T", messages[2048])
	}
}

func TestCursorExactBoundaryCompletesOnNextFetch(t *testing.T) {
	handler := initQueryHandlerForTest(t)
	_, statement, err := handler.HandleParseQuery(context.Background(), &pgproto3.Parse{Query: "SELECT i FROM generate_series(0,2047) t(i) ORDER BY i"})
	if err != nil {
		t.Fatal(err)
	}

	_, portal4, err := handler.HandleBindQuery(context.Background(), &pgproto3.Bind{}, statement)
	defer portal4.Close()
	if err != nil {
		t.Fatal(err)
	}
	messages, err := handler.HandleExecuteQuery(context.Background(), &pgproto3.Execute{MaxRows: 2048}, portal4)
	if err != nil {
		t.Fatal(err)
	}
	if len(messages) != 2049 {
		t.Fatalf("messages = %d", len(messages))
	}
	if _, ok := messages[2048].(*pgproto3.PortalSuspended); !ok {
		t.Fatalf("expected suspension at exact boundary, got %T", messages[2048])
	}
	directory := portal4.Reader.(*QueryCursor).directory
	// Fetch-all must finish the existing cursor rather than rerunning the SELECT.
	messages, err = handler.HandleExecuteQuery(context.Background(), &pgproto3.Execute{}, portal4)
	if err != nil {
		t.Fatal(err)
	}
	assertMessageTypes(t, messages, "*pgproto3.CommandComplete")
	if string(messages[0].(*pgproto3.CommandComplete).CommandTag) != "SELECT 2048" {
		t.Fatal("wrong completed row count")
	}
	if _, err := os.Stat(directory); !os.IsNotExist(err) {
		t.Fatalf("cursor directory retained: %v", err)
	}
}

func TestCursorCanceledReadStillCleansUp(t *testing.T) {
	handler := initQueryHandlerForTest(t)
	_, statement, err := handler.HandleParseQuery(context.Background(), &pgproto3.Parse{Query: "SELECT i FROM generate_series(0,9999) t(i)"})
	if err != nil {
		t.Fatal(err)
	}
	_, portal, err := handler.HandleBindQuery(context.Background(), &pgproto3.Bind{}, statement)
	if err != nil {
		t.Fatal(err)
	}
	defer portal.Close()
	if _, err := handler.HandleExecuteQuery(context.Background(), &pgproto3.Execute{MaxRows: 1}, portal); err != nil {
		t.Fatal(err)
	}
	directory := portal.Reader.(*QueryCursor).directory
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := handler.HandleExecuteQuery(ctx, &pgproto3.Execute{MaxRows: 1}, portal); !errors.Is(err, context.Canceled) {
		t.Fatalf("read cancellation = %v", err)
	}
	if _, err := os.Stat(directory); !os.IsNotExist(err) {
		t.Fatalf("canceled read retained spool: %v", err)
	}
	if err := portal.Close(); err != nil {
		t.Fatalf("repeated cleanup: %v", err)
	}
}

func TestCursorCloseReportsFilesystemFailure(t *testing.T) {
	parent := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(parent, []byte("not a directory"), 0600); err != nil {
		t.Fatal(err)
	}
	cursor := &QueryCursor{directory: filepath.Join(parent, "child")}
	if err := cursor.Close(); err == nil {
		t.Fatal("cleanup swallowed filesystem error")
	}
}

type failingResultReader struct {
	failure error
	closed  int
}

func (reader *failingResultReader) ReadBatch(context.Context, uint32) (*sql.Rows, error) {
	return nil, reader.failure
}
func (reader *failingResultReader) Close() error { reader.closed++; return reader.failure }

func TestPortalOwnsAndReportsReaderCleanup(t *testing.T) {
	failure := errors.New("cleanup failed")
	reader := &failingResultReader{failure: failure}
	statement := &PreparedStatement{QueryClassification: QueryClassification{Kind: QUERY_SELECT}}
	first := &Portal{PreparedStatement: statement, Reader: reader}
	second := &Portal{PreparedStatement: statement}
	if err := first.Close(); !errors.Is(err, failure) {
		t.Fatalf("cleanup error = %v", err)
	}
	if reader.closed != 1 || first.Reader != nil || second.Reader != nil {
		t.Fatal("portal ownership was not isolated")
	}
	if err := first.Close(); err != nil || reader.closed != 1 {
		t.Fatal("cleanup is not idempotent")
	}
}

func TestCursorCleanupWhenAttachDidNotCommit(t *testing.T) {
	handler := initQueryHandlerForTest(t)
	conn, err := handler.ServerDuckdbClient.Db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	directory := t.TempDir()
	// An interrupted ATTACH can return an error before the caller knows whether
	// it committed. Cleanup must handle both an attached and an absent database.
	cursor := &QueryCursor{conn: conn, db: handler.ServerDuckdbClient.Db, attached: true, alias: "uncommitted_cursor", directory: directory}
	if err := cursor.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(directory); !os.IsNotExist(err) {
		t.Fatalf("directory retained: %v", err)
	}
}

// A portal is a single execution, even after its reader has been released.
func TestCompletedPortalDoesNotReexecute(t *testing.T) {
	for _, limit := range []uint32{0, 1, 10} {
		t.Run(fmt.Sprint(limit), func(t *testing.T) {
			handler := initQueryHandlerForTest(t)
			ctx := context.Background()
			if _, err := handler.ServerDuckdbClient.ExecContext(ctx, "CREATE SEQUENCE portal_sequence"); err != nil {
				t.Fatal(err)
			}
			query := "SELECT nextval('portal_sequence')"
			if limit > 0 {
				if _, err := handler.ServerDuckdbClient.ExecContext(ctx, "CREATE TABLE portal_source AS SELECT 1 AS i"); err != nil {
					t.Fatal(err)
				}
				query = "SELECT i FROM portal_source"
			}
			_, statement, err := handler.HandleParseQuery(ctx, &pgproto3.Parse{Query: query})
			if err != nil {
				t.Fatal(err)
			}
			_, portal, err := handler.HandleBindQuery(ctx, &pgproto3.Bind{}, statement)
			if err != nil {
				t.Fatal(err)
			}
			defer portal.Close()
			messages, err := handler.HandleExecuteQuery(ctx, &pgproto3.Execute{MaxRows: limit}, portal)
			if err != nil {
				t.Fatal(err)
			}
			if got := string(messages[0].(*pgproto3.DataRow).Values[0]); got != "1" {
				t.Fatalf("first result = %s", got)
			}
			// An exact-boundary cursor reports completion on its next fetch.
			if _, ok := messages[len(messages)-1].(*pgproto3.PortalSuspended); ok {
				messages, err = handler.HandleExecuteQuery(ctx, &pgproto3.Execute{MaxRows: limit}, portal)
				if err != nil {
					t.Fatal(err)
				}
			}
			if got := string(messages[len(messages)-1].(*pgproto3.CommandComplete).CommandTag); got != "SELECT 1" {
				t.Fatalf("tag = %q", got)
			}
			if limit > 0 {
				if _, err := handler.ServerDuckdbClient.ExecContext(ctx, "UPDATE portal_source SET i=2"); err != nil {
					t.Fatal(err)
				}
			}
			for _, nextLimit := range []uint32{0, 1, 10} {
				messages, err = handler.HandleExecuteQuery(ctx, &pgproto3.Execute{MaxRows: nextLimit}, portal)
				if err != nil {
					t.Fatal(err)
				}
				assertMessageTypes(t, messages, "*pgproto3.CommandComplete")
				if got := string(messages[0].(*pgproto3.CommandComplete).CommandTag); got != "SELECT 1" {
					t.Fatalf("repeated tag = %q", got)
				}
			}
			if portal.Reader != nil {
				t.Fatal("completed portal retained reader")
			}
			// A new Bind starts a fresh execution of the same prepared statement.
			_, rebound, err := handler.HandleBindQuery(ctx, &pgproto3.Bind{}, statement)
			if err != nil {
				t.Fatal(err)
			}
			defer rebound.Close()
			messages, err = handler.HandleExecuteQuery(ctx, &pgproto3.Execute{}, rebound)
			if err != nil {
				t.Fatal(err)
			}
			if got := string(messages[0].(*pgproto3.DataRow).Values[0]); got != "2" {
				t.Fatalf("rebound result = %s; completed portal reran source query", got)
			}
		})
	}
}

func TestEmptyPortalRemainsEmptyOnRepeatedExecute(t *testing.T) {
	handler := initQueryHandlerForTest(t)
	_, statement, err := handler.HandleParseQuery(context.Background(), &pgproto3.Parse{})
	if err != nil {
		t.Fatal(err)
	}
	_, portal, err := handler.HandleBindQuery(context.Background(), &pgproto3.Bind{}, statement)
	if err != nil {
		t.Fatal(err)
	}
	defer portal.Close()
	for i := 0; i < 2; i++ {
		messages, err := handler.HandleExecuteQuery(context.Background(), &pgproto3.Execute{}, portal)
		if err != nil {
			t.Fatal(err)
		}
		assertMessageTypes(t, messages, "*pgproto3.EmptyQueryResponse")
	}
}
