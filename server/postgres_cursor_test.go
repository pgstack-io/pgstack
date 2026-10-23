package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
)

func cursorTestConnection(t *testing.T, configure ...func(*PostgresServer)) (*pgproto3.Frontend, *QueryHandler, *PostgresServer, func()) {
	t.Helper()
	t.Setenv("PGSTACK_AUTH_MODE", "scram")
	handler := initQueryHandlerForTest(t)
	// Protocol tests use passwordless startup in both cloud and standalone builds.
	handler.Config.EncryptedPassword = ""
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { listener.Close() })
	done := make(chan struct{})
	servers := make(chan *PostgresServer, 1)
	go func() {
		defer close(done)
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		server := NewPostgresServer(handler.Config, &conn)
		for _, option := range configure {
			option(server)
		}
		servers <- server
		server.Run(func(*Config) (*QueryHandler, func(), error) { return handler, func() {}, nil })
	}()
	conn, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	conn.SetDeadline(time.Now().Add(30 * time.Second))
	shutdown := func() {
		conn.Close()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("server did not stop after disconnect")
		}
	}
	t.Cleanup(shutdown)
	frontend := pgproto3.NewFrontend(conn, conn)
	frontend.Send(&pgproto3.StartupMessage{ProtocolVersion: 196608, Parameters: map[string]string{"database": "pgstack", "user": "user"}})
	if err := frontend.Flush(); err != nil {
		t.Fatal(err)
	}
	for {
		message := receiveCursorMessage(t, frontend)
		if failure, ok := message.(*pgproto3.ErrorResponse); ok {
			t.Fatalf("startup failed: %s (%s)", failure.Message, failure.Code)
		}
		if _, ok := message.(*pgproto3.ReadyForQuery); ok {
			break
		}
	}
	return frontend, handler, <-servers, shutdown
}

func receiveCursorMessage(t *testing.T, frontend *pgproto3.Frontend) pgproto3.BackendMessage {
	t.Helper()
	message, err := frontend.Receive()
	if err != nil {
		t.Fatal(err)
	}
	return message
}

func sendCursorMessages(t *testing.T, frontend *pgproto3.Frontend, messages ...pgproto3.FrontendMessage) {
	t.Helper()
	for _, message := range messages {
		frontend.Send(message)
	}
	if err := frontend.Flush(); err != nil {
		t.Fatal(err)
	}
}

func TestJDBCCursorAcrossSync(t *testing.T) {
	frontend, _, _, _ := cursorTestConnection(t)
	sendCursorMessages(t, frontend,
		&pgproto3.Parse{Query: "BEGIN READ ONLY"}, &pgproto3.Bind{}, &pgproto3.Execute{},
		&pgproto3.Parse{Name: "read", Query: "SELECT i FROM generate_series(0,7999) t(i) ORDER BY i"},
		&pgproto3.Bind{PreparedStatement: "read", DestinationPortal: "C_1"},
		&pgproto3.Describe{ObjectType: 'P', Name: "C_1"},
		&pgproto3.Execute{Portal: "C_1", MaxRows: 3677}, &pgproto3.Sync{},
	)
	next := 0
	for batch, count := range []int{3677, 3677, 646} {
		rows := 0
		terminal := ""
		for {
			message := receiveCursorMessage(t, frontend)
			switch message := message.(type) {
			case *pgproto3.ErrorResponse:
				t.Fatalf("server error: %+v", message)
			case *pgproto3.DataRow:
				if string(message.Values[0]) != fmt.Sprint(next) {
					t.Fatalf("row %d = %q", next, message.Values)
				}
				next++
				rows++
			case *pgproto3.PortalSuspended:
				terminal = "suspended"
			case *pgproto3.CommandComplete:
				if string(message.CommandTag) != "BEGIN" {
					terminal = string(message.CommandTag)
				}
			case *pgproto3.ReadyForQuery:
				if message.TxStatus != 'T' {
					t.Fatalf("transaction status = %c", message.TxStatus)
				}
				goto fetched
			}
		}
	fetched:
		if rows != count {
			t.Fatalf("batch %d rows = %d, want %d", batch, rows, count)
		}
		if batch < 2 && terminal != "suspended" || batch == 2 && terminal != "SELECT 8000" {
			t.Fatalf("batch %d terminal = %q", batch, terminal)
		}
		if batch < 2 {
			sendCursorMessages(t, frontend, &pgproto3.Execute{Portal: "C_1", MaxRows: 3677}, &pgproto3.Sync{})
		}
	}
	sendCursorMessages(t, frontend, &pgproto3.Query{String: "COMMIT"})
	for {
		message := receiveCursorMessage(t, frontend)
		if ready, ok := message.(*pgproto3.ReadyForQuery); ok {
			if ready.TxStatus != 'I' {
				t.Fatal("commit did not end transaction")
			}
			break
		}
	}
}

func TestCursorCloseAndDisconnectCleanup(t *testing.T) {
	for _, mode := range []string{"close", "disconnect"} {
		t.Run(mode, func(t *testing.T) {
			temporary := t.TempDir()
			t.Setenv("TMPDIR", temporary)
			frontend, _, _, shutdown := cursorTestConnection(t)
			sendCursorMessages(t, frontend, &pgproto3.Parse{Query: "BEGIN"}, &pgproto3.Bind{}, &pgproto3.Execute{},
				&pgproto3.Parse{Name: "read", Query: "SELECT i FROM range(10000) t(i)"},
				&pgproto3.Bind{PreparedStatement: "read", DestinationPortal: "C_1"}, &pgproto3.Execute{Portal: "C_1", MaxRows: 10}, &pgproto3.Sync{})
			for {
				if _, ok := receiveCursorMessage(t, frontend).(*pgproto3.ReadyForQuery); ok {
					break
				}
			}
			directories, err := filepath.Glob(filepath.Join(temporary, "pgstack-cursor-*"))
			if err != nil || len(directories) != 1 {
				t.Fatalf("expected one cursor spool, found %v: %v", directories, err)
			}
			directory := directories[0]
			if mode == "close" {
				sendCursorMessages(t, frontend, &pgproto3.Close{ObjectType: 'P', Name: "C_1"}, &pgproto3.Sync{})
				for {
					if _, ok := receiveCursorMessage(t, frontend).(*pgproto3.ReadyForQuery); ok {
						break
					}
				}
			}
			shutdown()
			if _, err := os.Stat(directory); !os.IsNotExist(err) {
				t.Fatalf("cursor directory retained: %v", err)
			}
		})
	}
}

func TestExtendedQueryErrorWaitsForSync(t *testing.T) {
	frontend, _, _, _ := cursorTestConnection(t)
	sendCursorMessages(t, frontend, &pgproto3.Parse{Query: "SELECT missing FROM missing_table"}, &pgproto3.Bind{}, &pgproto3.Execute{}, &pgproto3.Sync{})
	if _, ok := receiveCursorMessage(t, frontend).(*pgproto3.ErrorResponse); !ok {
		t.Fatal("expected error")
	}
	if _, ok := receiveCursorMessage(t, frontend).(*pgproto3.ReadyForQuery); !ok {
		t.Fatal("expected one ReadyForQuery at Sync")
	}
	sendCursorMessages(t, frontend, &pgproto3.Parse{Query: "SELECT 42"}, &pgproto3.Bind{}, &pgproto3.Execute{}, &pgproto3.Sync{})
	rows := 0
	for {
		message := receiveCursorMessage(t, frontend)
		if err, ok := message.(*pgproto3.ErrorResponse); ok {
			t.Fatalf("recovery failed: %+v", err)
		}
		if row, ok := message.(*pgproto3.DataRow); ok {
			if string(row.Values[0]) != "42" {
				t.Fatal("unexpected value")
			}
			rows++
		}
		if _, ok := message.(*pgproto3.ReadyForQuery); ok {
			break
		}
	}
	if rows != 1 {
		t.Fatalf("rows = %d", rows)
	}
}

func TestDiskCursorLargeResult(t *testing.T) {
	handler := initQueryHandlerForTest(t)
	// The uncompressed result exceeds the buffer-manager budget. CTAS must
	// persist it instead of handing a fully materialized result to database/sql.
	if _, err := handler.ServerDuckdbClient.ExecContext(context.Background(), "SET memory_limit='128MB'; SET threads=1"); err != nil {
		t.Fatal(err)
	}
	_, statement, err := handler.HandleParseQuery(context.Background(), &pgproto3.Parse{Query: "SELECT i, repeat(md5(i::text),256) AS payload FROM range(40000) t(i)"})
	if err != nil {
		t.Fatal(err)
	}

	_, portal1, err := handler.HandleBindQuery(context.Background(), &pgproto3.Bind{}, statement)
	defer portal1.Close()
	if err != nil {
		t.Fatal(err)
	}
	messages, err := handler.HandleExecuteQuery(context.Background(), &pgproto3.Execute{MaxRows: 100}, portal1)
	if err != nil {
		t.Fatal(err)
	}
	if len(messages) != 101 {
		t.Fatalf("messages = %d", len(messages))
	}
	if _, ok := messages[100].(*pgproto3.PortalSuspended); !ok {
		t.Fatal("expected suspension")
	}
}

func TestIndependentPortalsAndUnlimitedExecute(t *testing.T) {
	frontend, _, _, _ := cursorTestConnection(t)
	sendCursorMessages(t, frontend, &pgproto3.Parse{Query: "BEGIN"}, &pgproto3.Bind{}, &pgproto3.Execute{},
		&pgproto3.Parse{Name: "read", Query: "SELECT i FROM generate_series(0, $1::bigint) t(i) ORDER BY i"},
		&pgproto3.Bind{PreparedStatement: "read", DestinationPortal: "first", Parameters: [][]byte{[]byte("4999")}},
		&pgproto3.Bind{PreparedStatement: "read", DestinationPortal: "second", Parameters: [][]byte{[]byte("1")}},
		&pgproto3.Execute{Portal: "first", MaxRows: 1024}, &pgproto3.Sync{})
	for {
		if _, ok := receiveCursorMessage(t, frontend).(*pgproto3.ReadyForQuery); ok {
			break
		}
	}
	// Execute(MaxRows=0) must drain bounded internal batches and emit no suspension.
	for _, test := range []struct {
		portal       string
		first, count int
	}{{"second", 0, 2}, {"first", 1024, 3976}} {
		sendCursorMessages(t, frontend, &pgproto3.Execute{Portal: test.portal}, &pgproto3.Sync{})
		count := 0
		for {
			message := receiveCursorMessage(t, frontend)
			switch message := message.(type) {
			case *pgproto3.DataRow:
				if string(message.Values[0]) != fmt.Sprint(test.first+count) {
					t.Fatalf("unexpected row %q", message.Values)
				}
				count++
			case *pgproto3.PortalSuspended:
				t.Fatal("unlimited Execute suspended")
			case *pgproto3.ErrorResponse:
				t.Fatalf("server error: %+v", message)
			case *pgproto3.ReadyForQuery:
				goto complete
			}
		}
	complete:
		if count != test.count {
			t.Fatalf("portal %s rows = %d", test.portal, count)
		}
	}
	// Bind the same named statement again after Sync released its runtime.
	sendCursorMessages(t, frontend, &pgproto3.Bind{PreparedStatement: "read", DestinationPortal: "empty", Parameters: [][]byte{[]byte("-1")}}, &pgproto3.Execute{Portal: "empty", MaxRows: 1}, &pgproto3.Sync{})
	for {
		message := receiveCursorMessage(t, frontend)
		switch message := message.(type) {
		case *pgproto3.DataRow:
			t.Fatal("empty portal returned data")
		case *pgproto3.CommandComplete:
			if string(message.CommandTag) != "SELECT 0" {
				t.Fatalf("empty result tag %q", message.CommandTag)
			}
		case *pgproto3.ErrorResponse:
			t.Fatalf("server error: %+v", message)
		case *pgproto3.ReadyForQuery:
			return
		}
	}
}

func TestCursorFailureAbortsTransactionUntilRollback(t *testing.T) {
	frontend, _, _, _ := cursorTestConnection(t)
	sendCursorMessages(t, frontend, &pgproto3.Parse{Query: "BEGIN"}, &pgproto3.Bind{}, &pgproto3.Execute{},
		&pgproto3.Parse{Query: "SELECT CAST('bad' AS INTEGER)"}, &pgproto3.Bind{}, &pgproto3.Execute{MaxRows: 10}, &pgproto3.Sync{})
	failed := false
	for {
		message := receiveCursorMessage(t, frontend)
		if _, ok := message.(*pgproto3.ErrorResponse); ok {
			failed = true
		}
		if ready, ok := message.(*pgproto3.ReadyForQuery); ok {
			if ready.TxStatus != 'E' || !failed {
				t.Fatal("transaction must fail")
			}
			break
		}
	}
	sendCursorMessages(t, frontend, &pgproto3.Query{String: "SELECT 1"})
	if err, ok := receiveCursorMessage(t, frontend).(*pgproto3.ErrorResponse); !ok || err.Code != "25P02" {
		t.Fatal("aborted transaction accepted query")
	}
	receiveCursorMessage(t, frontend)
	sendCursorMessages(t, frontend, &pgproto3.Query{String: "ROLLBACK"})
	for {
		message := receiveCursorMessage(t, frontend)
		if err, ok := message.(*pgproto3.ErrorResponse); ok {
			t.Fatalf("rollback failed: %+v", err)
		}
		if ready, ok := message.(*pgproto3.ReadyForQuery); ok {
			if ready.TxStatus != 'I' {
				t.Fatal("rollback did not clear transaction")
			}
			break
		}
	}
}

func TestOrdinaryPreparedSelectOnWireDoesNotSpool(t *testing.T) {
	frontend, _, _, _ := cursorTestConnection(t)
	t.Setenv("TMPDIR", t.TempDir()+"/missing")
	sendCursorMessages(t, frontend, &pgproto3.Parse{Query: "SELECT i FROM generate_series(0,2047) t(i) ORDER BY i"}, &pgproto3.Bind{}, &pgproto3.Describe{ObjectType: 'P'}, &pgproto3.Execute{}, &pgproto3.Sync{})
	rows := 0
	for {
		message := receiveCursorMessage(t, frontend)
		switch message := message.(type) {
		case *pgproto3.DataRow:
			if string(message.Values[0]) != fmt.Sprint(rows) {
				t.Fatalf("unexpected row %q", message.Values)
			}
			rows++
		case *pgproto3.PortalSuspended:
			t.Fatal("ordinary query suspended")
		case *pgproto3.ErrorResponse:
			t.Fatalf("ordinary query tried to spool: %+v", message)
		case *pgproto3.ReadyForQuery:
			if rows != 2048 {
				t.Fatalf("rows = %d", rows)
			}
			return
		}
	}
}

func TestCursorIdleTimeoutReleasesSpool(t *testing.T) {
	temporary := t.TempDir()
	t.Setenv("TMPDIR", temporary)
	frontend, _, _, shutdown := cursorTestConnection(t, func(server *PostgresServer) { server.cursorIdleTimeout = 100 * time.Millisecond })
	sendCursorMessages(t, frontend, &pgproto3.Parse{Query: "BEGIN"}, &pgproto3.Bind{}, &pgproto3.Execute{},
		&pgproto3.Parse{Query: "SELECT i FROM generate_series(0,9999) t(i)"}, &pgproto3.Bind{DestinationPortal: "C_1"}, &pgproto3.Execute{Portal: "C_1", MaxRows: 1}, &pgproto3.Sync{})
	for {
		if _, ok := receiveCursorMessage(t, frontend).(*pgproto3.ReadyForQuery); ok {
			break
		}
	}
	if _, err := frontend.Receive(); err == nil {
		t.Fatal("idle cursor connection remained open")
	}
	shutdown()
	directories, err := filepath.Glob(filepath.Join(temporary, "pgstack-cursor-*"))
	if err != nil || len(directories) != 0 {
		t.Fatalf("abandoned spool retained: %v, %v", directories, err)
	}
}

func TestDisconnectCancelsSpooling(t *testing.T) {
	temporary := t.TempDir()
	t.Setenv("TMPDIR", temporary)
	frontend, _, _, shutdown := cursorTestConnection(t)
	sendCursorMessages(t, frontend, &pgproto3.Parse{Query: "SELECT sum(a.i*b.i) FROM generate_series(1,1000000000) a(i), generate_series(1,1000000000) b(i)"},
		&pgproto3.Bind{DestinationPortal: "C_1"}, &pgproto3.Execute{Portal: "C_1", MaxRows: 1}, &pgproto3.Sync{})
	deadline := time.Now().Add(5 * time.Second)
	for {
		files, err := filepath.Glob(filepath.Join(temporary, "pgstack-cursor-*", "result.duckdb"))
		if err != nil {
			t.Fatal(err)
		}
		if len(files) > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("spool creation did not start")
		}
		time.Sleep(time.Millisecond)
	}
	shutdown()
	directories, err := filepath.Glob(filepath.Join(temporary, "pgstack-cursor-*"))
	if err != nil || len(directories) != 0 {
		t.Fatalf("canceled spool retained: %v, %v", directories, err)
	}
}

func TestSimpleQueryTransactionSequence(t *testing.T) {
	frontend, _, _, _ := cursorTestConnection(t)
	for _, step := range []struct {
		query  string
		status byte
		code   string
		tags   []string
		rows   []string
	}{
		{"BEGIN; SELECT CAST('bad' AS INTEGER); SELECT 99", 'E', "XX000", []string{"BEGIN"}, nil},
		{"SELECT 99; ROLLBACK", 'E', "25P02", nil, nil},
		{"COMMIT", 'I', "", []string{"ROLLBACK"}, nil},
		{"BEGIN; SELECT 1; COMMIT; SELECT 2", 'I', "", []string{"BEGIN", "SELECT 1", "COMMIT", "SELECT 1"}, []string{"1", "2"}},
		{"BEGIN; SELECT CAST('bad' AS INTEGER)", 'E', "XX000", []string{"BEGIN"}, nil},
		{"ROLLBACK; SELECT 42", 'I', "", []string{"ROLLBACK", "SELECT 1"}, []string{"42"}},
		{"BEGIN; ROLLBACK AND CHAIN", 'T', "", []string{"BEGIN", "ROLLBACK"}, nil},
		{"COMMIT AND CHAIN", 'T', "", []string{"COMMIT"}, nil},
		{"ROLLBACK", 'I', "", []string{"ROLLBACK"}, nil},
		{"BEGIN; SELECT (", 'I', "42601", nil, nil},
		{"COMMIT AND CHAIN", 'I', "25P01", nil, nil},
		{"BEGIN; SAVEPOINT test", 'E', "0A000", []string{"BEGIN"}, nil},
		{"ROLLBACK", 'I', "", []string{"ROLLBACK"}, nil},
		{"BEGIN; DISCARD ALL", 'E', "25001", []string{"BEGIN"}, nil},
		{"ROLLBACK", 'I', "", []string{"ROLLBACK"}, nil},
	} {
		sendCursorMessages(t, frontend, &pgproto3.Query{String: step.query})
		var tags, rows []string
		code := ""
		for {
			message := receiveCursorMessage(t, frontend)
			switch message := message.(type) {
			case *pgproto3.CommandComplete:
				tags = append(tags, string(message.CommandTag))
			case *pgproto3.DataRow:
				rows = append(rows, string(message.Values[0]))
			case *pgproto3.ErrorResponse:
				code = message.Code
			case *pgproto3.ReadyForQuery:
				if message.TxStatus != step.status || code != step.code || !slices.Equal(tags, step.tags) || !slices.Equal(rows, step.rows) {
					t.Fatalf("%s: status=%c error=%q tags=%v rows=%v", step.query, message.TxStatus, code, tags, rows)
				}
				goto next
			}
		}
	next:
	}
}

func TestExtendedStatementDescribeBeforeBind(t *testing.T) {
	frontend, _, _, _ := cursorTestConnection(t)
	sendCursorMessages(t, frontend, &pgproto3.Parse{Name: "read", Query: "SELECT $1::bigint AS number", ParameterOIDs: []uint32{20}}, &pgproto3.Describe{ObjectType: 'S', Name: "read"}, &pgproto3.Sync{})
	foundParameters, foundColumns := false, false
	for {
		message := receiveCursorMessage(t, frontend)
		switch message := message.(type) {
		case *pgproto3.ParameterDescription:
			foundParameters = len(message.ParameterOIDs) == 1 && message.ParameterOIDs[0] == 20
		case *pgproto3.RowDescription:
			foundColumns = len(message.Fields) == 1 && string(message.Fields[0].Name) == "number" && message.Fields[0].DataTypeOID == 20
		case *pgproto3.ErrorResponse:
			t.Fatalf("Describe failed: %+v", message)
		case *pgproto3.ReadyForQuery:
			goto described
		}
	}
described:
	if !foundParameters || !foundColumns {
		t.Fatal("missing statement metadata")
	}
	sendCursorMessages(t, frontend, &pgproto3.Bind{PreparedStatement: "read", Parameters: [][]byte{[]byte("42")}}, &pgproto3.Execute{}, &pgproto3.Sync{})
	for {
		message := receiveCursorMessage(t, frontend)
		if row, ok := message.(*pgproto3.DataRow); ok && string(row.Values[0]) != "42" {
			t.Fatal("wrong bound result")
		}
		if err, ok := message.(*pgproto3.ErrorResponse); ok {
			t.Fatalf("Bind failed: %+v", err)
		}
		if _, ok := message.(*pgproto3.ReadyForQuery); ok {
			break
		}
	}
}

func TestTransactionErrorClosesOtherCursor(t *testing.T) {
	temporary := t.TempDir()
	t.Setenv("TMPDIR", temporary)
	frontend, _, _, _ := cursorTestConnection(t)
	sendCursorMessages(t, frontend, &pgproto3.Query{String: "BEGIN"})
	for {
		if _, ok := receiveCursorMessage(t, frontend).(*pgproto3.ReadyForQuery); ok {
			break
		}
	}
	sendCursorMessages(t, frontend, &pgproto3.Parse{Name: "read", Query: "SELECT i FROM generate_series(0,9999) t(i)"}, &pgproto3.Bind{PreparedStatement: "read", DestinationPortal: "C_1"}, &pgproto3.Execute{Portal: "C_1", MaxRows: 1}, &pgproto3.Sync{})
	for {
		if _, ok := receiveCursorMessage(t, frontend).(*pgproto3.ReadyForQuery); ok {
			break
		}
	}
	directories, err := filepath.Glob(filepath.Join(temporary, "pgstack-cursor-*"))
	if err != nil || len(directories) != 1 {
		t.Fatalf("expected active spool: %v, %v", directories, err)
	}
	sendCursorMessages(t, frontend, &pgproto3.Parse{Query: "SELECT missing FROM missing_table"}, &pgproto3.Sync{})
	for {
		if ready, ok := receiveCursorMessage(t, frontend).(*pgproto3.ReadyForQuery); ok {
			if ready.TxStatus != 'E' {
				t.Fatal("transaction did not abort")
			}
			break
		}
	}
	directories, err = filepath.Glob(filepath.Join(temporary, "pgstack-cursor-*"))
	if err != nil || len(directories) != 0 {
		t.Fatalf("failed transaction retained spool: %v, %v", directories, err)
	}
}

func TestCompletedPortalAcrossSync(t *testing.T) {
	frontend, handler, _, _ := cursorTestConnection(t)
	if _, err := handler.ServerDuckdbClient.ExecContext(context.Background(), "CREATE SEQUENCE wire_portal_sequence"); err != nil {
		t.Fatal(err)
	}
	sendCursorMessages(t, frontend,
		&pgproto3.Query{String: "BEGIN"},
	)
	for {
		if _, ok := receiveCursorMessage(t, frontend).(*pgproto3.ReadyForQuery); ok {
			break
		}
	}
	sendCursorMessages(t, frontend,
		&pgproto3.Parse{Name: "read", Query: "SELECT nextval('wire_portal_sequence')"},
		&pgproto3.Bind{PreparedStatement: "read", DestinationPortal: "result"},
		&pgproto3.Execute{Portal: "result"}, &pgproto3.Sync{},
	)
	readResult := func(wantRows int, wantValue string) {
		t.Helper()
		rows, completions := 0, 0
		for {
			switch message := receiveCursorMessage(t, frontend).(type) {
			case *pgproto3.ErrorResponse:
				t.Fatalf("server error: %+v", message)
			case *pgproto3.DataRow:
				rows++
				if string(message.Values[0]) != wantValue {
					t.Fatalf("value = %q", message.Values)
				}
			case *pgproto3.CommandComplete:
				completions++
				if string(message.CommandTag) != "SELECT 1" {
					t.Fatalf("tag = %q", message.CommandTag)
				}
			case *pgproto3.ReadyForQuery:
				if message.TxStatus != 'T' || rows != wantRows || completions != 1 {
					t.Fatalf("status=%c rows=%d completions=%d", message.TxStatus, rows, completions)
				}
				return
			}
		}
	}
	readResult(1, "1")
	for _, limit := range []uint32{0, 1} {
		sendCursorMessages(t, frontend, &pgproto3.Execute{Portal: "result", MaxRows: limit}, &pgproto3.Sync{})
		readResult(0, "")
	}
	sendCursorMessages(t, frontend,
		&pgproto3.Bind{PreparedStatement: "read", DestinationPortal: "result"},
		&pgproto3.Execute{Portal: "result"}, &pgproto3.Sync{},
	)
	readResult(1, "2")
}
