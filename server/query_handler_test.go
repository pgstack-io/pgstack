package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"
)

type queryResponseExpectation struct {
	query       string
	columnNames []string
	values      []string
}

func TestHandleQueryFixedSizeFloatArray(t *testing.T) {
	queryHandler := initQueryHandlerForTest(t)
	if _, err := queryHandler.ServerDuckdbClient.ExecContext(
		context.Background(),
		"CREATE TABLE public.fixed_vector AS SELECT [1.0, 2.0]::FLOAT[2] AS embedding",
	); err != nil {
		t.Fatalf("create fixed-size array fixture: %v", err)
	}
	messages, err := queryHandler.HandleSimpleQuery("SELECT embedding FROM fixed_vector")
	if err != nil {
		t.Fatalf("handle fixed-size array query: %v", err)
	}
	assertMessageTypes(t, messages, "*pgproto3.RowDescription", "*pgproto3.DataRow", "*pgproto3.CommandComplete")

	description := messages[0].(*pgproto3.RowDescription)
	if description.Fields[0].DataTypeOID != pgtype.Float4ArrayOID {
		t.Fatalf("fixed-size FLOAT array OID = %d, want %d", description.Fields[0].DataTypeOID, pgtype.Float4ArrayOID)
	}
	assertDataRow(t, messages[1], []string{"{1,2}"})
}

func TestDuckdbQueryContextBindsEmbeddingVector(t *testing.T) {
	queryHandler := initQueryHandlerForTest(t)
	rows, err := queryHandler.ServerDuckdbClient.QueryContext(
		context.Background(),
		"SELECT array_distance($1::REAL[2], [0.6, 0.8]::REAL[2])",
		[]float32{0.6, 0.8},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatal("expected distance row")
	}
	var distance float32
	if err := rows.Scan(&distance); err != nil {
		t.Fatal(err)
	}
	if distance != 0 {
		t.Fatalf("distance = %v, want 0", distance)
	}
}

func TestHandleQueryUnsignedIntegers(t *testing.T) {
	queryHandler := initQueryHandlerForTest(t)
	if _, err := queryHandler.ServerDuckdbClient.ExecContext(
		context.Background(),
		"CREATE TABLE public.unsigned_values AS SELECT 4294967295::UINTEGER AS chunk_index, 18446744073709551615::UBIGINT AS source_position",
	); err != nil {
		t.Fatalf("create unsigned integer fixture: %v", err)
	}
	messages, err := queryHandler.HandleSimpleQuery("SELECT chunk_index, source_position FROM unsigned_values")
	if err != nil {
		t.Fatalf("handle unsigned integer query: %v", err)
	}
	assertMessageTypes(t, messages, "*pgproto3.RowDescription", "*pgproto3.DataRow", "*pgproto3.CommandComplete")

	description := messages[0].(*pgproto3.RowDescription)
	if description.Fields[0].DataTypeOID != pgtype.XIDOID {
		t.Fatalf("UINTEGER OID = %d, want %d", description.Fields[0].DataTypeOID, pgtype.XIDOID)
	}
	if description.Fields[1].DataTypeOID != pgtype.XID8OID {
		t.Fatalf("UBIGINT OID = %d, want %d", description.Fields[1].DataTypeOID, pgtype.XID8OID)
	}
	assertDataRow(t, messages[1], []string{"4294967295", "18446744073709551615"})
}

func TestHandleQueryPgSystemTables(t *testing.T) {
	queryHandler := initQueryHandlerForTest(t)

	tests := []queryResponseExpectation{
		{
			query:       "SELECT * FROM pg_catalog.pg_inherits",
			columnNames: []string{"inhrelid", "inhparent", "inhseqno", "inhdetachpending"},
		},
		{
			query:       "SELECT indnullsnotdistinct FROM pg_catalog.pg_index",
			columnNames: []string{"indnullsnotdistinct"},
		},
		{
			query:       "SELECT relhassubclass FROM pg_class LIMIT 1",
			columnNames: []string{"relhassubclass"},
			values:      []string{"f"},
		},
		{
			query:       "SELECT oid FROM pg_catalog.pg_extension",
			columnNames: []string{"oid"},
			values:      []string{"13823"},
		},
		{
			query:       "SELECT slot_name FROM pg_replication_slots",
			columnNames: []string{"slot_name"},
		},
		{
			query:       "SELECT oid, datname, datdba FROM pg_catalog.pg_database WHERE oid = 16388",
			columnNames: []string{"oid", "datname", "datdba"},
			values:      []string{"16388", "pgstack", "10"},
		},
		{
			query:       "SELECT COALESCE(NULL, (SELECT datname FROM pg_database WHERE datname = 'pgstack')) AS datname",
			columnNames: []string{"datname"},
			values:      []string{"pgstack"},
		},
		{
			query:       "SELECT COALESCE(NULL, '[]'::jsonb) AS json_value",
			columnNames: []string{"json_value"},
			values:      []string{"[]"},
		},
		{
			query:       "SELECT jsonb_array_length(COALESCE('[]'::jsonb, '{}'::jsonb)) AS length",
			columnNames: []string{"length"},
			values:      []string{"0"},
		},
		{
			query:       "SELECT * FROM pg_catalog.pg_stat_gssapi",
			columnNames: []string{"pid", "gss_authenticated", "principal", "encrypted", "credentials_delegated"},
		},
		{
			query:       "SELECT * FROM pg_catalog.pg_user",
			columnNames: []string{"usename", "usesysid", "usecreatedb", "usesuper", "userepl", "usebypassrls", "passwd", "valuntil", "useconfig"},
			values:      []string{"user", "10", "t", "t", "t", "t", "", "", ""},
		},
		{
			query:       "SELECT datid FROM pg_catalog.pg_stat_activity",
			columnNames: []string{"datid"},
		},
		{
			query:       "SELECT schemaname, matviewname AS objectname FROM pg_catalog.pg_matviews",
			columnNames: []string{"schemaname", "objectname"},
		},
		{
			query:       "SELECT * FROM pg_catalog.pg_views",
			columnNames: []string{"schemaname", "viewname", "viewowner", "definition"},
		},
		{
			query:       "SELECT oid FROM pg_collation",
			columnNames: []string{"oid"},
			values:      []string{"100"},
		},
		{
			query:       "SELECT * FROM pg_opclass",
			columnNames: []string{"oid", "opcmethod", "opcname", "opcnamespace", "opcowner", "opcfamily", "opcintype", "opcdefault", "opckeytype"},
		},
		{
			query:       "SELECT * FROM pg_catalog.pg_shdescription",
			columnNames: []string{"objoid", "classoid", "description"},
		},
		{
			query:       "SELECT * FROM pg_catalog.pg_roles",
			columnNames: []string{"oid", "rolname", "rolsuper", "rolinherit", "rolcreaterole", "rolcreatedb", "rolcanlogin", "rolreplication", "rolconnlimit", "rolpassword", "rolvaliduntil", "rolbypassrls", "rolconfig"},
			values:      []string{"10", "user", "t", "t", "t", "t", "t", "f", "-1", "", "", "f", ""},
		},
		{
			query:       "SELECT * FROM pg_auth_members",
			columnNames: []string{"oid", "roleid", "member", "grantor", "admin_option", "inherit_option", "set_option"},
		},
		{
			query:       "SELECT * FROM pg_catalog.pg_policy",
			columnNames: []string{"oid", "polname", "polrelid", "polcmd", "polpermissive", "polroles", "polqual", "polwithcheck"},
		},
		{
			query:       "SELECT * FROM pg_catalog.pg_statistic_ext",
			columnNames: []string{"oid", "stxrelid", "stxname", "stxnamespace", "stxowner", "stxstattarget", "stxkeys", "stxkind", "stxexprs"},
		},
		{
			query:       "SELECT * FROM pg_catalog.pg_publication",
			columnNames: []string{"oid", "pubname", "pubowner", "puballtables", "pubinsert", "pubupdate", "pubdelete", "pubtruncate", "pubviaroot"},
		},
		{
			query:       "SELECT * FROM pg_catalog.pg_publication_rel",
			columnNames: []string{"oid", "prpubid", "prrelid", "prqual", "prattrs"},
		},
		{
			query:       "SELECT * FROM pg_catalog.pg_publication_namespace",
			columnNames: []string{"oid", "pnpubid", "pnnspid"},
		},
		{
			query:       "SELECT * FROM pg_catalog.pg_rewrite",
			columnNames: []string{"oid", "rulename", "ev_class", "ev_type", "ev_enabled", "is_instead", "ev_qual", "ev_action"},
		},
		{
			query:       "SELECT * FROM user",
			columnNames: []string{"user"},
			values:      []string{"user"},
		},
		{
			query:       "SELECT oid FROM pg_type WHERE typname = 'text'",
			columnNames: []string{"oid"},
			values:      []string{"25"},
		},
		{
			query:       "SELECT DISTINCT ON (typlen) oid FROM pg_type ORDER BY oid LIMIT 1",
			columnNames: []string{"oid"},
			values:      []string{"16"},
		},
		// Temporarily disabled: requires the BemiDB postgres.test_table Iceberg fixture.
		// {query: "SELECT relname FROM pg_catalog.pg_class ...", ...},
		// {query: "SELECT schemaname, relname, n_live_tup FROM pg_stat_user_tables ...", ...},
	}

	for _, test := range tests {
		t.Run(test.query, func(t *testing.T) {
			assertQueryResponse(t, queryHandler, test)
		})
	}
}

func TestHandleQueryPgAttributeUsesPostgresTypeOids(t *testing.T) {
	queryHandler := initQueryHandlerForTest(t)
	_, err := queryHandler.ServerDuckdbClient.ExecContext(context.Background(), "CREATE SCHEMA audit")
	if err != nil {
		t.Fatalf("create test schema: %v", err)
	}
	_, err = queryHandler.ServerDuckdbClient.ExecContext(
		context.Background(),
		`CREATE TABLE audit.changes (
			id VARCHAR,
			committed_at TIMESTAMPTZ,
			transaction_id BIGINT
		)`,
	)
	if err != nil {
		t.Fatalf("create test table: %v", err)
	}

	assertQueryResponse(t, queryHandler, queryResponseExpectation{
		query: `SELECT
			MAX(CASE WHEN a.attname = 'id' THEN a.atttypid END) AS text_oid,
			MAX(CASE WHEN a.attname = 'id' THEN t.typname END) AS text_name,
			MAX(CASE WHEN a.attname = 'committed_at' THEN a.atttypid END) AS timestamptz_oid,
			MAX(CASE WHEN a.attname = 'committed_at' THEN t.typname END) AS timestamptz_name,
			MAX(CASE WHEN a.attname = 'transaction_id' THEN a.atttypid END) AS int8_oid,
			MAX(CASE WHEN a.attname = 'transaction_id' THEN t.typname END) AS int8_name
		FROM pg_catalog.pg_attribute a
		JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
		JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
		LEFT JOIN pg_catalog.pg_type t ON t.oid = a.atttypid
		WHERE n.nspname = 'audit' AND c.relname = 'changes'`,
		columnNames: []string{"text_oid", "text_name", "timestamptz_oid", "timestamptz_name", "int8_oid", "int8_name"},
		values:      []string{"25", "text", "1184", "timestamptz", "20", "int8"},
	})
	assertQueryResponse(t, queryHandler, queryResponseExpectation{
		query: `SELECT udt_name
		FROM information_schema.columns
		WHERE table_schema = 'audit'
			AND table_name = 'changes'
			AND column_name = 'committed_at'`,
		columnNames: []string{"udt_name"},
		values:      []string{"timestamptz"},
	})
}

func TestHandleQueryPgFunctions(t *testing.T) {
	queryHandler := initQueryHandlerForTest(t)

	tests := []queryResponseExpectation{
		{
			query:       "SELECT VERSION()",
			columnNames: []string{"version"},
			values:      []string{"PostgreSQL 17.0, compiled by PgStack"},
		},
		{
			query:       "SELECT pg_catalog.pg_get_userbyid(p.proowner) AS owner, 'Foo' AS foo FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace LIMIT 1",
			columnNames: []string{"owner", "foo"},
			values:      []string{"user", "Foo"},
		},
		{
			query:       "SELECT QUOTE_IDENT('fooBar') AS quote_ident",
			columnNames: []string{"quote_ident"},
			values:      []string{"\"fooBar\""},
		},
		// Temporarily disabled: DuckDB now returns "NULLS_LAST" while the BemiDB
		// compatibility test expects PostgreSQL-style "nulls_last".
		// {
		// 	query:       "SELECT setting FROM pg_show_all_settings() WHERE name = 'default_null_order'",
		// 	columnNames: []string{"setting"},
		// 	values:      []string{"nulls_last"},
		// },
		{
			query:       "SELECT pg_catalog.pg_get_partkeydef(c.oid) AS pg_get_partkeydef FROM pg_catalog.pg_class c LIMIT 1",
			columnNames: []string{"pg_get_partkeydef"},
			values:      []string{""},
		},
		{
			query:       "SELECT pg_tablespace_location(t.oid) AS loc FROM pg_catalog.pg_tablespace t LIMIT 1",
			columnNames: []string{"loc"},
			values:      []string{""},
		},
		{
			query:       "SELECT pg_catalog.pg_get_viewdef(NULL, TRUE) AS viewdef",
			columnNames: []string{"viewdef"},
			values:      []string{""},
		},
		{
			query:       "SELECT set_config('bytea_output', 'hex', false) AS set_config",
			columnNames: []string{"set_config"},
			values:      []string{"hex"},
		},
		{
			query:       "SELECT pg_catalog.pg_encoding_to_char(6) AS pg_encoding_to_char",
			columnNames: []string{"pg_encoding_to_char"},
			values:      []string{"UTF8"},
		},
		{
			query:       "SELECT pg_backend_pid() AS pg_backend_pid",
			columnNames: []string{"pg_backend_pid"},
			values:      []string{"0"},
		},
		{
			query:       "SELECT pg_cancel_backend(12345) AS pg_cancel_backend",
			columnNames: []string{"pg_cancel_backend"},
			values:      []string{"t"},
		},
		{
			query:       "SELECT * FROM pg_is_in_recovery()",
			columnNames: []string{"pg_is_in_recovery"},
			values:      []string{"f"},
		},
		{
			query:       "SELECT row_to_json(t) AS row_to_json FROM (SELECT usename FROM pg_shadow WHERE usename = 'user') t",
			columnNames: []string{"row_to_json"},
			values:      []string{`{"usename":"user"}`},
		},
		{
			query:       "SELECT current_setting('default_tablespace') AS current_setting",
			columnNames: []string{"current_setting"},
			values:      []string{""},
		},
		{
			query:       "SELECT array_to_string('[1, 2, 3]', '') AS str",
			columnNames: []string{"str"},
			values:      []string{"123"},
		},
		{
			query:       "SELECT * FROM pg_catalog.generate_series(1, 1)",
			columnNames: []string{"generate_series"},
			values:      []string{"1"},
		},
		{
			query:       "SELECT format('Hello %s, %s, %1$s', 'World', 'Earth') AS str",
			columnNames: []string{"str"},
			values:      []string{"Hello World, Earth, World"},
		},
		{
			query:       "SELECT encode(sha256('foo'), 'hex'::text) AS encode",
			columnNames: []string{"encode"},
			values:      []string{"2c26b46b68ffc68ff99b453c1d30413413422d706483bfa0f98a5e886266e7ae"},
		},
		{
			query:       "SELECT jsonb_object_agg('key', 'value') AS jsonb_object_agg",
			columnNames: []string{"jsonb_object_agg"},
			values:      []string{`{"key":"value"}`},
		},
		// Temporarily disabled: DuckDB resolves only the last json_build_object
		// overload created by the compatibility macros.
		// {
		// 	query:       "SELECT json_build_object('min', 1, 'max', 2) AS json_build_object",
		// 	columnNames: []string{"json_build_object"},
		// 	values:      []string{`{"max":2,"min":1}`},
		// },
		{
			query:       "SELECT jsonb_array_length('[1, 2, 3]'::jsonb) AS jsonb_array_length",
			columnNames: []string{"jsonb_array_length"},
			values:      []string{"3"},
		},
		{
			query:       `SELECT jsonb_pretty('{"key": "value"}'::jsonb) AS jsonb_pretty`,
			columnNames: []string{"jsonb_pretty"},
			values:      []string{"{\n    \"key\": \"value\"\n}"},
		},
		{
			query:       `SELECT json_array_elements('[{"key": "value1"}]') AS json_array_elements`,
			columnNames: []string{"json_array_elements"},
			values:      []string{`{"key":"value1"}`},
		},
		{
			query:       "SELECT TO_CHAR('2024-01-15 14:30:00'::timestamp, 'YYYY-MM-DD') AS to_char",
			columnNames: []string{"to_char"},
			values:      []string{"2024-01-15"},
		},
	}

	for _, test := range tests {
		t.Run(test.query, func(t *testing.T) {
			assertQueryResponse(t, queryHandler, test)
		})
	}
}

func TestHandleQueryExpressions(t *testing.T) {
	queryHandler := initQueryHandlerForTest(t)

	tests := []queryResponseExpectation{
		{
			query:       "SELECT word FROM (VALUES ('abort', 'U', 't', 'unreserved', 'can be bare label')) t(word, catcode, barelabel, catdesc, baredesc) WHERE word <> ALL('{a,abs,absolute,action}'::text[])",
			columnNames: []string{"word"},
			values:      []string{"abort"},
		},
		{
			query:       "SELECT NULL::text AS word",
			columnNames: []string{"word"},
			values:      []string{""},
		},
		{
			query:       "SELECT t.x FROM (VALUES (1::int2, 'pg_type'::regclass)) t(x, y)",
			columnNames: []string{"x"},
			values:      []string{"1"},
		},
		{
			query:       "SELECT 'pg_catalog.array_in'::regproc AS regproc",
			columnNames: []string{"regproc"},
			values:      []string{"array_in"},
		},
		{
			query:       "SELECT '1 week'::interval AS interval",
			columnNames: []string{"interval"},
			values:      []string{"0 months 7 days 0 microseconds"},
		},
		{
			query:       `SELECT '{"key": "value"}'::jsonb AS jsonb`,
			columnNames: []string{"jsonb"},
			values:      []string{`{"key":"value"}`},
		},
		{
			query:       "SELECT date_trunc('month', '2025-02-24 15:58:23-05'::timestamptz + '-1 month'::interval) AS date",
			columnNames: []string{"date"},
			values:      []string{"2025-01-01 00:00:00+00:00"},
		},
		{
			query:       "SELECT 'foo'::pg_catalog.text AS text",
			columnNames: []string{"text"},
			values:      []string{"foo"},
		},
		{
			query:       "SELECT 1::pg_catalog.regtype::pg_catalog.text AS text",
			columnNames: []string{"text"},
			values:      []string{"1"},
		},
		{
			query:       "SELECT 1 AS value ORDER BY 1::pg_catalog.regclass::pg_catalog.text",
			columnNames: []string{"value"},
			values:      []string{"1"},
		},
		{
			query:       "SELECT stxnamespace::pg_catalog.regnamespace::pg_catalog.text AS text FROM pg_catalog.pg_statistic_ext",
			columnNames: []string{"text"},
		},
		{
			query:       "SELECT * FROM pg_catalog.pg_get_keywords() LIMIT 1",
			columnNames: []string{"word", "catcode", "barelabel", "catdesc", "baredesc"},
			values:      []string{"abort", "U", "t", "unreserved", "can be bare label"},
		},
		{
			query:       "SELECT pg_get_keywords.word FROM pg_catalog.pg_get_keywords() LIMIT 1",
			columnNames: []string{"word"},
			values:      []string{"abort"},
		},
		{
			query:       "SELECT * FROM generate_series(1, 2) AS series(index) LIMIT 1",
			columnNames: []string{"index"},
			values:      []string{"1"},
		},
		{
			query:       "SELECT * FROM generate_series(1, array_upper(current_schemas(FALSE), 1)) AS series(index) LIMIT 1",
			columnNames: []string{"index"},
			values:      []string{"1"},
		},
		// Temporarily disabled: _pg_expandarray is registered as a DuckDB table
		// function, but PostgreSQL permits this composite-returning scalar syntax.
		// {
		// 	query:       "SELECT (information_schema._pg_expandarray(ARRAY[10])).n",
		// 	columnNames: []string{"n"},
		// 	values:      []string{"1"},
		// },
		// {
		// 	query:       "SELECT (information_schema._pg_expandarray(ARRAY[10])).x AS value",
		// 	columnNames: []string{"value"},
		// 	values:      []string{"10"},
		// },
		{
			query:       "SELECT s.usename, r.rolconfig FROM pg_catalog.pg_shadow s LEFT JOIN pg_catalog.pg_roles r ON s.usename = r.rolname",
			columnNames: []string{"usename", "rolconfig"},
			values:      []string{"user", ""},
		},
		{
			query:       "SELECT a.oid, pd.description FROM pg_catalog.pg_roles a LEFT JOIN pg_catalog.pg_shdescription pd ON a.oid = pd.objoid",
			columnNames: []string{"oid", "description"},
			values:      []string{"10", ""},
		},
		{
			query:       "SELECT (SELECT 1 FROM (SELECT 1 AS inner_val) JOIN (SELECT NULL) ON inner_val = indclass[1]) AS test FROM pg_index",
			columnNames: []string{"test"},
		},
		{
			query:       "SELECT CASE WHEN true THEN 'yes' ELSE 'no' END AS case",
			columnNames: []string{"case"},
			values:      []string{"yes"},
		},
		{
			query:       "SELECT CASE WHEN false THEN 'yes' ELSE 'no' END AS case",
			columnNames: []string{"case"},
			values:      []string{"no"},
		},
		{
			query:       "SELECT CASE WHEN true THEN 'one' WHEN false THEN 'two' ELSE 'three' END AS case",
			columnNames: []string{"case"},
			values:      []string{"one"},
		},
		{
			query:       "SELECT CASE WHEN (SELECT count(extname) FROM pg_catalog.pg_extension WHERE extname = 'bdr') > 0 THEN 'pgd' WHEN (SELECT count(*) FROM pg_replication_slots) > 0 THEN 'log' ELSE NULL END AS type",
			columnNames: []string{"type"},
			values:      []string{""},
		},
		{
			query:       "SELECT CASE WHEN TRUE THEN pg_catalog.pg_is_in_recovery() END AS case",
			columnNames: []string{"case"},
			values:      []string{"f"},
		},
		{
			query:       "SELECT CASE WHEN FALSE THEN true ELSE pg_catalog.pg_is_in_recovery() END AS case",
			columnNames: []string{"case"},
			values:      []string{"f"},
		},
		{
			query:       "SELECT CASE WHEN nsp.nspname = ANY('{information_schema}') THEN false ELSE true END AS db_support FROM pg_catalog.pg_namespace nsp LIMIT 1",
			columnNames: []string{"db_support"},
			values:      []string{"t"},
		},
		{
			query:       "SELECT gss_authenticated, encrypted FROM (SELECT false, false, false, false, false WHERE false) t(pid, gss_authenticated, principal, encrypted, credentials_delegated) WHERE pid = pg_backend_pid()",
			columnNames: []string{"gss_authenticated", "encrypted"},
		},
		{
			query:       "WITH RECURSIVE simple_cte AS (SELECT oid, rolname FROM pg_roles WHERE rolname = 'postgres' UNION ALL SELECT oid, rolname FROM pg_roles) SELECT * FROM simple_cte",
			columnNames: []string{"oid", "rolname"},
			values:      []string{"10", "user"},
		},
		{
			query:       "SELECT ARRAY(SELECT 1 FROM pg_enum ORDER BY enumsortorder) AS array",
			columnNames: []string{"array"},
			values:      []string{"{}"},
		},
		{
			query:       "SELECT pg_shadow.usename FROM pg_shadow",
			columnNames: []string{"usename"},
			values:      []string{"user"},
		},
		{
			query:       "SELECT pg_roles.rolname FROM pg_roles",
			columnNames: []string{"rolname"},
			values:      []string{"user"},
		},
		{
			query:       "SELECT pg_extension.extname FROM pg_extension",
			columnNames: []string{"extname"},
			values:      []string{"plpgsql"},
		},
		{
			query:       "SELECT pg_database.datname FROM pg_database",
			columnNames: []string{"datname"},
			values:      []string{"pgstack"},
		},
		{
			query:       "SELECT pg_inherits.inhrelid FROM pg_inherits",
			columnNames: []string{"inhrelid"},
		},
		{
			query:       "SELECT pg_shdescription.objoid FROM pg_shdescription",
			columnNames: []string{"objoid"},
		},
		{
			query:       "SELECT pg_statio_user_tables.relid FROM pg_statio_user_tables",
			columnNames: []string{"relid"},
		},
		{
			query:       "SELECT pg_replication_slots.slot_name FROM pg_replication_slots",
			columnNames: []string{"slot_name"},
		},
		{
			query:       "SELECT pg_stat_gssapi.pid FROM pg_stat_gssapi",
			columnNames: []string{"pid"},
		},
		{
			query:       "SELECT pg_auth_members.oid FROM pg_auth_members",
			columnNames: []string{"oid"},
		},
		{
			query:       "SELECT x.usename, (SELECT split_part(passwd, ':', 1) FROM pg_shadow WHERE usename = x.usename) AS password FROM pg_shadow x WHERE x.usename = 'user'",
			columnNames: []string{"usename", "password"},
			values:      []string{"user", "SCRAM-SHA-256$4096"},
		},
		// Temporarily disabled: the remaining expression cases reference the
		// postgres.test_table Iceberg fixture from BemiDB.
	}

	for _, test := range tests {
		t.Run(test.query, func(t *testing.T) {
			assertQueryResponse(t, queryHandler, test)
		})
	}
}

func TestHandleQueryShow(t *testing.T) {
	queryHandler := initQueryHandlerForTest(t)

	tests := []queryResponseExpectation{
		{query: "SHOW search_path", columnNames: []string{"search_path"}, values: []string{`"$user", public`}},
		{query: "SHOW timezone", columnNames: []string{"timezone"}, values: []string{"UTC"}},
	}
	for _, test := range tests {
		t.Run(test.query, func(t *testing.T) {
			assertQueryResponse(t, queryHandler, test)
		})
	}
}

func TestHandleParseQuery(t *testing.T) {
	queryHandler := initQueryHandlerForTest(t)

	t.Run("query", func(t *testing.T) {
		messages, preparedStatement, err := queryHandler.HandleParseQuery(&pgproto3.Parse{
			Query: "SELECT usename, passwd FROM pg_shadow WHERE usename=$1",
		})
		if err != nil {
			t.Fatalf("parse query: %v", err)
		}
		assertMessageTypes(t, messages, "*pgproto3.ParseComplete")
		if preparedStatement.Query != "SELECT usename, passwd FROM main.pg_shadow WHERE usename = $1" {
			t.Errorf("remapped query = %q", preparedStatement.Query)
		}
		if preparedStatement.Statement == nil {
			t.Error("prepared statement is nil")
		}
	})

	t.Run("empty query", func(t *testing.T) {
		messages, preparedStatement, err := queryHandler.HandleParseQuery(&pgproto3.Parse{})
		if err != nil {
			t.Fatalf("parse empty query: %v", err)
		}
		assertMessageTypes(t, messages, "*pgproto3.ParseComplete")
		if preparedStatement.Query != "" || preparedStatement.Statement != nil {
			t.Errorf("empty prepared statement = %#v", preparedStatement)
		}
	})
}

func TestHandleBindQuery(t *testing.T) {
	queryHandler := initQueryHandlerForTest(t)

	tests := []struct {
		name       string
		query      string
		parameter  []byte
		formatCode int16
		want       any
	}{
		{
			name:      "text parameter",
			query:     "SELECT usename FROM pg_shadow WHERE usename=$1",
			parameter: []byte("user"),
			want:      "user",
		},
		{
			name:       "4-byte binary parameter",
			query:      "SELECT c.oid FROM pg_catalog.pg_class c WHERE c.relnamespace=$1",
			parameter:  binary.BigEndian.AppendUint32(nil, 2200),
			formatCode: 1,
			want:       int32(2200),
		},
		{
			name:       "8-byte binary parameter",
			query:      "SELECT c.oid FROM pg_catalog.pg_class c WHERE c.relnamespace=$1",
			parameter:  binary.BigEndian.AppendUint64(nil, 2200),
			formatCode: 1,
			want:       int64(2200),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, preparedStatement, err := queryHandler.HandleParseQuery(&pgproto3.Parse{Query: test.query})
			if err != nil {
				t.Fatalf("parse query: %v", err)
			}
			messages, preparedStatement, err := queryHandler.HandleBindQuery(&pgproto3.Bind{
				Parameters:           [][]byte{test.parameter},
				ParameterFormatCodes: []int16{test.formatCode},
			}, preparedStatement)
			if err != nil {
				t.Fatalf("bind query: %v", err)
			}
			assertMessageTypes(t, messages, "*pgproto3.BindComplete")
			if len(preparedStatement.Variables) != 1 || preparedStatement.Variables[0] != test.want {
				t.Errorf("bound variables = %#v, want %#v", preparedStatement.Variables, test.want)
			}
		})
	}

	// Requires an Iceberg table fixture that is not available in this repository's unit-test setup.
	// t.Run("16-byte UUID parameter", ...)
}

func TestHandleTextArrayBindQuery(t *testing.T) {
	queryHandler := initQueryHandlerForTest(t)

	_, preparedStatement, err := queryHandler.HandleParseQuery(&pgproto3.Parse{
		Query: "SELECT 'sec,ond' = ANY($1::pg_catalog.text[])",
	})
	if err != nil {
		t.Fatalf("parse query: %v", err)
	}

	_, preparedStatement, err = queryHandler.HandleBindQuery(&pgproto3.Bind{
		Parameters: [][]byte{[]byte(`{first,"sec,ond"}`)},
	}, preparedStatement)
	if err != nil {
		t.Fatalf("bind query: %v", err)
	}

	messages, err := queryHandler.HandleExecuteQuery(&pgproto3.Execute{}, preparedStatement)
	if err != nil {
		t.Fatalf("execute query: %v", err)
	}
	assertMessageTypes(t, messages, "*pgproto3.DataRow", "*pgproto3.CommandComplete")
	assertDataRow(t, messages[0], []string{"t"})
}

func TestHandleDescribeAndExecuteQuery(t *testing.T) {
	queryHandler := initQueryHandlerForTest(t)

	t.Run("describe and execute", func(t *testing.T) {
		_, preparedStatement, err := queryHandler.HandleParseQuery(&pgproto3.Parse{
			Query: "SELECT usename, split_part(passwd, ':', 1) FROM pg_shadow WHERE usename=$1",
		})
		if err != nil {
			t.Fatalf("parse query: %v", err)
		}
		_, preparedStatement, err = queryHandler.HandleBindQuery(&pgproto3.Bind{
			Parameters: [][]byte{[]byte("user")},
		}, preparedStatement)
		if err != nil {
			t.Fatalf("bind query: %v", err)
		}
		messages, preparedStatement, err := queryHandler.HandleDescribeQuery(
			&pgproto3.Describe{ObjectType: 'P'},
			preparedStatement,
		)
		if err != nil {
			t.Fatalf("describe query: %v", err)
		}
		assertMessageTypes(t, messages, "*pgproto3.RowDescription")
		if preparedStatement.Rows == nil {
			t.Fatal("described prepared statement has no rows")
		}

		messages, err = queryHandler.HandleExecuteQuery(&pgproto3.Execute{}, preparedStatement)
		if err != nil {
			t.Fatalf("execute query: %v", err)
		}
		assertMessageTypes(t, messages, "*pgproto3.DataRow", "*pgproto3.CommandComplete")
		assertDataRow(t, messages[0], []string{"user", "SCRAM-SHA-256$4096"})
	})

	t.Run("empty query", func(t *testing.T) {
		_, preparedStatement, err := queryHandler.HandleParseQuery(&pgproto3.Parse{})
		if err != nil {
			t.Fatalf("parse query: %v", err)
		}
		_, preparedStatement, err = queryHandler.HandleBindQuery(&pgproto3.Bind{}, preparedStatement)
		if err != nil {
			t.Fatalf("bind query: %v", err)
		}
		messages, preparedStatement, err := queryHandler.HandleDescribeQuery(
			&pgproto3.Describe{ObjectType: 'P'},
			preparedStatement,
		)
		if err != nil {
			t.Fatalf("describe query: %v", err)
		}
		assertMessageTypes(t, messages, "*pgproto3.NoData")

		messages, err = queryHandler.HandleExecuteQuery(&pgproto3.Execute{}, preparedStatement)
		if err != nil {
			t.Fatalf("execute query: %v", err)
		}
		assertMessageTypes(t, messages, "*pgproto3.EmptyQueryResponse")
	})

	t.Run("describe statement without bind", func(t *testing.T) {
		_, preparedStatement, err := queryHandler.HandleParseQuery(&pgproto3.Parse{
			Query:         "SELECT usename FROM pg_shadow WHERE usename=$1",
			ParameterOIDs: []uint32{25},
		})
		if err != nil {
			t.Fatalf("parse query: %v", err)
		}
		messages, _, err := queryHandler.HandleDescribeQuery(
			&pgproto3.Describe{ObjectType: 'S'},
			preparedStatement,
		)
		if err != nil {
			t.Fatalf("describe query: %v", err)
		}
		assertMessageTypes(t, messages, "*pgproto3.NoData")
	})
}

func TestHandleMultipleQueries(t *testing.T) {
	queryHandler := initQueryHandlerForTest(t)

	t.Run("multiple SET statements", func(t *testing.T) {
		messages, err := queryHandler.HandleSimpleQuery(`SET client_encoding TO 'UTF8';
SET client_min_messages TO 'warning';
SET standard_conforming_strings = on;`)
		if err != nil {
			t.Fatalf("handle queries: %v", err)
		}
		assertMessageTypes(t, messages,
			"*pgproto3.CommandComplete",
			"*pgproto3.CommandComplete",
			"*pgproto3.CommandComplete",
		)
	})

	t.Run("mixed SET and SELECT statements", func(t *testing.T) {
		messages, err := queryHandler.HandleSimpleQuery(`SET client_encoding TO 'UTF8';
SELECT split_part(passwd, ':', 1) FROM pg_shadow WHERE usename='user';`)
		if err != nil {
			t.Fatalf("handle queries: %v", err)
		}
		assertMessageTypes(t, messages,
			"*pgproto3.CommandComplete",
			"*pgproto3.RowDescription",
			"*pgproto3.DataRow",
			"*pgproto3.CommandComplete",
		)
		assertDataRow(t, messages[2], []string{"SCRAM-SHA-256$4096"})
	})

	t.Run("multiple SELECT statements", func(t *testing.T) {
		messages, err := queryHandler.HandleSimpleQuery(`SELECT 1;
SELECT split_part(passwd, ':', 1) FROM pg_shadow WHERE usename='user';`)
		if err != nil {
			t.Fatalf("handle queries: %v", err)
		}
		assertMessageTypes(t, messages,
			"*pgproto3.RowDescription", "*pgproto3.DataRow", "*pgproto3.CommandComplete",
			"*pgproto3.RowDescription", "*pgproto3.DataRow", "*pgproto3.CommandComplete",
		)
		assertDataRow(t, messages[1], []string{"1"})
		assertDataRow(t, messages[4], []string{"SCRAM-SHA-256$4096"})
	})

	t.Run("error in one statement", func(t *testing.T) {
		_, err := queryHandler.HandleSimpleQuery(`SET client_encoding TO 'UTF8';
SELECT * FROM non_existent_table;
SET standard_conforming_strings = on;`)
		if err == nil || !strings.Contains(err.Error(), "non_existent_table") {
			t.Errorf("error = %v, want missing-table error", err)
		}
	})
}

func TestHandleSimpleQueryControlStatements(t *testing.T) {
	queryHandler := initQueryHandlerForTest(t)

	tests := []struct {
		name        string
		query       string
		messageType string
		commandTag  string
	}{
		{name: "SET", query: "SET client_encoding TO 'UTF8'", messageType: "*pgproto3.CommandComplete", commandTag: "SET"},
		{name: "DISCARD ALL", query: "DISCARD ALL", messageType: "*pgproto3.CommandComplete", commandTag: "DISCARD ALL"},
		{name: "BEGIN", query: "BEGIN", messageType: "*pgproto3.CommandComplete", commandTag: "BEGIN"},
		{name: "empty", query: "", messageType: "*pgproto3.EmptyQueryResponse"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			messages, err := queryHandler.HandleSimpleQuery(test.query)
			if err != nil {
				t.Fatalf("handle query: %v", err)
			}
			assertMessageTypes(t, messages, test.messageType)
			if test.commandTag != "" && string(messages[0].(*pgproto3.CommandComplete).CommandTag) != test.commandTag {
				t.Errorf("command tag = %q, want %q", messages[0].(*pgproto3.CommandComplete).CommandTag, test.commandTag)
			}
		})
	}
}

func assertQueryResponse(t *testing.T, queryHandler *QueryHandler, expectation queryResponseExpectation) {
	t.Helper()

	messages, err := queryHandler.HandleSimpleQuery(expectation.query)
	if err != nil {
		t.Fatalf("handle query: %v", err)
	}
	wantMessageCount := 2
	if expectation.values != nil {
		wantMessageCount = 3
	}
	if len(messages) != wantMessageCount {
		t.Fatalf("message count = %d, want %d", len(messages), wantMessageCount)
	}
	rowDescription, ok := messages[0].(*pgproto3.RowDescription)
	if !ok {
		t.Fatalf("first message type = %T, want *pgproto3.RowDescription", messages[0])
	}
	columnNames := make([]string, len(rowDescription.Fields))
	for i, field := range rowDescription.Fields {
		columnNames[i] = string(field.Name)
	}
	if !slices.Equal(columnNames, expectation.columnNames) {
		t.Errorf("column names = %v, want %v", columnNames, expectation.columnNames)
	}
	if expectation.values != nil {
		assertDataRow(t, messages[1], expectation.values)
	}
}

func assertMessageTypes(t *testing.T, messages []pgproto3.Message, expectedTypes ...string) {
	t.Helper()
	if len(messages) != len(expectedTypes) {
		t.Fatalf("message count = %d, want %d", len(messages), len(expectedTypes))
	}
	for i, expectedType := range expectedTypes {
		if actualType := fmt.Sprintf("%T", messages[i]); actualType != expectedType {
			t.Errorf("message %d type = %s, want %s", i, actualType, expectedType)
		}
	}
}

func assertDataRow(t *testing.T, message pgproto3.Message, expectedValues []string) {
	t.Helper()
	dataRow, ok := message.(*pgproto3.DataRow)
	if !ok {
		t.Fatalf("message type = %T, want *pgproto3.DataRow", message)
	}
	values := make([]string, len(dataRow.Values))
	for i, value := range dataRow.Values {
		values[i] = string(value)
	}
	if !slices.Equal(values, expectedValues) {
		t.Errorf("row values = %v, want %v", values, expectedValues)
	}
}

func initQueryHandlerForTest(t *testing.T) *QueryHandler {
	t.Helper()

	s3Server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		response.Header().Set("Content-Type", "application/xml")
		_, _ = response.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
	<Name>test</Name>
	<KeyCount>0</KeyCount>
	<MaxKeys>1000</MaxKeys>
	<IsTruncated>false</IsTruncated>
</ListBucketResult>`))
	}))
	t.Cleanup(s3Server.Close)

	config := &Config{
		AwsRegion:          "us-east-1",
		AwsS3Endpoint:      s3Server.URL,
		AwsS3Bucket:        "test",
		AwsAccessKeyId:     "test",
		AwsSecretAccessKey: "test",
		Database:           "pgstack",
		User:               "user",
		EncryptedPassword:  "SCRAM-SHA-256$4096:test",
		DuckDBMemoryLimit:  DEFAULT_DUCKDB_MEMORY_LIMIT,
	}

	bootQueries := slices.Concat(
		[]string{
			"SELECT oid FROM pg_catalog.pg_namespace",
			"CREATE SCHEMA " + PG_SCHEMA_PUBLIC,
		},
		CreatePgCatalogMacroQueries(config),
		CreateInformationSchemaMacroQueries(config),
		CreatePgCatalogTableQueries(config),
		// Synthetic catalog fixture tests SQL remapping independently of authentication policy.
		[]string{"CREATE OR REPLACE VIEW pg_shadow AS SELECT 'user' AS usename, 'SCRAM-SHA-256$4096:test' AS passwd"},
		CreateInformationSchemaTableQueries(config),
		[]string{"USE " + PG_SCHEMA_PUBLIC},
	)
	duckdbClient, err := NewDuckdbClient(config, bootQueries)
	if err != nil {
		t.Fatalf("initialize DuckDB: %v", err)
	}
	t.Cleanup(duckdbClient.Close)

	queryHandler, err := NewQueryHandler(config, duckdbClient)
	if err != nil {
		t.Fatalf("initialize query handler: %v", err)
	}
	return queryHandler
}
