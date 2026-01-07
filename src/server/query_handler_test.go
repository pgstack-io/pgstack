package main

import (
	"encoding/binary"
	"flag"
	"os"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/BemiHQ/BemiDB/src/common"
)

func TestHandleQuery(t *testing.T) {
	queryHandler := initQueryHandler()
	defer queryHandler.ServerDuckdbClient.Close()

	t.Run("PG functions", func(t *testing.T) {
		testResponseByQuery(t, queryHandler, map[string]map[string][]string{
			"SELECT VERSION()": {
				"description": {"version"},
				"types":       {uint32ToString(pgtype.TextOID)},
				"values":      {"PostgreSQL 17.0, compiled by BemiDB"},
			},
			"SELECT pg_catalog.pg_get_userbyid(p.proowner) AS owner, 'Foo' AS foo FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace LIMIT 1": {
				"description": {"owner", "foo"},
				"types":       {uint32ToString(pgtype.TextOID), uint32ToString(pgtype.TextOID)},
				"values":      {"user", "Foo"},
			},
			"SELECT QUOTE_IDENT('fooBar') AS quote_ident": {
				"description": {"quote_ident"},
				"types":       {uint32ToString(pgtype.TextOID)},
				"values":      {"\"fooBar\""},
			},
			"SELECT setting from pg_show_all_settings() WHERE name = 'default_null_order'": {
				"description": {"setting"},
				"types":       {uint32ToString(pgtype.TextOID)},
				"values":      {"nulls_last"},
			},
			"SELECT setting from pg_catalog.pg_show_all_settings() WHERE name = 'default_null_order'": {
				"description": {"setting"},
				"types":       {uint32ToString(pgtype.TextOID)},
				"values":      {"nulls_last"},
			},
			"SELECT pg_catalog.pg_get_partkeydef(c.oid) AS pg_get_partkeydef FROM pg_catalog.pg_class c LIMIT 1": {
				"description": {"pg_get_partkeydef"},
				"types":       {uint32ToString(pgtype.TextOID)},
				"values":      {""},
			},
			"SELECT pg_tablespace_location(t.oid) loc FROM pg_catalog.pg_tablespace": {
				"description": {"loc"},
				"types":       {uint32ToString(pgtype.TextOID)},
				"values":      {""},
			},
			"SELECT pg_catalog.pg_get_expr(adbin, drelid) AS def_value FROM pg_catalog.pg_attrdef": {
				"description": {"def_value"},
			},
			"SELECT pg_catalog.pg_get_expr(adbin, drelid, TRUE) AS def_value FROM pg_catalog.pg_attrdef": {
				"description": {"def_value"},
			},
			"SELECT pg_catalog.pg_get_viewdef(NULL, TRUE) AS viewdef": {
				"description": {"viewdef"},
				"types":       {uint32ToString(pgtype.TextOID)},
				"values":      {""},
			},
			"SELECT set_config('bytea_output', 'hex', false) AS set_config": {
				"description": {"set_config"},
				"types":       {uint32ToString(pgtype.TextOID)},
				"values":      {"hex"},
			},
			"SELECT pg_catalog.pg_encoding_to_char(6) AS pg_encoding_to_char": {
				"description": {"pg_encoding_to_char"},
				"types":       {uint32ToString(pgtype.TextOID)},
				"values":      {"UTF8"},
			},
			"SELECT pg_backend_pid() AS pg_backend_pid": {
				"description": {"pg_backend_pid"},
				"types":       {uint32ToString(pgtype.Int4OID)},
				"values":      {"0"},
			},
			"SELECT pg_cancel_backend(12345) AS pg_cancel_backend": {
				"description": {"pg_cancel_backend"},
				"types":       {uint32ToString(pgtype.BoolOID)},
				"values":      {"t"},
			},
			"SELECT * from pg_is_in_recovery()": {
				"description": {"pg_is_in_recovery"},
				"types":       {uint32ToString(pgtype.BoolOID)},
				"values":      {"f"},
			},
			"SELECT row_to_json(t) AS row_to_json FROM (SELECT usename FROM pg_shadow WHERE usename='user') t": {
				"description": {"row_to_json"},
				"types":       {uint32ToString(pgtype.JSONOID)},
				"values":      {`{"usename":"user"}`},
			},
			"SELECT current_setting('default_tablespace') AS current_setting": {
				"description": {"current_setting"},
				"types":       {uint32ToString(pgtype.TextOID)},
				"values":      {""},
			},
			"SELECT main.array_to_string('[1, 2, 3]', '') as str": {
				"description": {"str"},
				"types":       {uint32ToString(pgtype.TextOID)},
				"values":      {"123"},
			},
			"SELECT pg_catalog.array_to_string('[1, 2, 3]', '') as str": {
				"description": {"str"},
				"types":       {uint32ToString(pgtype.TextOID)},
				"values":      {"123"},
			},
			"SELECT array_to_string('[1, 2, 3]', '') as str": {
				"description": {"str"},
				"types":       {uint32ToString(pgtype.TextOID)},
				"values":      {"123"},
			},
			"SELECT * FROM pg_catalog.generate_series(1, 1)": {
				"description": {"generate_series"},
				"types":       {uint32ToString(pgtype.Int8OID)},
				"values":      {"1"},
			},
			"SELECT pg_catalog.aclexplode(db.datacl) AS d FROM pg_catalog.pg_database db": {
				"description": {"d"},
				"types":       {uint32ToString(pgtype.JSONOID)},
				"values":      {""},
			},
			"SELECT TRIM (BOTH '\"' FROM pg_catalog.pg_get_indexdef(1, 1, false)) AS trim": {
				"description": {"trim"},
				"types":       {uint32ToString(pgtype.TextOID)},
				"values":      {""},
			},
			"SELECT (d).grantee AS grantee, (d).grantor AS grantor, (d).is_grantable AS is_grantable, (d).privilege_type AS privilege_type FROM (SELECT pg_catalog.aclexplode(db.datacl) AS d FROM pg_catalog.pg_database db WHERE db.oid = 16388::OID) a": {
				"description": {"grantee", "grantor", "is_grantable", "privilege_type"},
				"types":       {uint32ToString(pgtype.JSONOID), uint32ToString(pgtype.JSONOID), uint32ToString(pgtype.JSONOID), uint32ToString(pgtype.JSONOID)},
				"values":      {"", "", "", ""},
			},
			"SELECT format('Hello %s, %s, %1$s', 'World', 'Earth') AS str": {
				"description": {"str"},
				"types":       {uint32ToString(pgtype.TextOID)},
				"values":      {"Hello World, Earth, World"},
			},
			"SELECT format('%s', \"postgres\".\"test_table\".\"varchar_column\") AS str FROM postgres.test_table WHERE varchar_column IS NOT NULL": {
				"description": {"str"},
				"types":       {uint32ToString(pgtype.TextOID)},
				"values":      {"varchar"},
			},
			"SELECT jsonb_extract_path_text(json_column, 'key') AS jsonb_extract_path_text FROM postgres.test_table WHERE json_column IS NOT NULL": {
				"description": {"jsonb_extract_path_text"},
				"types":       {uint32ToString(pgtype.TextOID)},
				"values":      {"value"},
			},
			"SELECT jsonb_extract_path_text(json_column, VARIADIC ARRAY['key']) AS jsonb_extract_path_text FROM postgres.test_table WHERE json_column IS NOT NULL": {
				"description": {"jsonb_extract_path_text"},
				"types":       {uint32ToString(pgtype.TextOID)},
				"values":      {"value"},
			},
			"SELECT encode(sha256('foo'), 'hex'::text) AS encode": {
				"description": {"encode"},
				"types":       {uint32ToString(pgtype.TextOID)},
				"values":      {"2c26b46b68ffc68ff99b453c1d30413413422d706483bfa0f98a5e886266e7ae"},
			},
			"SELECT JSONB_AGG(jsonb_column->'key') FILTER (WHERE jsonb_column->'key' IS NOT NULL) as nested_objects FROM postgres.test_table": {
				"description": {"nested_objects"},
				"types":       {uint32ToString(pgtype.JSONOID)},
				"values":      {"[\"value\"]"},
			},
			"SELECT string_agg(jsonb_column->'key') FILTER (WHERE jsonb_column->'key' IS NOT NULL) FROM postgres.test_table;": {
				"description": {"string_agg"},
				"types":       {uint32ToString(pgtype.TextOID)},
				"values":      {"\"value\""},
			},
			"SELECT jsonb_object_agg('key', 'value')": {
				"description": {"jsonb_object_agg"},
				"types":       {uint32ToString(pgtype.JSONOID)},
				"values":      {"{\"key\":\"value\"}"},
			},
			"SELECT json_build_object('min', 1, 'max', 2) AS json_build_object": {
				"description": {"json_build_object"},
				"types":       {uint32ToString(pgtype.JSONOID)},
				"values":      {"{\"max\":2,\"min\":1}"},
			},
			"SELECT pg_catalog.pg_get_statisticsobjdef_columns(1) AS pg_get_statisticsobjdef_columns": {
				"description": {"pg_get_statisticsobjdef_columns"},
				"types":       {uint32ToString(pgtype.Int4OID)},
				"values":      {""},
			},
			"SELECT pg_catalog.pg_relation_is_publishable('1') AS pg_relation_is_publishable": {
				"description": {"pg_relation_is_publishable"},
				"types":       {uint32ToString(pgtype.Int4OID)},
				"values":      {""},
			},
			"SELECT jsonb_array_length('[1, 2, 3]'::jsonb)": {
				"description": {"jsonb_array_length"},
				"types":       {uint32ToString(pgtype.Int4OID)},
				"values":      {"3"},
			},
			"SELECT jsonb_pretty('{\"key\": \"value\"}'::JSONB)": {
				"description": {"jsonb_pretty"},
				"types":       {uint32ToString(pgtype.TextOID)},
				"values":      {"{\n    \"key\": \"value\"\n}"},
			},
			"SELECT json_array_elements('[{\"key\": \"value1\"}]')": {
				"description": {"json_array_elements"},
				"types":       {uint32ToString(pgtype.JSONOID)},
				"values":      {"{\"key\":\"value1\"}"},
			},
			"SELECT value FROM json_array_elements('[{\"key\": \"value1\"}]')": {
				"description": {"value"},
				"types":       {uint32ToString(pgtype.JSONOID)},
				"values":      {"{\"key\":\"value1\"}"},
			},
			"SELECT foo FROM jsonb_array_elements('[{\"key\": \"value1\"}]') AS foo": {
				"description": {"foo"},
				"types":       {uint32ToString(pgtype.JSONOID)},
				"values":      {"{\"key\":\"value1\"}"},
			},
			"SELECT TO_CHAR('2024-01-15 14:30:00'::timestamp, 'YYYY-MM-DD')": {
				"description": {"to_char"},
				"types":       {uint32ToString(pgtype.TextOID)},
				"values":      {"2024-01-15"},
			},
		})
	})

	t.Run("PG system tables", func(t *testing.T) {
		testResponseByQuery(t, queryHandler, map[string]map[string][]string{
			"SELECT oid, typname AS typename FROM pg_type WHERE typname='geometry' OR typname='geography'": {
				"description": {"oid", "typename"},
				"types":       {uint32ToString(pgtype.OIDOID), uint32ToString(pgtype.TextOID)},
				"values":      {},
			},
			"SELECT relname FROM pg_catalog.pg_class WHERE relnamespace = (SELECT oid FROM pg_catalog.pg_namespace WHERE nspname = 'postgres' LIMIT 1) ORDER BY relname DESC LIMIT 1": {
				"description": {"relname"},
				"types":       {uint32ToString(pgtype.TextOID)},
				"values":      {"test_table"},
			},
			"SELECT oid FROM pg_catalog.pg_extension": {
				"description": {"oid"},
				"types":       {uint32ToString(pgtype.OIDOID)},
				"values":      {"13823"},
			},
			"SELECT slot_name FROM pg_replication_slots": {
				"description": {"slot_name"},
				"types":       {uint32ToString(pgtype.TextOID)},
				"values":      {},
			},
			"SELECT oid, datname, datdba FROM pg_catalog.pg_database where oid = 16388": {
				"description": {"oid", "datname", "datdba"},
				"types":       {uint32ToString(pgtype.OIDOID), uint32ToString(pgtype.TextOID), uint32ToString(pgtype.Int8OID)},
				"values":      {"16388", "bemidb", "10"},
			},
			"SELECT COALESCE(NULL, (SELECT datname FROM pg_database WHERE datname = 'bemidb')) AS datname": {
				"description": {"datname"},
				"types":       {uint32ToString(pgtype.TextOID)},
				"values":      {"bemidb"},
			},
			"SELECT COALESCE(NULL, '[]'::jsonb) AS json_value": {
				"description": {"json_value"},
				"types":       {uint32ToString(pgtype.JSONOID)},
				"values":      {"[]"},
			},
			"SELECT jsonb_array_length(COALESCE('[]'::jsonb, '{}'::jsonb)) AS length": {
				"description": {"length"},
				"types":       {uint32ToString(pgtype.Int4OID)},
				"values":      {"0"},
			},
			"SELECT * FROM pg_catalog.pg_stat_gssapi": {
				"description": {"pid", "gss_authenticated", "principal", "encrypted", "credentials_delegated"},
				"types":       {uint32ToString(pgtype.Int4OID), uint32ToString(pgtype.BoolOID), uint32ToString(pgtype.TextOID), uint32ToString(pgtype.BoolOID), uint32ToString(pgtype.BoolOID)},
				"values":      {},
			},
			"SELECT * FROM pg_catalog.pg_user": {
				"description": {"usename", "usesysid", "usecreatedb", "usesuper", "userepl", "usebypassrls", "passwd", "valuntil", "useconfig"},
				"types":       {uint32ToString(pgtype.TextOID)},
				"values":      {"user", "10", "t", "t", "t", "t", "", "", ""},
			},
			"SELECT datid FROM pg_catalog.pg_stat_activity": {
				"description": {"datid"},
				"types":       {uint32ToString(pgtype.Int8OID)},
				"values":      {},
			},
			"SELECT schemaname, matviewname AS objectname FROM pg_catalog.pg_matviews": {
				"description": {"schemaname", "objectname"},
				"types":       {uint32ToString(pgtype.TextOID), uint32ToString(pgtype.TextOID)},
				"values":      {},
			},
			"SELECT * FROM pg_catalog.pg_views": {
				"description": {"schemaname", "viewname", "viewowner", "definition"},
				"types":       {uint32ToString(pgtype.TextOID), uint32ToString(pgtype.TextOID), uint32ToString(pgtype.TextOID), uint32ToString(pgtype.TextOID)},
			},
			"SELECT oid FROM pg_collation": {
				"description": {"oid"},
				"types":       {uint32ToString(pgtype.OIDOID)},
				"values":      {"100"},
			},
			"SELECT * FROM pg_opclass": {
				"description": {"oid", "opcmethod", "opcname", "opcnamespace", "opcowner", "opcfamily", "opcintype", "opcdefault", "opckeytype"},
				"types":       {uint32ToString(pgtype.OIDOID), uint32ToString(pgtype.Int8OID), uint32ToString(pgtype.TextOID), uint32ToString(pgtype.Int8OID), uint32ToString(pgtype.Int8OID), uint32ToString(pgtype.Int8OID), uint32ToString(pgtype.Int8OID), uint32ToString(pgtype.BoolOID), uint32ToString(pgtype.Int8OID)},
			},
			"SELECT schemaname, relname, n_live_tup FROM pg_stat_user_tables WHERE schemaname = 'postgres' ORDER BY relname DESC LIMIT 1": {
				"description": {"schemaname", "relname", "n_live_tup"},
				"types":       {uint32ToString(pgtype.TextOID), uint32ToString(pgtype.TextOID), uint32ToString(pgtype.Int8OID)},
				"values":      {"postgres", "test_table", "1"},
			},
			"SELECT DISTINCT(nspname) FROM pg_catalog.pg_namespace WHERE nspname != 'information_schema' AND nspname != 'pg_catalog' ORDER BY nspname LIMIT 1": {
				"description": {"nspname"},
				"types":       {uint32ToString(pgtype.TextOID)},
				"values":      {"postgres"},
			},
			"SELECT nspname FROM pg_catalog.pg_namespace WHERE nspname == 'main'": {
				"description": {"nspname"},
				"types":       {uint32ToString(pgtype.TextOID)},
				"values":      {},
			},
			"SELECT n.nspname FROM pg_catalog.pg_namespace n LEFT OUTER JOIN pg_catalog.pg_description d ON d.objoid = n.oid ORDER BY n.nspname LIMIT 1": {
				"description": {"nspname"},
				"types":       {uint32ToString(pgtype.TextOID)},
				"values":      {"postgres"},
			},
			"SELECT rel.oid FROM pg_class rel LEFT JOIN pg_extension ON rel.oid = pg_extension.oid ORDER BY rel.oid LIMIT 1;": {
				"description": {"oid"},
				"types":       {uint32ToString(pgtype.OIDOID)},
				"values":      {"1988"},
			},
			"SELECT pg_total_relation_size(relid) AS total_size FROM pg_catalog.pg_statio_user_tables WHERE schemaname = 'postgres'": {
				"description": {"total_size"},
				"types":       {uint32ToString(pgtype.Int4OID)},
			},
			"SELECT pg_total_relation_size(relid) AS total_size FROM pg_catalog.pg_statio_user_tables WHERE schemaname = 'postgres' UNION SELECT NULL AS total_size FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace WHERE n.nspname = 'postgres'": {
				"description": {"total_size"},
				"types":       {uint32ToString(pgtype.Int4OID)},
			},
			"SELECT * FROM pg_catalog.pg_shdescription": {
				"description": {"objoid", "classoid", "description"},
				"types":       {uint32ToString(pgtype.OIDOID), uint32ToString(pgtype.OIDOID), uint32ToString(pgtype.TextOID)},
			},
			"SELECT * FROM pg_catalog.pg_roles": {
				"description": {"oid", "rolname", "rolsuper", "rolinherit", "rolcreaterole", "rolcreatedb", "rolcanlogin", "rolreplication", "rolconnlimit", "rolpassword", "rolvaliduntil", "rolbypassrls", "rolconfig"},
				"types": {
					uint32ToString(pgtype.OIDOID),
					uint32ToString(pgtype.TextOID),
					uint32ToString(pgtype.BoolOID),
					uint32ToString(pgtype.BoolOID),
					uint32ToString(pgtype.BoolOID),
					uint32ToString(pgtype.BoolOID),
					uint32ToString(pgtype.BoolOID),
					uint32ToString(pgtype.BoolOID),
					uint32ToString(pgtype.Int4OID),
					uint32ToString(pgtype.TextOID),
					uint32ToString(pgtype.TimestampOID),
					uint32ToString(pgtype.BoolOID),
					uint32ToString(pgtype.TextArrayOID),
				},
				"values": {"10", "user", "t", "t", "t", "t", "t", "f", "-1", "", "", "f", ""},
			},
			"SELECT * FROM pg_catalog.pg_inherits": {
				"description": {"inhrelid", "inhparent", "inhseqno", "inhdetachpending"},
			},
			"SELECT * FROM pg_auth_members": {
				"description": {"oid", "roleid", "member", "grantor", "admin_option", "inherit_option", "set_option"},
			},
			"SELECT ARRAY(select pg_get_indexdef(indexrelid, attnum, true) FROM pg_attribute WHERE attrelid = indexrelid ORDER BY attnum) AS expressions FROM pg_index": {
				"description": {"expressions"},
				"types":       {uint32ToString(pgtype.TextArrayOID)},
				"values":      {},
			},
			"SELECT indnullsnotdistinct FROM pg_index": {
				"description": {"indnullsnotdistinct"},
				"types":       {uint32ToString(pgtype.BoolOID)},
			},
			"SELECT n.nspname, c.relname FROM pg_catalog.pg_class c LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE c.relname OPERATOR(pg_catalog.~) '^(test_table)$' COLLATE pg_catalog.default AND n.nspname OPERATOR(pg_catalog.~) '^(postgres)$' COLLATE pg_catalog.default": {
				"description": {"nspname", "relname"},
				"types":       {uint32ToString(pgtype.TextOID), uint32ToString(pgtype.TextOID)},
				"values":      {"postgres", "test_table"},
			},
			"SELECT * FROM pg_catalog.pg_policy": {
				"description": {"oid", "polname", "polrelid", "polcmd", "polpermissive", "polroles", "polqual", "polwithcheck"},
				"types":       {uint32ToString(pgtype.OIDOID), uint32ToString(pgtype.TextOID), uint32ToString(pgtype.Int8OID), uint32ToString(pgtype.TextOID), uint32ToString(pgtype.BoolOID), uint32ToString(pgtype.Int8OID), uint32ToString(pgtype.TextOID), uint32ToString(pgtype.TextOID)},
			},
			"SELECT * FROM pg_catalog.pg_statistic_ext": {
				"description": {"oid", "stxrelid", "stxname", "stxnamespace", "stxowner", "stxstattarget", "stxkeys", "stxkind", "stxexprs"},
				"types":       {uint32ToString(pgtype.OIDOID), uint32ToString(pgtype.Int8OID), uint32ToString(pgtype.TextOID), uint32ToString(pgtype.Int8OID), uint32ToString(pgtype.Int8OID), uint32ToString(pgtype.Int4OID), uint32ToString(pgtype.Int8OID), uint32ToString(pgtype.TextOID), uint32ToString(pgtype.TextOID)},
			},
			"SELECT * FROM pg_catalog.pg_publication": {
				"description": {"oid", "pubname", "pubowner", "puballtables", "pubinsert", "pubupdate", "pubdelete", "pubtruncate", "pubviaroot"},
				"types":       {uint32ToString(pgtype.OIDOID), uint32ToString(pgtype.TextOID), uint32ToString(pgtype.Int8OID), uint32ToString(pgtype.BoolOID), uint32ToString(pgtype.BoolOID), uint32ToString(pgtype.BoolOID), uint32ToString(pgtype.BoolOID), uint32ToString(pgtype.BoolOID), uint32ToString(pgtype.BoolOID)},
			},
			"SELECT * FROM pg_catalog.pg_publication_rel": {
				"description": {"oid", "prpubid", "prrelid", "prqual", "prattrs"},
				"types":       {uint32ToString(pgtype.OIDOID), uint32ToString(pgtype.Int8OID), uint32ToString(pgtype.Int8OID), uint32ToString(pgtype.TextOID), uint32ToString(pgtype.TextOID)},
			},
			"SELECT * FROM pg_catalog.pg_publication_namespace": {
				"description": {"oid", "pnpubid", "pnnspid"},
				"types":       {uint32ToString(pgtype.OIDOID), uint32ToString(pgtype.Int8OID), uint32ToString(pgtype.Int8OID)},
			},
			"SELECT * FROM pg_catalog.pg_rewrite": {
				"description": {"oid", "rulename", "ev_class", "ev_type", "ev_enabled", "is_instead", "ev_qual", "ev_action"},
				"types":       {uint32ToString(pgtype.OIDOID), uint32ToString(pgtype.TextOID), uint32ToString(pgtype.Int8OID), uint32ToString(pgtype.TextOID), uint32ToString(pgtype.TextOID), uint32ToString(pgtype.BoolOID), uint32ToString(pgtype.TextOID), uint32ToString(pgtype.TextOID)},
			},
			"SELECT pubname, NULL, NULL FROM pg_catalog.pg_publication p JOIN pg_catalog.pg_publication_namespace pn ON p.oid = pn.pnpubid JOIN pg_catalog.pg_class pc ON pc.relnamespace = pn.pnnspid UNION SELECT pubname, pg_get_expr(pr.prqual, c.oid), (CASE WHEN pr.prattrs IS NOT NULL THEN (SELECT string_agg(attname, ', ') FROM pg_catalog.generate_series(0, pg_catalog.array_upper(pr.prattrs::pg_catalog.int2[], 1)) s, pg_catalog.pg_attribute WHERE attrelid = pr.prrelid AND attnum = prattrs[s]) ELSE NULL END) FROM pg_catalog.pg_publication p JOIN pg_catalog.pg_publication_rel pr ON p.oid = pr.prpubid JOIN pg_catalog.pg_class c ON c.oid = pr.prrelid UNION SELECT pubname, NULL, NULL FROM pg_catalog.pg_publication p ORDER BY 1": {
				"description": {"pubname", "NULL", "NULL"},
				"types":       {uint32ToString(pgtype.TextOID), uint32ToString(pgtype.TextOID), uint32ToString(pgtype.TextOID)},
			},
			"SELECT * FROM user": {
				"description": {"user"},
				"types":       {uint32ToString(pgtype.TextOID)},
				"values":      {"user"},
			},
			"SELECT oid FROM pg_type WHERE typname = 'text'": {
				"description": {"oid"},
				"types":       {uint32ToString(pgtype.OIDOID)},
				"values":      {"25"},
			},
			"SELECT DISTINCT ON(typlen) oid FROM pg_type ORDER BY oid LIMIT 1": {
				"description": {"oid"},
				"types":       {uint32ToString(pgtype.OIDOID)},
				"values":      {"16"},
			},
		})
	})

	t.Run("Information schema", func(t *testing.T) {
		testResponseByQuery(t, queryHandler, map[string]map[string][]string{
			"SELECT * FROM information_schema.tables WHERE table_schema = 'postgres' ORDER BY table_name DESC LIMIT 1": {
				"description": {"table_catalog", "table_schema", "table_name", "table_type", "self_referencing_column_name", "reference_generation", "user_defined_type_catalog", "user_defined_type_schema", "user_defined_type_name", "is_insertable_into", "is_typed", "commit_action"},
				"types":       {uint32ToString(pgtype.TextOID)},
				"values":      {"memory", "postgres", "test_table", "BASE TABLE", "", "", "", "", "", "YES", "NO", ""},
			},
			"SELECT * FROM information_schema.tables WHERE table_schema = 'postgres' /*BEMIDB_PERMISSIONS {\"postgres.test_table\": [\"id\"]} BEMIDB_PERMISSIONS*/": {
				"description": {"table_catalog", "table_schema", "table_name", "table_type", "self_referencing_column_name", "reference_generation", "user_defined_type_catalog", "user_defined_type_schema", "user_defined_type_name", "is_insertable_into", "is_typed", "commit_action"},
				"types":       {uint32ToString(pgtype.TextOID)},
				"values":      {"memory", "postgres", "test_table", "BASE TABLE", "", "", "", "", "", "YES", "NO", ""},
			},
			// information_schema.columns
			"SELECT udt_name FROM information_schema.columns WHERE table_schema = 'postgres' AND table_name = 'test_table' AND column_name = 'id'": {
				"description": {"udt_name"},
				"types":       {uint32ToString(pgtype.TextOID)},
				"values":      {"int4"},
			},
			"SELECT udt_name FROM information_schema.columns WHERE table_schema = 'postgres' AND table_name = 'test_table' AND column_name = 'bit_column'": {
				"description": {"udt_name"},
				"types":       {uint32ToString(pgtype.TextOID)},
				"values":      {"int4"},
			},
			"SELECT udt_name FROM information_schema.columns WHERE table_schema = 'postgres' AND table_name = 'test_table' AND column_name = 'bool_column'": {
				"description": {"udt_name"},
				"types":       {uint32ToString(pgtype.TextOID)},
				"values":      {"bool"},
			},
			"SELECT udt_name FROM information_schema.columns WHERE table_schema = 'postgres' AND table_name = 'test_table' AND column_name = 'bpchar_column'": {
				"description": {"udt_name"},
				"types":       {uint32ToString(pgtype.TextOID)},
				"values":      {"text"},
			},
			"SELECT udt_name FROM information_schema.columns WHERE table_schema = 'postgres' AND table_name = 'test_table' AND column_name = 'varchar_column'": {
				"description": {"udt_name"},
				"types":       {uint32ToString(pgtype.TextOID)},
				"values":      {"text"},
			},
			"SELECT udt_name FROM information_schema.columns WHERE table_schema = 'postgres' AND table_name = 'test_table' AND column_name = 'text_column'": {
				"description": {"udt_name"},
				"types":       {uint32ToString(pgtype.TextOID)},
				"values":      {"text"},
			},
			"SELECT udt_name FROM information_schema.columns WHERE table_schema = 'postgres' AND table_name = 'test_table' AND column_name = 'int2_column'": {
				"description": {"udt_name"},
				"types":       {uint32ToString(pgtype.TextOID)},
				"values":      {"int4"},
			},
			"SELECT udt_name FROM information_schema.columns WHERE table_schema = 'postgres' AND table_name = 'test_table' AND column_name = 'int4_column'": {
				"description": {"udt_name"},
				"types":       {uint32ToString(pgtype.TextOID)},
				"values":      {"int4"},
			},
			"SELECT udt_name FROM information_schema.columns WHERE table_schema = 'postgres' AND table_name = 'test_table' AND column_name = 'int8_column'": {
				"description": {"udt_name"},
				"types":       {uint32ToString(pgtype.TextOID)},
				"values":      {"numeric"},
			},
			"SELECT udt_name FROM information_schema.columns WHERE table_schema = 'postgres' AND table_name = 'test_table' AND column_name = 'hugeint_column'": {
				"description": {"udt_name"},
				"types":       {uint32ToString(pgtype.TextOID)},
				"values":      {"numeric"},
			},
			"SELECT udt_name FROM information_schema.columns WHERE table_schema = 'postgres' AND table_name = 'test_table' AND column_name = 'xid_column'": {
				"description": {"udt_name"},
				"types":       {uint32ToString(pgtype.TextOID)},
				"values":      {"int8"},
			},
			"SELECT udt_name FROM information_schema.columns WHERE table_schema = 'postgres' AND table_name = 'test_table' AND column_name = 'xid8_column'": {
				"description": {"udt_name"},
				"types":       {uint32ToString(pgtype.TextOID)},
				"values":      {"numeric"},
			},
			"SELECT udt_name FROM information_schema.columns WHERE table_schema = 'postgres' AND table_name = 'test_table' AND column_name = 'float4_column'": {
				"description": {"udt_name"},
				"types":       {uint32ToString(pgtype.TextOID)},
				"values":      {"float4"},
			},
			"SELECT udt_name FROM information_schema.columns WHERE table_schema = 'postgres' AND table_name = 'test_table' AND column_name = 'float8_column'": {
				"description": {"udt_name"},
				"types":       {uint32ToString(pgtype.TextOID)},
				"values":      {"float8"},
			},
			"SELECT udt_name FROM information_schema.columns WHERE table_schema = 'postgres' AND table_name = 'test_table' AND column_name = 'numeric_column'": {
				"description": {"udt_name"},
				"types":       {uint32ToString(pgtype.TextOID)},
				"values":      {"numeric"},
			},
			"SELECT udt_name FROM information_schema.columns WHERE table_schema = 'postgres' AND table_name = 'test_table' AND column_name = 'numeric_column_without_precision'": {
				"description": {"udt_name"},
				"types":       {uint32ToString(pgtype.TextOID)},
				"values":      {"numeric"},
			},
			"SELECT udt_name FROM information_schema.columns WHERE table_schema = 'postgres' AND table_name = 'test_table' AND column_name = 'date_column'": {
				"description": {"udt_name"},
				"types":       {uint32ToString(pgtype.TextOID)},
				"values":      {"date"},
			},
			"SELECT udt_name FROM information_schema.columns WHERE table_schema = 'postgres' AND table_name = 'test_table' AND column_name = 'time_column'": {
				"description": {"udt_name"},
				"types":       {uint32ToString(pgtype.TextOID)},
				"values":      {"time"},
			},
			"SELECT udt_name FROM information_schema.columns WHERE table_schema = 'postgres' AND table_name = 'test_table' AND column_name = 'timeMsColumn'": {
				"description": {"udt_name"},
				"types":       {uint32ToString(pgtype.TextOID)},
				"values":      {"time"},
			},
			"SELECT udt_name FROM information_schema.columns WHERE table_schema = 'postgres' AND table_name = 'test_table' AND column_name = 'timetz_column'": {
				"description": {"udt_name"},
				"types":       {uint32ToString(pgtype.TextOID)},
				"values":      {"time"},
			},
			"SELECT udt_name FROM information_schema.columns WHERE table_schema = 'postgres' AND table_name = 'test_table' AND column_name = 'timetz_ms_column'": {
				"description": {"udt_name"},
				"types":       {uint32ToString(pgtype.TextOID)},
				"values":      {"time"},
			},
			"SELECT udt_name FROM information_schema.columns WHERE table_schema = 'postgres' AND table_name = 'test_table' AND column_name = 'timestamp_column'": {
				"description": {"udt_name"},
				"types":       {uint32ToString(pgtype.TextOID)},
				"values":      {"timestamp"},
			},
			"SELECT udt_name FROM information_schema.columns WHERE table_schema = 'postgres' AND table_name = 'test_table' AND column_name = 'timestamp_ms_column'": {
				"description": {"udt_name"},
				"types":       {uint32ToString(pgtype.TextOID)},
				"values":      {"timestamp"},
			},
			"SELECT udt_name FROM information_schema.columns WHERE table_schema = 'postgres' AND table_name = 'test_table' AND column_name = 'timestamptz_column'": {
				"description": {"udt_name"},
				"types":       {uint32ToString(pgtype.TextOID)},
				"values":      {"timestamp"},
			},
			"SELECT udt_name FROM information_schema.columns WHERE table_schema = 'postgres' AND table_name = 'test_table' AND column_name = 'timestamptz_ms_column'": {
				"description": {"udt_name"},
				"types":       {uint32ToString(pgtype.TextOID)},
				"values":      {"timestamp"},
			},
			"SELECT udt_name FROM information_schema.columns WHERE table_schema = 'postgres' AND table_name = 'test_table' AND column_name = 'uuid_column'": {
				"description": {"udt_name"},
				"types":       {uint32ToString(pgtype.TextOID)},
				"values":      {"text"},
			},
			"SELECT udt_name FROM information_schema.columns WHERE table_schema = 'postgres' AND table_name = 'test_table' AND column_name = 'bytea_column'": {
				"description": {"udt_name"},
				"types":       {uint32ToString(pgtype.TextOID)},
				"values":      {"bytea"},
			},
			"SELECT udt_name FROM information_schema.columns WHERE table_schema = 'postgres' AND table_name = 'test_table' AND column_name = 'interval_column'": {
				"description": {"udt_name"},
				"types":       {uint32ToString(pgtype.TextOID)},
				"values":      {"numeric"},
			},
			"SELECT udt_name FROM information_schema.columns WHERE table_schema = 'postgres' AND table_name = 'test_table' AND column_name = 'point_column'": {
				"description": {"udt_name"},
				"types":       {uint32ToString(pgtype.TextOID)},
				"values":      {"text"},
			},
			"SELECT udt_name FROM information_schema.columns WHERE table_schema = 'postgres' AND table_name = 'test_table' AND column_name = 'inet_column'": {
				"description": {"udt_name"},
				"types":       {uint32ToString(pgtype.TextOID)},
				"values":      {"text"},
			},
			"SELECT udt_name FROM information_schema.columns WHERE table_schema = 'postgres' AND table_name = 'test_table' AND column_name = 'json_column'": {
				"description": {"udt_name"},
				"types":       {uint32ToString(pgtype.TextOID)},
				"values":      {"json"},
			},
			"SELECT udt_name FROM information_schema.columns WHERE table_schema = 'postgres' AND table_name = 'test_table' AND column_name = 'jsonb_column'": {
				"description": {"udt_name"},
				"types":       {uint32ToString(pgtype.TextOID)},
				"values":      {"json"},
			},
			"SELECT udt_name FROM information_schema.columns WHERE table_schema = 'postgres' AND table_name = 'test_table' AND column_name = 'tsvector_column'": {
				"description": {"udt_name"},
				"types":       {uint32ToString(pgtype.TextOID)},
				"values":      {"text"},
			},
			"SELECT udt_name FROM information_schema.columns WHERE table_schema = 'postgres' AND table_name = 'test_table' AND column_name = 'xml_column'": {
				"description": {"udt_name"},
				"types":       {uint32ToString(pgtype.TextOID)},
				"values":      {"text"},
			},
			"SELECT udt_name FROM information_schema.columns WHERE table_schema = 'postgres' AND table_name = 'test_table' AND column_name = 'pg_snapshot_column'": {
				"description": {"udt_name"},
				"types":       {uint32ToString(pgtype.TextOID)},
				"values":      {"text"},
			},
			"SELECT udt_name FROM information_schema.columns WHERE table_schema = 'postgres' AND table_name = 'test_table' AND column_name = 'array_text_column'": {
				"description": {"udt_name"},
				"types":       {uint32ToString(pgtype.TextOID)},
				"values":      {"_text"},
			},
			"SELECT udt_name FROM information_schema.columns WHERE table_schema = 'postgres' AND table_name = 'test_table' AND column_name = 'array_int_column'": {
				"description": {"udt_name"},
				"types":       {uint32ToString(pgtype.TextOID)},
				"values":      {"_int4"},
			},
			"SELECT udt_name FROM information_schema.columns WHERE table_schema = 'postgres' AND table_name = 'test_table' AND column_name = 'array_jsonb_column'": {
				"description": {"udt_name"},
				"types":       {uint32ToString(pgtype.TextOID)},
				"values":      {"_json"},
			},
			"SELECT udt_name FROM information_schema.columns WHERE table_schema = 'postgres' AND table_name = 'test_table' AND column_name = 'array_ltree_column'": {
				"description": {"udt_name"},
				"types":       {uint32ToString(pgtype.TextOID)},
				"values":      {"_text"},
			},
			"SELECT udt_name FROM information_schema.columns WHERE table_schema = 'postgres' AND table_name = 'test_table' AND column_name = 'user_defined_column'": {
				"description": {"udt_name"},
				"types":       {uint32ToString(pgtype.TextOID)},
				"values":      {"text"},
			},
			"SELECT udt_name FROM information_schema.columns WHERE table_schema = 'postgres' AND table_name = 'test_table' /*BEMIDB_PERMISSIONS {\"postgres.test_table\": [\"id\"]} BEMIDB_PERMISSIONS*/": {
				"description": {"udt_name"},
				"types":       {uint32ToString(pgtype.TextOID)},
				"values":      {"int4"},
			},
		})
	})

	t.Run("SHOW", func(t *testing.T) {
		testResponseByQuery(t, queryHandler, map[string]map[string][]string{
			"SHOW search_path": {
				"description": {"search_path"},
				"types":       {uint32ToString(pgtype.TextOID)},
				"values":      {`"$user", public`},
			},
			"SHOW timezone": {
				"description": {"timezone"},
				"types":       {uint32ToString(pgtype.TextOID)},
				"values":      {"UTC"},
			},
		})
	})

	t.Run("Iceberg tables", func(t *testing.T) {
		testResponseByQuery(t, queryHandler, map[string]map[string][]string{
			"SELECT COUNT(*) AS count FROM postgres.test_table": {
				"description": {"count"},
				"types":       {uint32ToString(pgtype.Int8OID)},
				"values":      {"2"},
			},
			"SELECT COUNT(DISTINCT postgres.test_table.id) AS count FROM postgres.test_table": {
				"description": {"count"},
				"types":       {uint32ToString(pgtype.Int8OID)},
				"values":      {"2"},
			},
			"SELECT x.id FROM postgres.test_table x WHERE x.id = 1": {
				"description": {"id"},
				"types":       {uint32ToString(pgtype.Int4OID)},
				"values":      {"1"},
			},
			"SELECT postgres.test_table.id FROM postgres.test_table WHERE postgres.test_table.id = 1": {
				"description": {"id"},
				"types":       {uint32ToString(pgtype.Int4OID)},
				"values":      {"1"},
			},
			"SELECT postgres.test_empty_table.id FROM postgres.test_empty_table": {
				"description": {"id"},
				"types":       {uint32ToString(pgtype.Int4OID)},
			},
			"SELECT postgres.test_table.id FROM postgres.test_table WHERE id = 1": {
				"description": {"id"},
				"types":       {uint32ToString(pgtype.Int4OID)},
				"values":      {"1"},
			},
			// TODO: add support for partitioned tables
			// "SELECT COUNT(*) FROM postgres.partitioned_table": {
			// 	"description": {"count"},
			// 	"types":       {uint32ToString(pgtype.Int8OID)},
			// 	"values":      {"3"},
			// },
		})
	})

	t.Run("Column types", func(t *testing.T) {
		testResponseByQuery(t, queryHandler, map[string]map[string][]string{
			"SELECT bit_column FROM postgres.test_table WHERE bit_column IS NOT NULL": {
				"description": {"bit_column"},
				"types":       {uint32ToString(pgtype.Int4OID)},
				"values":      {"1"},
			},
			"SELECT bit_column FROM postgres.test_table WHERE bit_column IS NULL": {
				"description": {"bit_column"},
				"types":       {uint32ToString(pgtype.Int4OID)},
				"values":      {""},
			},
			"SELECT bool_column FROM postgres.test_table WHERE bool_column = TRUE": {
				"description": {"bool_column"},
				"types":       {uint32ToString(pgtype.BoolOID)},
				"values":      {"t"},
			},
			"SELECT bool_column FROM postgres.test_table WHERE bool_column = FALSE": {
				"description": {"bool_column"},
				"types":       {uint32ToString(pgtype.BoolOID)},
				"values":      {"f"},
			},
			"SELECT bpchar_column FROM postgres.test_table WHERE bool_column = TRUE": {
				"description": {"bpchar_column"},
				"types":       {uint32ToString(pgtype.TextOID)},
				"values":      {"bpchar"},
			},
			"SELECT bpchar_column FROM postgres.test_table WHERE bool_column = FALSE": {
				"description": {"bpchar_column"},
				"types":       {uint32ToString(pgtype.TextOID)},
				"values":      {""},
			},
			"SELECT varchar_column FROM postgres.test_table WHERE varchar_column IS NOT NULL": {
				"description": {"varchar_column"},
				"types":       {uint32ToString(pgtype.TextOID)},
				"values":      {"varchar"},
			},
			"SELECT varchar_column FROM postgres.test_table WHERE varchar_column IS NULL": {
				"description": {"varchar_column"},
				"types":       {uint32ToString(pgtype.TextOID)},
				"values":      {""},
			},
			"SELECT text_column FROM postgres.test_table WHERE bool_column = TRUE": {
				"description": {"text_column"},
				"types":       {uint32ToString(pgtype.TextOID)},
				"values":      {"text"},
			},
			"SELECT text_column FROM postgres.test_table WHERE bool_column = FALSE": {
				"description": {"text_column"},
				"types":       {uint32ToString(pgtype.TextOID)},
				"values":      {""},
			},
			"SELECT int2_column FROM postgres.test_table WHERE bool_column = TRUE": {
				"description": {"int2_column"},
				"types":       {uint32ToString(pgtype.Int4OID)},
				"values":      {"32767"},
			},
			"SELECT int2_column FROM postgres.test_table WHERE bool_column = FALSE": {
				"description": {"int2_column"},
				"types":       {uint32ToString(pgtype.Int4OID)},
				"values":      {"-32767"},
			},
			"SELECT int4_column FROM postgres.test_table WHERE int4_column IS NOT NULL": {
				"description": {"int4_column"},
				"types":       {uint32ToString(pgtype.Int4OID)},
				"values":      {"2147483647"},
			},
			"SELECT int4_column FROM postgres.test_table WHERE int4_column IS NULL": {
				"description": {"int4_column"},
				"types":       {uint32ToString(pgtype.Int4OID)},
				"values":      {""},
			},
			"SELECT int8_column FROM postgres.test_table WHERE bool_column = TRUE": {
				"description": {"int8_column"},
				"types":       {uint32ToString(pgtype.NumericOID)},
				"values":      {"9.223372036854776e+18"},
			},
			"SELECT int8_column FROM postgres.test_table WHERE bool_column = FALSE": {
				"description": {"int8_column"},
				"types":       {uint32ToString(pgtype.NumericOID)},
				"values":      {"-9.223372036854776e+18"},
			},
			"SELECT hugeint_column FROM postgres.test_table WHERE hugeint_column IS NOT NULL": {
				"description": {"hugeint_column"},
				"types":       {uint32ToString(pgtype.NumericOID)},
				"values":      {"1e+19"},
			},
			"SELECT hugeint_column FROM postgres.test_table WHERE hugeint_column IS NULL": {
				"description": {"hugeint_column"},
				"types":       {uint32ToString(pgtype.NumericOID)},
				"values":      {""},
			},
			"SELECT xid_column FROM postgres.test_table WHERE xid_column IS NOT NULL": {
				"description": {"xid_column"},
				"types":       {uint32ToString(pgtype.Int8OID)},
				"values":      {"4294967295"},
			},
			"SELECT xid_column FROM postgres.test_table WHERE xid_column IS NULL": {
				"description": {"xid_column"},
				"types":       {uint32ToString(pgtype.Int8OID)},
				"values":      {""},
			},
			"SELECT xid8_column FROM postgres.test_table WHERE xid8_column IS NOT NULL": {
				"description": {"xid8_column"},
				"types":       {uint32ToString(pgtype.NumericOID)},
				"values":      {"1.8446744073709552e+19"},
			},
			"SELECT xid8_column FROM postgres.test_table WHERE xid8_column IS NULL": {
				"description": {"xid8_column"},
				"types":       {uint32ToString(pgtype.NumericOID)},
				"values":      {""},
			},
			"SELECT float4_column FROM postgres.test_table WHERE float4_column = 3.14": {
				"description": {"float4_column"},
				"types":       {uint32ToString(pgtype.Float4OID)},
				"values":      {"3.14"},
			},
			"SELECT float4_column FROM postgres.test_table WHERE float4_column != 3.14": {
				"description": {"float4_column"},
				"types":       {uint32ToString(pgtype.Float4OID)},
				"values":      {"0"},
			},
			"SELECT float8_column FROM postgres.test_table WHERE bool_column = TRUE": {
				"description": {"float8_column"},
				"types":       {uint32ToString(pgtype.Float8OID)},
				"values":      {"3.141592653589793"},
			},
			"SELECT float8_column FROM postgres.test_table WHERE bool_column = FALSE": {
				"description": {"float8_column"},
				"types":       {uint32ToString(pgtype.Float8OID)},
				"values":      {"-3.141592653589793"},
			},
			"SELECT numeric_column FROM postgres.test_table WHERE bool_column = TRUE": {
				"description": {"numeric_column"},
				"types":       {uint32ToString(pgtype.NumericOID)},
				"values":      {"12345.67"},
			},
			"SELECT numeric_column FROM postgres.test_table WHERE bool_column = FALSE": {
				"description": {"numeric_column"},
				"types":       {uint32ToString(pgtype.NumericOID)},
				"values":      {"-12345"},
			},
			"SELECT numeric_column_without_precision FROM postgres.test_table WHERE numeric_column_without_precision IS NOT NULL": {
				"description": {"numeric_column_without_precision"},
				"types":       {uint32ToString(pgtype.NumericOID)},
				"values":      {"12345.67"},
			},
			"SELECT numeric_column_without_precision FROM postgres.test_table WHERE numeric_column_without_precision IS NULL": {
				"description": {"numeric_column_without_precision"},
				"types":       {uint32ToString(pgtype.NumericOID)},
				"values":      {""},
			},
			"SELECT date_column FROM postgres.test_table ORDER BY date_column LIMIT 1": {
				"description": {"date_column"},
				"types":       {uint32ToString(pgtype.DateOID)},
				"values":      {"2024-01-01"},
			},
			"SELECT date_column FROM postgres.test_table ORDER BY date_column LIMIT 1 OFFSET 1": {
				"description": {"date_column"},
				"types":       {uint32ToString(pgtype.DateOID)},
				"values":      {"20025-11-12"},
			},
			"SELECT time_column FROM postgres.test_table WHERE bool_column = TRUE": {
				"description": {"time_column"},
				"types":       {uint32ToString(pgtype.TimeOID)},
				"values":      {"12:00:00.123456"},
			},
			"SELECT time_column FROM postgres.test_table WHERE bool_column = FALSE": {
				"description": {"time_column"},
				"types":       {uint32ToString(pgtype.TimeOID)},
				"values":      {"12:00:00.123"},
			},
			"SELECT timeMsColumn FROM postgres.test_table WHERE timeMsColumn IS NOT NULL": {
				"description": {"timeMsColumn"},
				"types":       {uint32ToString(pgtype.TimeOID)},
				"values":      {"12:00:00.123"},
			},
			"SELECT timeMsColumn FROM postgres.test_table WHERE timeMsColumn IS NULL": {
				"description": {"timeMsColumn"},
				"types":       {uint32ToString(pgtype.TimeOID)},
				"values":      {""},
			},
			"SELECT timetz_column FROM postgres.test_table WHERE bool_column = TRUE": {
				"description": {"timetz_column"},
				"types":       {uint32ToString(pgtype.TimeOID)},
				"values":      {"17:00:00.123456"},
			},
			"SELECT timetz_column FROM postgres.test_table WHERE bool_column = FALSE": {
				"description": {"timetz_column"},
				"types":       {uint32ToString(pgtype.TimeOID)},
				"values":      {"07:00:00.123"},
			},
			"SELECT timestamp_column FROM postgres.test_table WHERE bool_column = TRUE": {
				"description": {"timestamp_column"},
				"types":       {uint32ToString(pgtype.TimestampOID)},
				"values":      {"2024-01-01 12:00:00.123456"},
			},
			"SELECT timestamp_column FROM postgres.test_table WHERE bool_column = FALSE": {
				"description": {"timestamp_column"},
				"types":       {uint32ToString(pgtype.TimestampOID)},
				"values":      {"2024-01-01 12:00:00"},
			},
			"SELECT timestamp_ms_column FROM postgres.test_table WHERE timestamp_ms_column IS NOT NULL": {
				"description": {"timestamp_ms_column"},
				"types":       {uint32ToString(pgtype.TimestampOID)},
				"values":      {"2024-01-01 12:00:00.123"},
			},
			"SELECT timestamp_ms_column FROM postgres.test_table WHERE timestamp_ms_column IS NULL": {
				"description": {"timestamp_ms_column"},
				"types":       {uint32ToString(pgtype.TimestampOID)},
				"values":      {""},
			},
			"SELECT timestamptz_column FROM postgres.test_table WHERE bool_column = TRUE": {
				"description": {"timestamptz_column"},
				"types":       {uint32ToString(pgtype.TimestampOID)},
				"values":      {"2024-01-01 17:00:00.123456"},
			},
			"SELECT timestamptz_column FROM postgres.test_table WHERE bool_column = FALSE": {
				"description": {"timestamptz_column"},
				"types":       {uint32ToString(pgtype.TimestampOID)},
				"values":      {"2024-01-01 06:30:00.000123"},
			},
			"SELECT timestamptz_ms_column FROM postgres.test_table WHERE bool_column = TRUE": {
				"description": {"timestamptz_ms_column"},
				"types":       {uint32ToString(pgtype.TimestampOID)},
				"values":      {"2024-01-01 17:00:00.123"},
			},
			"SELECT timestamptz_ms_column FROM postgres.test_table WHERE bool_column = FALSE": {
				"description": {"timestamptz_ms_column"},
				"types":       {uint32ToString(pgtype.TimestampOID)},
				"values":      {"2024-01-01 07:00:00.12"},
			},
			"SELECT uuid_column FROM postgres.test_table WHERE uuid_column = '58a7c845-af77-44b2-8664-7ca613d92f04'": {
				"description": {"uuid_column"},
				"types":       {uint32ToString(pgtype.TextOID)},
				"values":      {"58a7c845-af77-44b2-8664-7ca613d92f04"},
			},
			"SELECT uuid_column FROM postgres.test_table WHERE uuid_column IS NULL": {
				"description": {"uuid_column"},
				"types":       {uint32ToString(pgtype.TextOID)},
				"values":      {""},
			},
			"SELECT bytea_column FROM postgres.test_table WHERE bytea_column IS NOT NULL": {
				"description": {"bytea_column"},
				"types":       {uint32ToString(pgtype.ByteaOID)},
				"values":      {"\\x48656c6c6f"},
			},
			"SELECT bytea_column FROM postgres.test_table WHERE bytea_column IS NULL": {
				"description": {"bytea_column"},
				"types":       {uint32ToString(pgtype.ByteaOID)},
				"values":      {""},
			},
			"SELECT interval_column FROM postgres.test_table WHERE interval_column IS NOT NULL": {
				"description": {"interval_column"},
				"types":       {uint32ToString(pgtype.NumericOID)},
				"values":      {"2.806201000001e+12"},
			},
			"SELECT interval_column FROM postgres.test_table WHERE interval_column IS NULL": {
				"description": {"interval_column"},
				"types":       {uint32ToString(pgtype.NumericOID)},
				"values":      {""},
			},
			"SELECT json_column FROM postgres.test_table WHERE json_column IS NOT NULL": {
				"description": {"json_column"},
				"types":       {uint32ToString(pgtype.TextOID)},
				"values":      {"{\"key\": \"value\"}"},
			},
			"SELECT json_column FROM postgres.test_table WHERE json_column IS NULL": {
				"description": {"json_column"},
				"types":       {uint32ToString(pgtype.TextOID)},
				"values":      {""},
			},
			"SELECT jsonb_column FROM postgres.test_table WHERE bool_column = TRUE": {
				"description": {"jsonb_column"},
				"types":       {uint32ToString(pgtype.TextOID)},
				"values":      {"{\"key\": \"value\", \"nestedKey\": { \"key\": \"value\" }}"},
			},
			"SELECT jsonb_column->'key' FROM postgres.test_table WHERE jsonb_column->'nestedKey'->>'key' = 'value'": {
				"description": {"jsonb_column_key"},
				"types":       {uint32ToString(pgtype.JSONOID)},
				"values":      {"\"value\""},
			},
			"SELECT json_column->>'key' FROM postgres.test_table WHERE id = 1 AND json_column::json->>'key' IN ('value')": {
				"description": {"json_column_key"},
				"types":       {uint32ToString(pgtype.TextOID)},
				"values":      {"value"},
			},
			"SELECT json_column ? 'key' AS exists FROM postgres.test_table WHERE id = 1": {
				"description": {"exists"},
				"types":       {uint32ToString(pgtype.BoolOID)},
				"values":      {"t"},
			},
			"SELECT jsonb_column FROM postgres.test_table WHERE bool_column = FALSE": {
				"description": {"jsonb_column"},
				"types":       {uint32ToString(pgtype.TextOID)},
				"values":      {"{}"},
			},
			"SELECT tsvector_column FROM postgres.test_table WHERE tsvector_column IS NOT NULL": {
				"description": {"tsvector_column"},
				"types":       {uint32ToString(pgtype.TextOID)},
				"values":      {"'sampl':1 'text':2 'tsvector':4"},
			},
			"SELECT tsvector_column FROM postgres.test_table WHERE tsvector_column IS NULL": {
				"description": {"tsvector_column"},
				"types":       {uint32ToString(pgtype.TextOID)},
				"values":      {""},
			},
			"SELECT xml_column FROM postgres.test_table WHERE xml_column IS NOT NULL": {
				"description": {"xml_column"},
				"types":       {uint32ToString(pgtype.TextOID)},
				"values":      {"<root><child>text</child></root>"},
			},
			"SELECT xml_column FROM postgres.test_table WHERE xml_column IS NULL": {
				"description": {"xml_column"},
				"types":       {uint32ToString(pgtype.TextOID)},
				"values":      {""},
			},
			"SELECT pg_snapshot_column FROM postgres.test_table WHERE pg_snapshot_column IS NOT NULL": {
				"description": {"pg_snapshot_column"},
				"types":       {uint32ToString(pgtype.TextOID)},
				"values":      {"1896:1896:"},
			},
			"SELECT pg_snapshot_column FROM postgres.test_table WHERE pg_snapshot_column IS NULL": {
				"description": {"pg_snapshot_column"},
				"types":       {uint32ToString(pgtype.TextOID)},
				"values":      {""},
			},
			"SELECT point_column FROM postgres.test_table WHERE point_column IS NOT NULL": {
				"description": {"point_column"},
				"types":       {uint32ToString(pgtype.TextOID)},
				"values":      {"(37.347301483154,45.002101898193)"},
			},
			"SELECT point_column FROM postgres.test_table WHERE point_column IS NULL": {
				"description": {"point_column"},
				"types":       {uint32ToString(pgtype.TextOID)},
				"values":      {""},
			},
			"SELECT inet_column FROM postgres.test_table WHERE inet_column IS NOT NULL": {
				"description": {"inet_column"},
				"types":       {uint32ToString(pgtype.TextOID)},
				"values":      {"192.168.0.1"},
			},
			"SELECT inet_column FROM postgres.test_table WHERE inet_column IS NULL": {
				"description": {"inet_column"},
				"types":       {uint32ToString(pgtype.TextOID)},
				"values":      {""},
			},
			"SELECT array_text_column FROM postgres.test_table WHERE array_text_column IS NOT NULL": {
				"description": {"array_text_column"},
				"types":       {uint32ToString(pgtype.TextArrayOID)},
				"values":      {"{one,two,three}"},
			},
			"SELECT array_text_column FROM postgres.test_table WHERE array_text_column IS NULL": {
				"description": {"array_text_column"},
				"types":       {uint32ToString(pgtype.TextArrayOID)},
				"values":      {""},
			},
			"SELECT array_int_column FROM postgres.test_table WHERE bool_column = TRUE": {
				"description": {"array_int_column"},
				"types":       {uint32ToString(pgtype.Int4ArrayOID)},
				"values":      {"{1,2,3}"},
			},
			"SELECT array_int_column FROM postgres.test_table WHERE bool_column = FALSE": {
				"description": {"array_int_column"},
				"types":       {uint32ToString(pgtype.Int4ArrayOID)},
				"values":      {"{}"},
			},
			"SELECT array_jsonb_column FROM postgres.test_table WHERE array_jsonb_column IS NOT NULL": {
				"description": {"array_jsonb_column"},
				"types":       {uint32ToString(pgtype.TextArrayOID)},
				"values":      {`{"{""key"": ""value1""}","{""key"": ""value2""}"}`},
			},
			"SELECT array_jsonb_column FROM postgres.test_table WHERE array_jsonb_column IS NULL": {
				"description": {"array_jsonb_column"},
				"types":       {uint32ToString(pgtype.TextArrayOID)},
				"values":      {""},
			},
			"SELECT array_ltree_column FROM postgres.test_table WHERE array_ltree_column IS NOT NULL": {
				"description": {"array_ltree_column"},
				"types":       {uint32ToString(pgtype.TextArrayOID)},
				"values":      {"{a.b,c.d}"},
			},
			"SELECT array_ltree_column FROM postgres.test_table WHERE array_ltree_column IS NULL": {
				"description": {"array_ltree_column"},
				"types":       {uint32ToString(pgtype.TextArrayOID)},
				"values":      {""},
			},
			"SELECT user_defined_column FROM postgres.test_table WHERE user_defined_column IS NOT NULL": {
				"description": {"user_defined_column"},
				"types":       {uint32ToString(pgtype.TextOID)},
				"values":      {"(Toronto)"},
			},
			"SELECT user_defined_column FROM postgres.test_table WHERE user_defined_column IS NULL": {
				"description": {"user_defined_column"},
				"types":       {uint32ToString(pgtype.TextOID)},
				"values":      {""},
			},
			"SELECT relforcerowsecurity FROM pg_catalog.pg_class LIMIT 1": {
				"description": {"relforcerowsecurity"},
				"types":       {uint32ToString(pgtype.BoolOID)},
				"values":      {"f"},
			},
		})
	})

	t.Run("Type casts", func(t *testing.T) {
		testResponseByQuery(t, queryHandler, map[string]map[string][]string{
			"SELECT '\"postgres\".\"test_table\"'::regclass::oid > 0 AS oid": {
				"description": {"oid"},
				"types":       {uint32ToString(pgtype.BoolOID)},
				"values":      {"t"},
			},
			"SELECT FORMAT('%I.%I', 'postgres', 'test_table')::regclass::oid > 0 AS oid": { // NOTE: ::regclass::oid on non-constants is not fully supported yet
				"description": {"oid"},
				"types":       {uint32ToString(pgtype.BoolOID)},
				"values":      {""},
			},
			"SELECT attrelid > 0 AS attrelid FROM pg_attribute WHERE attrelid = '\"postgres\".\"test_table\"'::regclass LIMIT 1": {
				"description": {"attrelid"},
				"types":       {uint32ToString(pgtype.BoolOID)},
				"values":      {"t"},
			},
			"SELECT COUNT(*) AS count FROM pg_attribute WHERE attrelid = '\"postgres\".\"test_table\"'::regclass": {
				"description": {"count"},
				"types":       {uint32ToString(pgtype.Int8OID)},
				"values":      {"40"},
			},
			"SELECT objoid, classoid, objsubid, description FROM pg_description WHERE classoid = 'pg_class'::regclass": {
				"description": {"objoid", "classoid", "objsubid", "description"},
				"types":       {uint32ToString(pgtype.OIDOID), uint32ToString(pgtype.TextOID), uint32ToString(pgtype.Int4OID), uint32ToString(pgtype.TextOID)},
				"values":      {},
			},
			"SELECT d.objoid, d.classoid, c.relname, d.description FROM pg_description d JOIN pg_class c ON d.classoid = 'pg_class'::regclass": {
				"description": {"objoid", "classoid", "relname", "description"},
				"types":       {uint32ToString(pgtype.OIDOID), uint32ToString(pgtype.TextOID), uint32ToString(pgtype.TextOID), uint32ToString(pgtype.TextOID)},
				"values":      {},
			},
			"SELECT objoid, classoid, objsubid, description FROM (SELECT * FROM pg_description WHERE classoid = 'pg_class'::regclass) d": {
				"description": {"objoid", "classoid", "objsubid", "description"},
				"types":       {uint32ToString(pgtype.OIDOID), uint32ToString(pgtype.TextOID), uint32ToString(pgtype.Int4OID), uint32ToString(pgtype.TextOID)},
				"values":      {},
			},
			"SELECT objoid, classoid, objsubid, description FROM pg_description WHERE (classoid = 'pg_class'::regclass AND objsubid = 0) OR classoid = 'pg_type'::regclass": {
				"description": {"objoid", "classoid", "objsubid", "description"},
				"types":       {uint32ToString(pgtype.OIDOID), uint32ToString(pgtype.TextOID), uint32ToString(pgtype.Int4OID), uint32ToString(pgtype.TextOID)},
				"values":      {},
			},
			"SELECT objoid, classoid, objsubid, description FROM pg_description WHERE classoid IN ('pg_class'::regclass, 'pg_type'::regclass)": {
				"description": {"objoid", "classoid", "objsubid", "description"},
				"types":       {uint32ToString(pgtype.OIDOID), uint32ToString(pgtype.TextOID), uint32ToString(pgtype.Int4OID), uint32ToString(pgtype.TextOID)},
				"values":      {},
			},
			"SELECT objoid FROM pg_description WHERE classoid = CASE WHEN true THEN 'pg_class'::regclass ELSE 'pg_type'::regclass END": {
				"description": {"objoid"},
				"types":       {uint32ToString(pgtype.OIDOID)},
				"values":      {},
			},
			"SELECT word FROM (VALUES ('abort', 'U', 't', 'unreserved', 'can be bare label')) t(word, catcode, barelabel, catdesc, baredesc) WHERE word <> ALL('{a,abs,absolute,action}'::text[])": {
				"description": {"word"},
				"types":       {uint32ToString(pgtype.TextOID)},
				"values":      {"abort"},
			},
			"SELECT NULL::text AS word": {
				"description": {"word"},
				"types":       {uint32ToString(pgtype.TextOID)},
				"values":      {""},
			},
			"SELECT t.x FROM (VALUES (1::int2, 'pg_type'::regclass)) t(x, y)": {
				"description": {"x"},
				"types":       {uint32ToString(pgtype.Int2OID)},
				"values":      {"1"},
			},
			"SELECT 'pg_catalog.array_in'::regproc AS regproc": {
				"description": {"regproc"},
				"types":       {uint32ToString(pgtype.TextOID)},
				"values":      {"array_in"},
			},
			"SELECT uuid_column FROM postgres.test_table WHERE uuid_column IN ('58a7c845-af77-44b2-8664-7ca613d92f04'::uuid)": {
				"description": {"uuid_column"},
				"types":       {uint32ToString(pgtype.TextOID)},
				"values":      {"58a7c845-af77-44b2-8664-7ca613d92f04"},
			},
			"SELECT '1 week'::INTERVAL AS interval": {
				"description": {"interval"},
				"types":       {uint32ToString(pgtype.IntervalOID)},
				"values":      {"0 months 7 days 0 microseconds"},
			},
			"SELECT '{\"key\": \"value\"}'::JSONB AS jsonb": {
				"description": {"jsonb"},
				"types":       {uint32ToString(pgtype.JSONOID)},
				"values":      {"{\"key\":\"value\"}"},
			},
			"SELECT date_trunc('month', '2025-02-24 15:58:23-05'::timestamptz + '-1 month'::interval) AS date": {
				"description": {"date"},
				"types":       {uint32ToString(pgtype.TimestamptzOID)},
				"values":      {"2025-01-01 00:00:00+00:00"},
			},
			"SELECT 'foo'::pg_catalog.text AS text": {
				"description": {"text"},
				"types":       {uint32ToString(pgtype.TextOID)},
				"values":      {"foo"},
			},
			"SELECT 1::pg_catalog.regtype::pg_catalog.text AS text": {
				"description": {"text"},
				"types":       {uint32ToString(pgtype.TextOID)},
				"values":      {"1"},
			},
			"SELECT 1 AS value ORDER BY 1::pg_catalog.regclass::pg_catalog.text": {
				"description": {"value"},
				"types":       {uint32ToString(pgtype.Int4OID)},
				"values":      {"1"},
			},
			"SELECT stxnamespace::pg_catalog.regnamespace::pg_catalog.text AS text FROM pg_catalog.pg_statistic_ext": {
				"description": {"text"},
				"types":       {uint32ToString(pgtype.TextOID)},
				"values":      {},
			},
			"SELECT relname FROM pg_catalog.pg_namespace, pg_catalog.pg_class LEFT JOIN pg_catalog.pg_description d ON (d.classoid = 'pg_class'::regclass) WHERE relname = 'test_table' AND nspname = 'postgres'": {
				"description": {"relname"},
				"types":       {uint32ToString(pgtype.TextOID)},
				"values":      {"test_table"},
			},
		})
	})

	t.Run("FROM function()", func(t *testing.T) {
		testResponseByQuery(t, queryHandler, map[string]map[string][]string{
			"SELECT * FROM pg_catalog.pg_get_keywords() LIMIT 1": {
				"description": {"word", "catcode", "barelabel", "catdesc", "baredesc"},
				"types":       {uint32ToString(pgtype.TextOID), uint32ToString(pgtype.TextOID), uint32ToString(pgtype.BoolOID), uint32ToString(pgtype.TextOID), uint32ToString(pgtype.TextOID)},
				"values":      {"abort", "U", "t", "unreserved", "can be bare label"},
			},
			"SELECT pg_get_keywords.word FROM pg_catalog.pg_get_keywords() LIMIT 1": {
				"description": {"word"},
				"types":       {uint32ToString(pgtype.TextOID)},
				"values":      {"abort"},
			},
			"SELECT * FROM generate_series(1, 2) AS series(index) LIMIT 1": {
				"description": {"index"},
				"types":       {uint32ToString(pgtype.Int8OID)},
				"values":      {"1"},
			},
			"SELECT * FROM generate_series(1, array_upper(current_schemas(FALSE), 1)) AS series(index) LIMIT 1": {
				"description": {"index"},
				"types":       {uint32ToString(pgtype.Int8OID)},
				"values":      {"1"},
			},
			"SELECT (information_schema._pg_expandarray(ARRAY[10])).n": {
				"description": {"n"},
				"types":       {uint32ToString(pgtype.Int8OID)},
				"values":      {"1"},
			},
			"SELECT (information_schema._pg_expandarray(ARRAY[10])).x AS value": {
				"description": {"value"},
				"types":       {uint32ToString(pgtype.Int4OID)},
				"values":      {"10"},
			},
		})
	})

	t.Run("JOIN", func(t *testing.T) {
		testResponseByQuery(t, queryHandler, map[string]map[string][]string{
			"SELECT s.usename, r.rolconfig FROM pg_catalog.pg_shadow s LEFT JOIN pg_catalog.pg_roles r ON s.usename = r.rolname": {
				"description": {"usename", "rolconfig"},
				"types":       {uint32ToString(pgtype.TextOID)},
				"values":      {"user", ""},
			},
			"SELECT a.oid, pd.description FROM pg_catalog.pg_roles a LEFT JOIN pg_catalog.pg_shdescription pd ON a.oid = pd.objoid": {
				"description": {"oid", "description"},
				"types":       {uint32ToString(pgtype.OIDOID), uint32ToString(pgtype.TextOID)},
				"values":      {"10", ""},
			},
			"SELECT (SELECT 1 FROM (SELECT 1 AS inner_val) JOIN (SELECT NULL) ON inner_val = indclass[1]) AS test FROM pg_index": {
				"description": {"test"},
				"types":       {uint32ToString(pgtype.Int4OID)},
			},
		})
	})

	t.Run("CASE", func(t *testing.T) {
		testResponseByQuery(t, queryHandler, map[string]map[string][]string{
			"SELECT CASE WHEN true THEN 'yes' ELSE 'no' END AS case": {
				"description": {"case"},
				"types":       {uint32ToString(pgtype.TextOID)},
				"values":      {"yes"},
			},
			"SELECT CASE WHEN false THEN 'yes' ELSE 'no' END AS case": {
				"description": {"case"},
				"types":       {uint32ToString(pgtype.TextOID)},
				"values":      {"no"},
			},
			"SELECT CASE WHEN true THEN 'one' WHEN false THEN 'two' ELSE 'three' END AS case": {
				"description": {"case"},
				"types":       {uint32ToString(pgtype.TextOID)},
				"values":      {"one"},
			},
			"SELECT CASE WHEN (SELECT count(extname) FROM pg_catalog.pg_extension WHERE extname = 'bdr') > 0 THEN 'pgd' WHEN (SELECT count(*) FROM pg_replication_slots) > 0 THEN 'log' ELSE NULL END AS type": {
				"description": {"type"},
				"types":       {uint32ToString(pgtype.TextOID)},
				"values":      {""},
			},
			"SELECT roles.oid AS id, roles.rolname AS name, roles.rolsuper AS is_superuser, CASE WHEN roles.rolsuper THEN true ELSE false END AS can_create_role FROM pg_catalog.pg_roles roles WHERE rolname = current_user": {
				"description": {"id", "name", "is_superuser", "can_create_role"},
				"types":       {uint32ToString(pgtype.Int8OID), uint32ToString(pgtype.TextOID), uint32ToString(pgtype.BoolOID), uint32ToString(pgtype.BoolOID)},
				"values":      {},
			},
			"SELECT roles.oid AS id, roles.rolname AS name, roles.rolsuper AS is_superuser, CASE WHEN roles.rolsuper THEN true ELSE roles.rolcreaterole END AS can_create_role FROM pg_catalog.pg_roles roles WHERE rolname = current_user": {
				"description": {"id", "name", "is_superuser", "can_create_role"},
				"types":       {uint32ToString(pgtype.Int8OID), uint32ToString(pgtype.TextOID), uint32ToString(pgtype.BoolOID), uint32ToString(pgtype.BoolOID)},
				"values":      {},
			},
			"SELECT CASE WHEN TRUE THEN pg_catalog.pg_is_in_recovery() END AS CASE": {
				"description": {"case"},
				"types":       {uint32ToString(pgtype.BoolOID)},
				"values":      {"f"},
			},
			"SELECT CASE WHEN FALSE THEN true ELSE pg_catalog.pg_is_in_recovery() END AS CASE": {
				"description": {"case"},
				"types":       {uint32ToString(pgtype.BoolOID)},
				"values":      {"f"},
			},
			"SELECT CASE WHEN nsp.nspname = ANY('{information_schema}') THEN false ELSE true END AS db_support FROM pg_catalog.pg_namespace nsp LIMIT 1": {
				"description": {"db_support"},
				"types":       {uint32ToString(pgtype.BoolOID)},
				"values":      {"t"},
			},
			"SELECT CASE WHEN FORMAT('%s', postgres.test_table.varchar_column) = 'varchar' THEN 1 ELSE 2 END AS test_case FROM postgres.test_table WHERE varchar_column = 'varchar'": {
				"description": {"test_case"},
				"types":       {uint32ToString(pgtype.Int4OID)},
				"values":      {"1"},
			},
		})
	})

	t.Run("WHERE pg_function()", func(t *testing.T) {
		testResponseByQuery(t, queryHandler, map[string]map[string][]string{
			"SELECT gss_authenticated, encrypted FROM (SELECT false, false, false, false, false WHERE false) t(pid, gss_authenticated, principal, encrypted, credentials_delegated) WHERE pid = pg_backend_pid()": {
				"description": {"gss_authenticated", "encrypted"},
				"types":       {uint32ToString(pgtype.BoolOID), uint32ToString(pgtype.BoolOID)},
				"values":      {},
			},
		})
	})

	t.Run("WHERE with nested SELECT", func(t *testing.T) {
		testResponseByQuery(t, queryHandler, map[string]map[string][]string{
			"SELECT int2_column FROM postgres.test_table WHERE int2_column > 0 AND int2_column = (SELECT int2_column FROM postgres.test_table WHERE int2_column = 32767)": {
				"description": {"int2_column"},
				"types":       {uint32ToString(pgtype.Int4OID)},
				"values":      {"32767"},
			},
		})
	})

	t.Run("WHERE ANY(column reference)", func(t *testing.T) {
		testResponseByQuery(t, queryHandler, map[string]map[string][]string{
			"SELECT id FROM postgres.test_table WHERE 'one' = ANY(array_text_column)": {
				"description": {"id"},
				"types":       {uint32ToString(pgtype.Int4OID)},
				"values":      {"1"},
			},
		})
	})

	t.Run("WITH", func(t *testing.T) {
		testResponseByQuery(t, queryHandler, map[string]map[string][]string{
			"WITH RECURSIVE simple_cte AS (SELECT oid, rolname FROM pg_roles WHERE rolname = 'postgres' UNION ALL SELECT oid, rolname FROM pg_roles) SELECT * FROM simple_cte": {
				"description": {"oid", "rolname"},
				"types":       {uint32ToString(pgtype.OIDOID), uint32ToString(pgtype.TextOID)},
				"values":      {"10", "user"},
			},
		})
	})

	t.Run("ORDER BY", func(t *testing.T) {
		testResponseByQuery(t, queryHandler, map[string]map[string][]string{
			"SELECT ARRAY(SELECT 1 FROM pg_enum ORDER BY enumsortorder) AS array": {
				"description": {"array"},
				"types":       {uint32ToString(pgtype.Int4ArrayOID)},
				"values":      {"{}"},
			},
			"SELECT postgres.test_table.id FROM postgres.test_table ORDER BY postgres.test_table.id DESC LIMIT 1": {
				"description": {"id"},
				"types":       {uint32ToString(pgtype.Int4OID)},
				"values":      {"2"},
			},
		})
	})

	t.Run("GROUP BY", func(t *testing.T) {
		testResponseByQuery(t, queryHandler, map[string]map[string][]string{
			"SELECT MAX(id) AS max FROM postgres.test_table GROUP BY postgres.test_table.id ORDER BY max LIMIT 1": {
				"description": {"max"},
				"types":       {uint32ToString(pgtype.Int4OID)},
				"values":      {"1"},
			},
			`WITH schema AS (SELECT pg_namespace.nspname AS name FROM pg_namespace WHERE nspname != 'information_schema' AND nspname NOT LIKE 'pg\_%') SELECT schema.name AS schema FROM schema GROUP BY schema ORDER BY schema LIMIT 1`: {
				"description": {"schema_"},
				"types":       {uint32ToString(pgtype.TextOID)},
				"values":      {"postgres"},
			},
		})
	})

	t.Run("FROM table alias", func(t *testing.T) {
		testResponseByQuery(t, queryHandler, map[string]map[string][]string{
			"SELECT pg_shadow.usename FROM pg_shadow": {
				"description": {"usename"},
				"types":       {uint32ToString(pgtype.TextOID)},
				"values":      {"user"},
			},
			"SELECT pg_roles.rolname FROM pg_roles": {
				"description": {"rolname"},
				"types":       {uint32ToString(pgtype.TextOID)},
				"values":      {"user"},
			},
			"SELECT pg_extension.extname FROM pg_extension": {
				"description": {"extname"},
				"types":       {uint32ToString(pgtype.TextOID)},
				"values":      {"plpgsql"},
			},
			"SELECT pg_database.datname FROM pg_database": {
				"description": {"datname"},
				"types":       {uint32ToString(pgtype.TextOID)},
				"values":      {"bemidb"},
			},
			"SELECT pg_inherits.inhrelid FROM pg_inherits": {
				"description": {"inhrelid"},
				"types":       {uint32ToString(pgtype.Int8OID)},
				"values":      {},
			},
			"SELECT pg_shdescription.objoid FROM pg_shdescription": {
				"description": {"objoid"},
				"types":       {uint32ToString(pgtype.OIDOID)},
				"values":      {},
			},
			"SELECT pg_statio_user_tables.relid FROM pg_statio_user_tables": {
				"description": {"relid"},
				"types":       {uint32ToString(pgtype.Int8OID)},
				"values":      {},
			},
			"SELECT pg_replication_slots.slot_name FROM pg_replication_slots": {
				"description": {"slot_name"},
				"types":       {uint32ToString(pgtype.TextOID)},
				"values":      {},
			},
			"SELECT pg_stat_gssapi.pid FROM pg_stat_gssapi": {
				"description": {"pid"},
				"types":       {uint32ToString(pgtype.Int4OID)},
				"values":      {},
			},
			"SELECT pg_auth_members.oid FROM pg_auth_members": {
				"description": {"oid"},
				"types":       {uint32ToString(pgtype.TextOID)},
				"values":      {},
			},
		})
	})

	t.Run("Sublink", func(t *testing.T) {
		testResponseByQuery(t, queryHandler, map[string]map[string][]string{
			"SELECT x.usename, (SELECT split_part(passwd, ':', 1) FROM pg_shadow WHERE usename = x.usename) as password FROM pg_shadow x WHERE x.usename = 'user'": {
				"description": {"usename", "password"},
				"types":       {uint32ToString(pgtype.TextOID), uint32ToString(pgtype.TextOID)},
				"values":      {"user", "SCRAM-SHA-256$4096"},
			},
		})
	})

	t.Run("Type comparisons", func(t *testing.T) {
		testResponseByQuery(t, queryHandler, map[string]map[string][]string{
			"SELECT db.oid AS did, db.datname AS name, ta.spcname AS spcname, db.datallowconn, db.datistemplate AS is_template, pg_catalog.has_database_privilege(db.oid, 'CREATE') AS cancreate, datdba AS owner, descr.description FROM pg_catalog.pg_database db LEFT OUTER JOIN pg_catalog.pg_tablespace ta ON db.dattablespace = ta.oid LEFT OUTER JOIN pg_catalog.pg_shdescription descr ON (db.oid = descr.objoid AND descr.classoid = 'pg_database'::regclass) WHERE db.oid > 1145::OID OR db.datname IN ('postgres', 'edb') ORDER BY datname": {
				"description": {"did", "name", "spcname", "datallowconn", "is_template", "cancreate", "owner", "description"},
				"types": {
					uint32ToString(pgtype.OIDOID),
					uint32ToString(pgtype.TextOID),
					uint32ToString(pgtype.TextOID),
					uint32ToString(pgtype.BoolOID),
					uint32ToString(pgtype.BoolOID),
					uint32ToString(pgtype.BoolOID),
					uint32ToString(pgtype.Int8OID),
					uint32ToString(pgtype.TextOID),
				},
				"values": {"16388", "bemidb", "", "t", "f", "t", "10", ""},
			},
		})
	})

	t.Run("Returns an error if a table does not exist", func(t *testing.T) {
		_, err := queryHandler.HandleSimpleQuery("SELECT * FROM non_existent_table")

		if err == nil {
			t.Errorf("Expected an error, got nil")
		}

		expectedErrorMessage := strings.Join([]string{
			"Catalog Error: Table with name non_existent_table does not exist!",
			"Did you mean \"pg_statio_user_tables\"?",
			"",
			"LINE 1: SELECT * FROM non_existent_table",
			"                      ^",
		}, "\n")
		if err.Error() != expectedErrorMessage {
			t.Errorf("Expected the error to be '"+expectedErrorMessage+"', got %v", err.Error())
		}
	})

	t.Run("Returns an error if permission for a column is denied", func(t *testing.T) {
		_, err := queryHandler.HandleSimpleQuery("SELECT id, bit_column FROM postgres.test_table /*BEMIDB_PERMISSIONS {\"postgres.test_table\": [\"id\"]} BEMIDB_PERMISSIONS*/")

		if err == nil {
			t.Errorf("Expected an error, got nil")
		}

		expectedErrorMessage := strings.Join([]string{
			"Binder Error: Referenced column \"bit_column\" not found in FROM clause!",
			"Candidate bindings: \"id\"",
			"",
			"LINE 1: SELECT id, bit_column FROM (SELECT id FROM iceberg_scan('s3://bemidb...",
			"                   ^",
		}, "\n")
		if err.Error() != expectedErrorMessage {
			t.Errorf("Expected the error to be '"+expectedErrorMessage+"', got %v", err.Error())
		}
	})

	t.Run("Returns a result without a row description for SET queries", func(t *testing.T) {
		messages, err := queryHandler.HandleSimpleQuery("SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL READ UNCOMMITTED")

		testNoError(t, err)
		testMessageTypes(t, messages, []pgproto3.Message{
			&pgproto3.CommandComplete{},
		})
		testCommandCompleteTag(t, messages[0], "SET")
	})

	t.Run("Allows setting and querying timezone", func(t *testing.T) {
		queryHandler.HandleSimpleQuery("SET timezone = 'UTC'")

		messages, err := queryHandler.HandleSimpleQuery("show timezone")

		testNoError(t, err)
		testMessageTypes(t, messages, []pgproto3.Message{
			&pgproto3.RowDescription{},
			&pgproto3.DataRow{},
			&pgproto3.CommandComplete{},
		})
		testRowDescription(t, messages[0], []string{"timezone"}, []string{uint32ToString(pgtype.TextOID)})
		testDataRowValues(t, messages[1], []string{"UTC"})
		testCommandCompleteTag(t, messages[2], "SHOW")
	})

	t.Run("Handles an empty query", func(t *testing.T) {
		messages, err := queryHandler.HandleSimpleQuery("-- ping")

		testNoError(t, err)
		testMessageTypes(t, messages, []pgproto3.Message{
			&pgproto3.EmptyQueryResponse{},
		})
	})

	t.Run("Handles a DISCARD ALL query", func(t *testing.T) {
		messages, err := queryHandler.HandleSimpleQuery("DISCARD ALL")

		testNoError(t, err)
		testMessageTypes(t, messages, []pgproto3.Message{
			&pgproto3.CommandComplete{},
		})
		testCommandCompleteTag(t, messages[0], "DISCARD ALL")
	})

	t.Run("Handles a BEGIN query", func(t *testing.T) {
		messages, err := queryHandler.HandleSimpleQuery("BEGIN")

		testNoError(t, err)
		testMessageTypes(t, messages, []pgproto3.Message{
			&pgproto3.CommandComplete{},
		})
		testCommandCompleteTag(t, messages[0], "BEGIN")
	})
}

func TestHandleParseQuery(t *testing.T) {
	queryHandler := initQueryHandler()
	defer queryHandler.ServerDuckdbClient.Close()

	t.Run("Handles PARSE extended query step", func(t *testing.T) {
		query := "SELECT usename, passwd FROM pg_shadow WHERE usename=$1"
		message := &pgproto3.Parse{Query: query}

		messages, preparedStatement, err := queryHandler.HandleParseQuery(message)

		testNoError(t, err)
		testMessageTypes(t, messages, []pgproto3.Message{
			&pgproto3.ParseComplete{},
		})

		remappedQuery := "SELECT usename, passwd FROM main.pg_shadow WHERE usename = $1"
		if preparedStatement.Query != remappedQuery {
			t.Errorf("Expected the prepared statement query to be %v, got %v", remappedQuery, preparedStatement.Query)
		}
		if preparedStatement.Statement == nil {
			t.Errorf("Expected the prepared statement to have a statement")
		}
	})

	t.Run("Handles PARSE extended query step if query is empty", func(t *testing.T) {
		message := &pgproto3.Parse{Query: ""}

		messages, preparedStatement, err := queryHandler.HandleParseQuery(message)

		testNoError(t, err)
		testMessageTypes(t, messages, []pgproto3.Message{
			&pgproto3.ParseComplete{},
		})

		if preparedStatement.Query != "" {
			t.Errorf("Expected the prepared statement query to be empty, got %v", preparedStatement.Query)
		}
		if preparedStatement.Statement != nil {
			t.Errorf("Expected the prepared statement not to have a statement, got %v", preparedStatement.Statement)
		}
	})
}

func TestHandleBindQuery(t *testing.T) {
	queryHandler := initQueryHandler()
	defer queryHandler.ServerDuckdbClient.Close()

	t.Run("Handles BIND extended query step with text format parameter", func(t *testing.T) {
		parseMessage := &pgproto3.Parse{Query: "SELECT usename, passwd FROM pg_shadow WHERE usename=$1"}
		_, preparedStatement, err := queryHandler.HandleParseQuery(parseMessage)
		testNoError(t, err)

		bindMessage := &pgproto3.Bind{
			Parameters:           [][]byte{[]byte("user")},
			ParameterFormatCodes: []int16{0}, // Text format
		}
		messages, preparedStatement, err := queryHandler.HandleBindQuery(bindMessage, preparedStatement)

		testNoError(t, err)
		testMessageTypes(t, messages, []pgproto3.Message{
			&pgproto3.BindComplete{},
		})
		if len(preparedStatement.Variables) != 1 {
			t.Errorf("Expected the prepared statement to have 1 variable, got %v", len(preparedStatement.Variables))
		}
		if preparedStatement.Variables[0] != "user" {
			t.Errorf("Expected the prepared statement variable to be 'user', got %v", preparedStatement.Variables[0])
		}
	})

	t.Run("Handles BIND extended query step with binary format 4-byte parameter", func(t *testing.T) {
		parseMessage := &pgproto3.Parse{Query: "SELECT c.oid FROM pg_catalog.pg_class c WHERE c.relnamespace = $1"}
		_, preparedStatement, err := queryHandler.HandleParseQuery(parseMessage)
		testNoError(t, err)

		paramValue := int32(2200)
		paramBytes := make([]byte, 4)
		binary.BigEndian.PutUint32(paramBytes, uint32(paramValue))

		bindMessage := &pgproto3.Bind{
			Parameters:           [][]byte{paramBytes},
			ParameterFormatCodes: []int16{1}, // Binary format
		}
		messages, preparedStatement, err := queryHandler.HandleBindQuery(bindMessage, preparedStatement)

		testNoError(t, err)
		testMessageTypes(t, messages, []pgproto3.Message{
			&pgproto3.BindComplete{},
		})
		if len(preparedStatement.Variables) != 1 {
			t.Errorf("Expected the prepared statement to have 1 variable, got %v", len(preparedStatement.Variables))
		}
		if preparedStatement.Variables[0] != paramValue {
			t.Errorf("Expected the prepared statement variable to be %v, got %v", paramValue, preparedStatement.Variables[0])
		}
	})

	t.Run("Handles BIND extended query step with binary format 8-byte parameter", func(t *testing.T) {
		parseMessage := &pgproto3.Parse{Query: "SELECT c.oid FROM pg_catalog.pg_class c WHERE c.relnamespace = $1"}
		_, preparedStatement, err := queryHandler.HandleParseQuery(parseMessage)
		testNoError(t, err)

		paramValue := int64(2200)
		paramBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(paramBytes, uint64(paramValue))

		bindMessage := &pgproto3.Bind{
			Parameters:           [][]byte{paramBytes},
			ParameterFormatCodes: []int16{1}, // Binary format
		}
		messages, preparedStatement, err := queryHandler.HandleBindQuery(bindMessage, preparedStatement)

		testNoError(t, err)
		testMessageTypes(t, messages, []pgproto3.Message{
			&pgproto3.BindComplete{},
		})
		if len(preparedStatement.Variables) != 1 {
			t.Errorf("Expected the prepared statement to have 1 variable, got %v", len(preparedStatement.Variables))
		}
		if preparedStatement.Variables[0] != paramValue {
			t.Errorf("Expected the prepared statement variable to be %v, got %v", paramValue, preparedStatement.Variables[0])
		}
	})

	t.Run("Handles BIND extended query step with binary format 16-byte (uuid) parameter", func(t *testing.T) {
		parseMessage := &pgproto3.Parse{Query: "SELECT uuid_column FROM postgres.test_table WHERE uuid_column = $1"}
		_, preparedStatement, err := queryHandler.HandleParseQuery(parseMessage)
		testNoError(t, err)

		uuidParam := "58a7c845-af77-44b2-8664-7ca613d92f04"
		paramBytes, _ := uuid.Must(uuid.Parse(uuidParam)).MarshalBinary()

		bindMessage := &pgproto3.Bind{
			Parameters:           [][]byte{paramBytes},
			ParameterFormatCodes: []int16{1}, // Binary format
		}
		messages, preparedStatement, err := queryHandler.HandleBindQuery(bindMessage, preparedStatement)

		testNoError(t, err)
		testMessageTypes(t, messages, []pgproto3.Message{
			&pgproto3.BindComplete{},
		})
		if len(preparedStatement.Variables) != 1 {
			t.Errorf("Expected the prepared statement to have 1 variable, got %v", len(preparedStatement.Variables))
		}
		if preparedStatement.Variables[0] != uuidParam {
			t.Errorf("Expected the prepared statement variable to be %v, got %v", uuidParam, preparedStatement.Variables[0])
		}
	})
}

func TestHandleDescribeQuery(t *testing.T) {
	queryHandler := initQueryHandler()
	defer queryHandler.ServerDuckdbClient.Close()

	t.Run("Handles DESCRIBE extended query step", func(t *testing.T) {
		query := "SELECT usename, passwd FROM pg_shadow WHERE usename=$1"
		parseMessage := &pgproto3.Parse{Query: query}
		_, preparedStatement, _ := queryHandler.HandleParseQuery(parseMessage)
		bindMessage := &pgproto3.Bind{Parameters: [][]byte{[]byte("user")}}
		_, preparedStatement, _ = queryHandler.HandleBindQuery(bindMessage, preparedStatement)
		message := &pgproto3.Describe{ObjectType: 'P'}

		messages, preparedStatement, err := queryHandler.HandleDescribeQuery(message, preparedStatement)

		testNoError(t, err)
		testMessageTypes(t, messages, []pgproto3.Message{
			&pgproto3.RowDescription{},
		})
		testRowDescription(t, messages[0], []string{"usename", "passwd"}, []string{uint32ToString(pgtype.TextOID), uint32ToString(pgtype.TextOID)})
		if preparedStatement.Rows == nil {
			t.Errorf("Expected the prepared statement to have rows")
		}
	})

	t.Run("Handles DESCRIBE extended query step if query is empty", func(t *testing.T) {
		parseMessage := &pgproto3.Parse{Query: ""}
		_, preparedStatement, _ := queryHandler.HandleParseQuery(parseMessage)
		bindMessage := &pgproto3.Bind{}
		_, preparedStatement, _ = queryHandler.HandleBindQuery(bindMessage, preparedStatement)
		message := &pgproto3.Describe{ObjectType: 'P'}

		messages, _, err := queryHandler.HandleDescribeQuery(message, preparedStatement)

		testNoError(t, err)
		testMessageTypes(t, messages, []pgproto3.Message{
			&pgproto3.NoData{},
		})
	})

	t.Run("Handles DESCRIBE (Statement) extended query step if there was no BIND step", func(t *testing.T) {
		query := "SELECT usename, passwd FROM pg_shadow WHERE usename=$1"
		parseMessage := &pgproto3.Parse{Query: query, ParameterOIDs: []uint32{pgtype.TextOID}}
		_, preparedStatement, _ := queryHandler.HandleParseQuery(parseMessage)
		message := &pgproto3.Describe{ObjectType: 'S'}

		messages, _, err := queryHandler.HandleDescribeQuery(message, preparedStatement)

		testNoError(t, err)
		testMessageTypes(t, messages, []pgproto3.Message{
			&pgproto3.NoData{},
		})
	})
}

func TestHandleExecuteQuery(t *testing.T) {
	queryHandler := initQueryHandler()
	defer queryHandler.ServerDuckdbClient.Close()

	t.Run("Handles EXECUTE extended query step", func(t *testing.T) {
		query := "SELECT usename, split_part(passwd, ':', 1) FROM pg_shadow WHERE usename=$1"
		parseMessage := &pgproto3.Parse{Query: query}
		_, preparedStatement, _ := queryHandler.HandleParseQuery(parseMessage)
		bindMessage := &pgproto3.Bind{Parameters: [][]byte{[]byte("user")}}
		_, preparedStatement, _ = queryHandler.HandleBindQuery(bindMessage, preparedStatement)
		describeMessage := &pgproto3.Describe{ObjectType: 'P'}
		_, preparedStatement, _ = queryHandler.HandleDescribeQuery(describeMessage, preparedStatement)
		message := &pgproto3.Execute{}

		messages, err := queryHandler.HandleExecuteQuery(message, preparedStatement)

		testNoError(t, err)
		testMessageTypes(t, messages, []pgproto3.Message{
			&pgproto3.DataRow{},
			&pgproto3.CommandComplete{},
		})
		testDataRowValues(t, messages[0], []string{"user", "SCRAM-SHA-256$4096"})
	})

	t.Run("Handles EXECUTE extended query step if query is empty", func(t *testing.T) {
		parseMessage := &pgproto3.Parse{Query: ""}
		_, preparedStatement, _ := queryHandler.HandleParseQuery(parseMessage)
		bindMessage := &pgproto3.Bind{}
		_, preparedStatement, _ = queryHandler.HandleBindQuery(bindMessage, preparedStatement)
		describeMessage := &pgproto3.Describe{ObjectType: 'P'}
		_, preparedStatement, _ = queryHandler.HandleDescribeQuery(describeMessage, preparedStatement)
		message := &pgproto3.Execute{}

		messages, err := queryHandler.HandleExecuteQuery(message, preparedStatement)

		testNoError(t, err)
		testMessageTypes(t, messages, []pgproto3.Message{
			&pgproto3.EmptyQueryResponse{},
		})
	})
}

func TestHandleMultipleQueries(t *testing.T) {
	queryHandler := initQueryHandler()
	defer queryHandler.ServerDuckdbClient.Close()

	t.Run("Handles multiple SET statements", func(t *testing.T) {
		query := `SET client_encoding TO 'UTF8';
SET client_min_messages TO 'warning';
SET standard_conforming_strings = on;`

		messages, err := queryHandler.HandleSimpleQuery(query)

		testNoError(t, err)
		testMessageTypes(t, messages, []pgproto3.Message{
			&pgproto3.CommandComplete{},
			&pgproto3.CommandComplete{},
			&pgproto3.CommandComplete{},
		})
		testCommandCompleteTag(t, messages[0], "SET")
		testCommandCompleteTag(t, messages[1], "SET")
		testCommandCompleteTag(t, messages[2], "SET")
	})

	t.Run("Handles mixed SET and SELECT statements", func(t *testing.T) {
		query := `SET client_encoding TO 'UTF8';
SELECT split_part(passwd, ':', 1) FROM pg_shadow WHERE usename='user';`

		messages, err := queryHandler.HandleSimpleQuery(query)

		testNoError(t, err)
		testMessageTypes(t, messages, []pgproto3.Message{
			&pgproto3.CommandComplete{},
			&pgproto3.RowDescription{},
			&pgproto3.DataRow{},
			&pgproto3.CommandComplete{},
		})
		testCommandCompleteTag(t, messages[0], "SET")
		testDataRowValues(t, messages[2], []string{"SCRAM-SHA-256$4096"})
		testCommandCompleteTag(t, messages[3], "SELECT 1")
	})

	t.Run("Handles multiple SELECT statements", func(t *testing.T) {
		query := `SELECT 1;
SELECT split_part(passwd, ':', 1) FROM pg_shadow WHERE usename='user';`

		messages, err := queryHandler.HandleSimpleQuery(query)

		testNoError(t, err)
		testMessageTypes(t, messages, []pgproto3.Message{
			&pgproto3.RowDescription{},
			&pgproto3.DataRow{},
			&pgproto3.CommandComplete{},
			&pgproto3.RowDescription{},
			&pgproto3.DataRow{},
			&pgproto3.CommandComplete{},
		})
		testDataRowValues(t, messages[1], []string{"1"})
		testCommandCompleteTag(t, messages[2], "SELECT 1")
		testDataRowValues(t, messages[4], []string{"SCRAM-SHA-256$4096"})
		testCommandCompleteTag(t, messages[5], "SELECT 1")
	})

	t.Run("Handles error in any of multiple statements", func(t *testing.T) {
		query := `SET client_encoding TO 'UTF8';
SELECT * FROM non_existent_table;
SET standard_conforming_strings = on;`

		_, err := queryHandler.HandleSimpleQuery(query)

		if err == nil {
			t.Error("Expected an error for non-existent table, got nil")
			return
		}

		if !strings.Contains(err.Error(), "non_existent_table") {
			t.Errorf("Expected error message to contain 'non_existent_table', got: %s", err.Error())
		}
	})
}

func initQueryHandler() *QueryHandler {
	config := loadTestConfig()
	serverDuckdbClient := common.NewDuckdbClient(config.CommonConfig, duckdbBootQueris(config))
	return NewQueryHandler(config, serverDuckdbClient)
}

func loadTestConfig() *Config {
	setTestArgs([]string{})

	_config.CommonConfig.DisableAnonymousAnalytics = true

	return LoadConfig()
}

func setTestArgs(args []string) {
	os.Args = append([]string{"cmd"}, args...)
	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	registerFlags()
}

func testNoError(t *testing.T, err error) {
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
}

func testMessageTypes(t *testing.T, messages []pgproto3.Message, expectedTypes []pgproto3.Message) {
	if len(messages) != len(expectedTypes) {
		t.Errorf("Expected %v messages, got %v", len(expectedTypes), len(messages))
	}

	for i, expectedType := range expectedTypes {
		if reflect.TypeOf(messages[i]) != reflect.TypeOf(expectedType) {
			t.Errorf("Expected the %v message to be a %v", i, expectedType)
		}
	}
}

func testRowDescription(t *testing.T, rowDescriptionMessage pgproto3.Message, expectedColumnNames []string, expectedColumnTypes []string) {
	rowDescription := rowDescriptionMessage.(*pgproto3.RowDescription)

	if len(rowDescription.Fields) != len(expectedColumnNames) {
		t.Errorf("Expected %v row description fields, got %v", len(expectedColumnNames), len(rowDescription.Fields))
	}

	for i, expectedColumnName := range expectedColumnNames {
		if string(rowDescription.Fields[i].Name) != expectedColumnName {
			t.Errorf("Expected the %v row description field to be %v, got %v", i, expectedColumnName, string(rowDescription.Fields[i].Name))
		}
	}

	for i, expectedColumnType := range expectedColumnTypes {
		if uint32ToString(rowDescription.Fields[i].DataTypeOID) != expectedColumnType {
			t.Errorf("Expected the %v row description field data type to be %v, got %v", i, expectedColumnType, uint32ToString(rowDescription.Fields[i].DataTypeOID))
		}
	}
}

func testDataRowValues(t *testing.T, dataRowMessage pgproto3.Message, expectedValues []string) {
	dataRow := dataRowMessage.(*pgproto3.DataRow)

	if len(dataRow.Values) != len(expectedValues) {
		t.Errorf("Expected %v data row values, got %v", len(expectedValues), len(dataRow.Values))
	}

	for i, expectedValue := range expectedValues {
		if string(dataRow.Values[i]) != expectedValue {
			t.Errorf("Expected the %v data row value to be %v, got %v", i, expectedValue, string(dataRow.Values[i]))
		}
	}
}

func testCommandCompleteTag(t *testing.T, message pgproto3.Message, expectedTag string) {
	commandComplete := message.(*pgproto3.CommandComplete)
	if string(commandComplete.CommandTag) != expectedTag {
		t.Errorf("Expected the command tag to be %v, got %v", expectedTag, string(commandComplete.CommandTag))
	}
}

func testResponseByQuery(t *testing.T, queryHandler *QueryHandler, responseByQuery map[string]map[string][]string) {
	for query, responses := range responseByQuery {
		t.Run(query, func(t *testing.T) {
			messages, err := queryHandler.HandleSimpleQuery(query)

			testNoError(t, err)
			testRowDescription(t, messages[0], responses["description"], responses["types"])

			if len(responses["values"]) > 0 {
				testMessageTypes(t, messages, []pgproto3.Message{
					&pgproto3.RowDescription{},
					&pgproto3.DataRow{},
					&pgproto3.CommandComplete{},
				})
				testDataRowValues(t, messages[1], responses["values"])
			} else {
				testMessageTypes(t, messages, []pgproto3.Message{
					&pgproto3.RowDescription{},
					&pgproto3.CommandComplete{},
				})
			}
		})
	}
}

func uint32ToString(i uint32) string {
	return strconv.FormatUint(uint64(i), 10)
}
