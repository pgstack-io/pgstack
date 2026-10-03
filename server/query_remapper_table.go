package main

import (
	"context"
	"encoding/json"
	"regexp"
	"strings"

	pgQuery "github.com/pganalyze/pg_query_go/v6"
)

var PG_CATALOG_TABLE_NAMES = Set[string]{}

type QueryRemapperTable struct {
	parserTable        *ParserTable
	parserFunction     *ParserFunction
	remapperFunction   *QueryRemapperFunction
	icebergReader      *IcebergReader
	config             *Config
	ServerDuckdbClient *DuckdbClient // nilable

	// Runtime loaded
	IcebergPersistentSchemaTables Set[IcebergSchemaTable]
}

func NewQueryRemapperTable(config *Config, icebergReader *IcebergReader, serverDuckdbClient *DuckdbClient) *QueryRemapperTable {
	remapper := &QueryRemapperTable{
		parserTable:        NewParserTable(config),
		parserFunction:     NewParserFunction(config),
		remapperFunction:   NewQueryRemapperFunction(config, icebergReader),
		icebergReader:      icebergReader,
		ServerDuckdbClient: serverDuckdbClient,
		config:             config,
	}
	remapper.Reset()
	return remapper
}

// FROM / JOIN [TABLE]
func (remapper *QueryRemapperTable) RemapTable(node *pgQuery.Node) *pgQuery.Node {
	parser := remapper.parserTable
	qSchemaTable := parser.NodeToQuerySchemaTable(node)
	if qSchemaTable.Schema == PG_SCHEMA_SEARCH {
		var tables []serverSearchTable
		if remapper.config != nil {
			_ = json.Unmarshal([]byte(remapper.config.SearchTablesJSON), &tables)
		}
		columns := "*"
		for _, table := range tables {
			if strings.ReplaceAll(table.Name, ".", "_") == qSchemaTable.Table {
				quoted := make([]string, len(table.StoreColumns))
				for i, column := range table.StoreColumns {
					quoted[i] = quoteVectorIdentifier(column)
				}
				columns = strings.Join(quoted, ", ")
				break
			}
		}
		query := "SELECT " + columns + " FROM search_store.main." + quoteVectorIdentifier(qSchemaTable.Table) +
			" WHERE " + quoteVectorIdentifier(PGSTACK_CHUNK_INDEX_COLUMN) + " = 0"
		return parser.makeSubselectNode(query, qSchemaTable)
	}

	// pg_catalog.pg_* system tables
	if remapper.isTableFromPgCatalog(qSchemaTable) {
		switch qSchemaTable.Table {

		// pg_class -> reload Iceberg tables
		case PG_TABLE_PG_CLASS:
			remapper.reloadIcebergTables()

		// pg_stat_user_tables -> return Iceberg tables
		case PG_TABLE_PG_STAT_USER_TABLES:
			remapper.reloadIcebergTables()
			remapper.upsertPgStatUserTables()

		// pg_matviews -> reload Iceberg materialized views
		case PG_TABLE_PG_MATVIEWS:
		}

		// pg_catalog.[table] -> main.[table] for tables defined in CreatePgCatalogTableQueries
		if PG_CATALOG_TABLE_NAMES.Contains(qSchemaTable.Table) {
			parser.RemapSchemaToMain(node)
			return node
		}

		// pg_catalog.pg_* other system tables -> return as is
		return node
	}

	// information_schema.* system tables
	if parser.IsTableFromInformationSchema(qSchemaTable) {
		switch qSchemaTable.Table {
		// information_schema.tables -> (SELECT * FROM main.tables) information_schema_tables
		case PG_TABLE_TABLES:
			remapper.reloadIcebergTables()
			return parser.MakeInformationSchemaTablesNode(qSchemaTable)

		// information_schema.columns -> (SELECT * FROM main.columns) information_schema_columns
		case PG_TABLE_COLUMNS:
			return parser.MakeInformationSchemaColumnsNode(qSchemaTable)
		}

		// information_schema.* other system tables -> return as is
		return node
	}

	schemaTable := qSchemaTable.ToIcebergSchemaTable()
	if !remapper.IcebergPersistentSchemaTables.Contains(schemaTable) { // Reload Iceberg tables if not found
		remapper.reloadIcebergTables()
		if !remapper.IcebergPersistentSchemaTables.Contains(schemaTable) {
			return node // Let it return "Catalog Error: Table with name _ does not exist!"
		}
	}

	// public.table -> (SELECT * FROM iceberg_scan('s3://bucket/path')) table
	// schema.table -> (SELECT * FROM iceberg_scan('s3://bucket/path')) schema_table
	icebergPath := remapper.icebergReader.MetadataFileS3Path(schemaTable)           // iceberg/schema/table/metadata/v1.metadata.json
	icebergS3Path := remapper.icebergReader.S3Client.BucketS3Prefix() + icebergPath // s3://bucket/iceberg/schema/table/metadata/v1.metadata.json
	return parser.MakeIcebergTableNode(QueryToIcebergTable{
		QuerySchemaTable: qSchemaTable,
		IcebergTablePath: icebergS3Path,
	})
}

// FROM FUNCTION()
func (remapper *QueryRemapperTable) RemapTableFunctionCall(rangeFunction *pgQuery.RangeFunction) {
	schemaFunction := remapper.parserTable.TopLevelSchemaFunction(rangeFunction)
	if schemaFunction != nil {
		// SELECT value FROM jsonb_array_elements(...) value -> SELECT value FROM unnest(json_extract(..., '$[*]')) unnest(value)
		if (schemaFunction.Schema == PG_SCHEMA_PG_CATALOG || schemaFunction.Schema == "") &&
			(schemaFunction.Function == PG_FUNCTION_JSON_ARRAY_ELEMENTS || schemaFunction.Function == PG_FUNCTION_JSONB_ARRAY_ELEMENTS) {
			alias := remapper.parserTable.Alias(rangeFunction)
			if alias == "" {
				remapper.parserTable.SetAlias(rangeFunction, "unnest", "value")
			} else {
				remapper.parserTable.SetAlias(rangeFunction, "unnest", alias)
			}
		}

		remapper.parserTable.SetAliasIfNotExists(rangeFunction, schemaFunction.Function)
	}

	for _, functionCall := range remapper.parserTable.TableFunctionCalls(rangeFunction) {
		remapper.remapperFunction.RemapFunctionCall(functionCall)
		remapper.remapperFunction.RemapNestedFunctionCalls(functionCall) // recursion
	}
}

func (remapper *QueryRemapperTable) Reset() {
	remapper.IcebergPersistentSchemaTables = NewSet[IcebergSchemaTable]()

	remapper.reloadIcebergTables()
}

func (remapper *QueryRemapperTable) reloadIcebergTables() {
	if !remapper.config.AuditEnabled {
		return
	}
	remapper.reloadIcebergPersistentTables()
}

func (remapper *QueryRemapperTable) reloadIcebergPersistentTables() {
	newIcebergSchemaTables, err := remapper.icebergReader.SchemaTables()
	PanicIfError(remapper.config, err)

	previousIcebergSchemaTables := remapper.IcebergPersistentSchemaTables
	remapper.IcebergPersistentSchemaTables = newIcebergSchemaTables

	ctx := context.Background()
	// CREATE TABLE
	for _, icebergSchemaTable := range newIcebergSchemaTables.Values() {
		if !previousIcebergSchemaTables.Contains(icebergSchemaTable) {
			catalogTableColumns, err := remapper.icebergReader.TableColumns(icebergSchemaTable)
			PanicIfError(remapper.config, err)

			var sqlColumns []string
			for _, catalogTableColumn := range catalogTableColumns {
				sqlColumns = append(sqlColumns, catalogTableColumn.ToSql())
			}

			// Create schema and table
			_, err = remapper.ServerDuckdbClient.ExecContext(ctx, "CREATE SCHEMA IF NOT EXISTS "+icebergSchemaTable.Schema)
			PanicIfError(remapper.config, err)
			_, err = remapper.ServerDuckdbClient.ExecContext(ctx, "CREATE TABLE "+icebergSchemaTable.String()+" ("+strings.Join(sqlColumns, ", ")+")")
			PanicIfError(remapper.config, err)
		}
	}
	// DROP TABLE
	for _, icebergSchemaTable := range previousIcebergSchemaTables.Values() {
		if !newIcebergSchemaTables.Contains(icebergSchemaTable) {
			_, err = remapper.ServerDuckdbClient.ExecContext(ctx, "DROP TABLE "+icebergSchemaTable.String())
			PanicIfError(remapper.config, err)
		}
	}
}

func (remapper *QueryRemapperTable) upsertPgStatUserTables() {
	icebergSchemaTables := remapper.IcebergPersistentSchemaTables.Values()
	sqls := []string{"DELETE FROM pg_stat_user_tables"}
	if len(icebergSchemaTables) > 0 {
		values := make([]string, len(icebergSchemaTables))
		for i, icebergSchemaTable := range icebergSchemaTables {
			values[i] = "('123456', '" + icebergSchemaTable.Schema + "', '" + icebergSchemaTable.Table + "', 0, NULL, 0, 0, NULL, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, NULL, NULL, NULL, NULL, 0, 0, 0, 0)"
		}
		sqls = append(sqls, "INSERT INTO pg_stat_user_tables VALUES "+strings.Join(values, ", "))
	}
	err := remapper.ServerDuckdbClient.ExecTransactionContext(context.Background(), sqls)
	PanicIfError(remapper.config, err)
}

// System pg_* tables
func (remapper *QueryRemapperTable) isTableFromPgCatalog(qSchemaTable QuerySchemaTable) bool {
	return qSchemaTable.Schema == PG_SCHEMA_PG_CATALOG ||
		(qSchemaTable.Schema == "" &&
			(PG_SYSTEM_TABLES.Contains(qSchemaTable.Table) || PG_SYSTEM_VIEWS.Contains(qSchemaTable.Table)) &&
			!remapper.IcebergPersistentSchemaTables.Contains(qSchemaTable.ToIcebergSchemaTable()))
}

func extractTableNames(tables []string) Set[string] {
	names := make(Set[string])
	re := regexp.MustCompile(`CREATE (TABLE|VIEW) (\w+)`)

	for _, table := range tables {
		matches := re.FindStringSubmatch(table)
		if len(matches) > 1 {
			names.Add(matches[2])
		}
	}

	return names
}

func CreatePgCatalogTableQueries(config *Config) []string {
	result := []string{
		// Static empty tables
		"CREATE TABLE pg_constraint(oid oid, conname text, connamespace oid, contype char, condeferrable bool, condeferred bool, convalidated bool, conrelid oid, contypid oid, conindid oid, conparentid oid, confrelid oid, confupdtype char, confdeltype char, confmatchtype char, conislocal bool, coninhcount int4, connoinherit bool, conkey int2[], confkey int2[], conpfeqop oid[], conppeqop oid[], conffeqop oid[], confdelsetcols int2[], conexclop oid[], conbin text)",
		"CREATE TABLE pg_inherits(inhrelid oid, inhparent oid, inhseqno int4, inhdetachpending bool)",
		"CREATE TABLE pg_shdescription(objoid oid, classoid oid, description text)",
		"CREATE TABLE pg_statio_user_tables(relid oid, schemaname text, relname text, heap_blks_read int8, heap_blks_hit int8, idx_blks_read int8, idx_blks_hit int8, toast_blks_read int8, toast_blks_hit int8, tidx_blks_read int8, tidx_blks_hit int8)",
		"CREATE TABLE pg_replication_slots(slot_name text, plugin text, slot_type text, datoid oid, database text, temporary bool, active bool, active_pid int4, xmin int8, catalog_xmin int8, restart_lsn text, confirmed_flush_lsn text, wal_status text, safe_wal_size int8, two_phase bool, conflicting bool)",
		"CREATE TABLE pg_stat_gssapi(pid int4, gss_authenticated bool, principal text, encrypted bool, credentials_delegated bool)",
		"CREATE TABLE pg_auth_members(oid text, roleid oid, member oid, grantor oid, admin_option bool, inherit_option bool, set_option bool)",
		"CREATE TABLE pg_stat_activity(datid oid, datname text, pid int4, usesysid oid, usename text, application_name text, client_addr inet, client_hostname text, client_port int4, backend_start timestamp, xact_start timestamp, query_start timestamp, state_change timestamp, wait_event_type text, wait_event text, state text, backend_xid int8, backend_xmin int8, query text, backend_type text)",
		"CREATE TABLE pg_views(schemaname text, viewname text, viewowner text, definition text)",
		"CREATE TABLE pg_matviews(schemaname text, matviewname text, matviewowner text, tablespace text, hasindexes bool, ispopulated bool, definition text)",
		"CREATE TABLE pg_opclass(oid oid, opcmethod oid, opcname text, opcnamespace oid, opcowner oid, opcfamily oid, opcintype oid, opcdefault bool, opckeytype oid)",
		"CREATE TABLE pg_policy(oid oid, polname text, polrelid oid, polcmd text, polpermissive bool, polroles oid, polqual text, polwithcheck text)",
		"CREATE TABLE pg_statistic_ext(oid oid, stxrelid oid, stxname text, stxnamespace oid, stxowner oid, stxstattarget int4, stxkeys oid, stxkind text, stxexprs text)",
		"CREATE TABLE pg_publication(oid oid, pubname text, pubowner oid, puballtables bool, pubinsert bool, pubupdate bool, pubdelete bool, pubtruncate bool, pubviaroot bool)",
		"CREATE TABLE pg_publication_rel(oid oid, prpubid oid, prrelid oid, prqual text, prattrs text)",
		"CREATE TABLE pg_publication_namespace(oid oid, pnpubid oid, pnnspid oid)",
		"CREATE TABLE pg_rewrite(oid oid, rulename text, ev_class oid, ev_type char, ev_enabled char, is_instead bool, ev_qual text, ev_action text)",

		// Dynamic tables
		// DuckDB doesn't handle dynamic view replacement properly
		"CREATE TABLE pg_stat_user_tables(relid oid, schemaname text, relname text, seq_scan int8, last_seq_scan timestamp, seq_tup_read int8, idx_scan int8, last_idx_scan timestamp, idx_tup_fetch int8, n_tup_ins int8, n_tup_upd int8, n_tup_del int8, n_tup_hot_upd int8, n_tup_newpage_upd int8, n_live_tup int8, n_dead_tup int8, n_mod_since_analyze int8, n_ins_since_vacuum int8, last_vacuum timestamp, last_autovacuum timestamp, last_analyze timestamp, last_autoanalyze timestamp, vacuum_count int8, autovacuum_count int8, analyze_count int8, autoanalyze_count int8)",

		// Static views
		passwordCatalogQuery(config),
		"CREATE VIEW pg_roles AS SELECT '10'::oid AS oid, '" + config.User + "' AS rolname, TRUE AS rolsuper, TRUE AS rolinherit, TRUE AS rolcreaterole, TRUE AS rolcreatedb, TRUE AS rolcanlogin, FALSE AS rolreplication, -1 AS rolconnlimit, NULL::text AS rolpassword, NULL::timestamp AS rolvaliduntil, FALSE AS rolbypassrls, NULL::text[] AS rolconfig",
		"CREATE VIEW pg_extension AS SELECT '13823'::oid AS oid, 'plpgsql' AS extname, '10'::oid AS extowner, '11'::oid AS extnamespace, FALSE AS extrelocatable, '1.0'::text AS extversion, NULL::text[] AS extconfig, NULL::text[] AS extcondition",
		"CREATE VIEW pg_database AS SELECT '16388'::oid AS oid, '" + config.Database + "' AS datname, '10'::oid AS datdba, '6'::int4 AS encoding, 'c' AS datlocprovider, FALSE AS datistemplate, TRUE AS datallowconn, '-1'::int4 AS datconnlimit, '722'::int8 AS datfrozenxid, '1'::int4 AS datminmxid, '1663'::oid AS dattablespace, 'en_US.UTF-8' AS datcollate, 'en_US.UTF-8' AS datctype, 'en_US.UTF-8' AS datlocale, NULL::text AS daticurules, NULL::text AS datcollversion, NULL::text[] AS datacl",
		"CREATE VIEW pg_user AS SELECT '" + config.User + "' AS usename, '10'::oid AS usesysid, TRUE AS usecreatedb, TRUE AS usesuper, TRUE AS userepl, TRUE AS usebypassrls, '' AS passwd, NULL::timestamp AS valuntil, NULL::text[] AS useconfig",
		"CREATE VIEW pg_collation AS SELECT '100'::oid AS oid, 'default' AS collname, '11'::oid AS collnamespace, '10'::oid AS collowner, 'd' AS collprovider, TRUE AS collisdeterministic, '-1'::int4 AS collencoding, NULL::text AS collcollate, NULL::text AS collctype, NULL::text AS colliculocale, NULL::text AS collicurules, NULL::text AS collversion",
		"CREATE VIEW user AS SELECT '" + config.User + "' AS user",

		// Dynamic views
		// DuckDB does not support indnullsnotdistinct column
		"CREATE VIEW pg_index AS SELECT *, FALSE AS indnullsnotdistinct FROM pg_catalog.pg_index",
		// Hide DuckDB's system and duplicate schemas
		"CREATE VIEW pg_namespace AS SELECT * FROM pg_catalog.pg_namespace WHERE oid >= (SELECT oid FROM pg_catalog.pg_namespace WHERE nspname = '" + PG_SCHEMA_PUBLIC + "')",
		// DuckDB does not support relhassubclass and relforcerowsecurity columns
		`CREATE VIEW pg_class AS SELECT
			oid,
			relname,
			relnamespace,
			reltype,
			reloftype,
			relowner,
			relam,
			relfilenode,
			reltablespace,
			relpages,
			reltuples,
			relallvisible,
			reltoastrelid,
			relhasindex,
			relisshared,
			relpersistence,
			CASE relkind
				WHEN 'v' THEN
					CASE relnamespace >= (SELECT oid FROM pg_catalog.pg_namespace WHERE nspname = '` + PG_SCHEMA_PUBLIC + `')
					WHEN TRUE THEN
						'm'
					ELSE
						'v'
					END
			ELSE
				relkind
			END AS relkind,
			FALSE AS relhassubclass,
			FALSE AS relforcerowsecurity
		FROM pg_catalog.pg_class`,
		// DuckDB's pg_attribute.atttypid values use DuckDB-internal type IDs. Translate
		// user-visible columns to PostgreSQL OIDs so clients can join them to pg_type.
		`CREATE VIEW ` + PG_TABLE_PG_ATTRIBUTE + ` AS SELECT
			a.* REPLACE (
				CASE columns.data_type
					WHEN 'BOOLEAN' THEN 16
					WHEN 'BOOLEAN[]' THEN 1000
					WHEN 'BLOB' THEN 17
					WHEN 'BLOB[]' THEN 1001
					WHEN 'SMALLINT' THEN 21
					WHEN 'SMALLINT[]' THEN 1005
					WHEN 'INTEGER' THEN 23
					WHEN 'INTEGER[]' THEN 1007
					WHEN 'BIGINT' THEN 20
					WHEN 'BIGINT[]' THEN 1016
					WHEN 'UINTEGER' THEN 28
					WHEN 'UINTEGER[]' THEN 1011
					WHEN 'UBIGINT' THEN 5069
					WHEN 'UBIGINT[]' THEN 271
					WHEN 'HUGEINT' THEN 1700
					WHEN 'HUGEINT[]' THEN 1231
					WHEN 'FLOAT' THEN 700
					WHEN 'FLOAT[]' THEN 1021
					WHEN 'DOUBLE' THEN 701
					WHEN 'DOUBLE[]' THEN 1022
					WHEN 'VARCHAR' THEN 25
					WHEN 'VARCHAR[]' THEN 1009
					WHEN 'TIME' THEN 1083
					WHEN 'TIME[]' THEN 1183
					WHEN 'DATE' THEN 1082
					WHEN 'DATE[]' THEN 1182
					WHEN 'TIMESTAMP' THEN 1114
					WHEN 'TIMESTAMP[]' THEN 1115
					WHEN 'TIMESTAMP WITH TIME ZONE' THEN 1184
					WHEN 'TIMESTAMP WITH TIME ZONE[]' THEN 1185
					WHEN 'UUID' THEN 2950
					WHEN 'UUID[]' THEN 2951
					WHEN 'INTERVAL' THEN 1186
					WHEN 'INTERVAL[]' THEN 1187
					WHEN 'JSON' THEN 114
					WHEN 'JSON[]' THEN 199
					ELSE CASE
						WHEN starts_with(columns.data_type, 'DECIMAL') AND ends_with(columns.data_type, '[]') THEN 1231
						WHEN starts_with(columns.data_type, 'DECIMAL') THEN 1700
						ELSE a.atttypid
					END
				END AS atttypid
			)
		FROM pg_catalog.pg_attribute a
		LEFT JOIN pg_catalog.pg_class class ON class.oid = a.attrelid
		LEFT JOIN pg_catalog.pg_namespace namespace ON namespace.oid = class.relnamespace
		LEFT JOIN information_schema.columns columns
			ON columns.table_schema = namespace.nspname
			AND columns.table_name = class.relname
			AND columns.column_name = a.attname`,
		`CREATE VIEW pg_type AS
			SELECT * FROM pg_catalog.pg_type
			UNION ALL
			SELECT 18, 'char', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, 1, true, 'b', 'Z', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 19, 'name', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, 64, false, 'b', 'S', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 22, 'int2vector', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 24, 'regproc', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, 4, true, 'b', 'N', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 25, 'text', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'S', true, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 26, 'oid', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, 4, true, 'b', 'N', true, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 27, 'tid', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, 6, false, 'b', 'U', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 28, 'xid', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, 4, true, 'b', 'U', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 29, 'cid', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, 4, true, 'b', 'U', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 30, 'oidvector', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 32, 'pg_ddl_command', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, 8, true, 'p', 'P', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 71, 'pg_type', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'c', 'C', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 75, 'pg_attribute', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'c', 'C', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 81, 'pg_proc', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'c', 'C', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 83, 'pg_class', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'c', 'C', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 114, 'json', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'U', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 142, 'xml', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'U', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 143, '_xml', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 194, 'pg_node_tree', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'Z', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 199, '_json', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 210, '_pg_type', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 269, 'table_am_handler', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, 4, true, 'p', 'P', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 270, '_pg_attribute', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 271, '_xid8', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 272, '_pg_proc', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 273, '_pg_class', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 325, 'index_am_handler', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, 4, true, 'p', 'P', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 600, 'point', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, 16, false, 'b', 'G', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 601, 'lseg', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, 32, false, 'b', 'G', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 602, 'path', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'G', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 603, 'box', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, 32, false, 'b', 'G', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 604, 'polygon', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'G', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 628, 'line', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, 24, false, 'b', 'G', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 629, '_line', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 650, 'cidr', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'I', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 651, '_cidr', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 705, 'unknown', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -2, false, 'p', 'X', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 718, 'circle', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, 24, false, 'b', 'G', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 719, '_circle', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 774, 'macaddr8', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, 8, false, 'b', 'U', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 775, '_macaddr8', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 790, 'money', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, 8, true, 'b', 'N', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 791, '_money', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 829, 'macaddr', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, 6, false, 'b', 'U', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 869, 'inet', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'I', true, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 1000, '_bool', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 1001, '_bytea', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 1002, '_char', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 1003, '_name', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 1005, '_int2', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 1006, '_int2vector', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 1007, '_int4', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 1008, '_regproc', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 1009, '_text', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 1010, '_tid', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 1011, '_xid', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 1012, '_cid', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 1013, '_oidvector', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 1014, '_bpchar', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 1015, '_varchar', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 1016, '_int8', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 1017, '_point', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 1018, '_lseg', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 1019, '_path', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 1020, '_box', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 1021, '_float4', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 1022, '_float8', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 1027, '_polygon', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 1028, '_oid', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 1033, 'aclitem', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, 16, false, 'b', 'U', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 1034, '_aclitem', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 1040, '_macaddr', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 1041, '_inet', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 1042, 'bpchar', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'S', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 1115, '_timestamp', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 1182, '_date', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 1183, '_time', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 1185, '_timestamptz', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 1187, '_interval', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 1231, '_numeric', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 1248, 'pg_database', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'c', 'C', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 1263, '_cstring', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 1270, '_timetz', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 1561, '_bit', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 1562, 'varbit', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'V', true, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 1563, '_varbit', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 1790, 'refcursor', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'U', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 2201, '_refcursor', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 2202, 'regprocedure', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, 4, true, 'b', 'N', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 2203, 'regoper', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, 4, true, 'b', 'N', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 2204, 'regoperator', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, 4, true, 'b', 'N', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 2205, 'regclass', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, 4, true, 'b', 'N', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 2206, 'regtype', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, 4, true, 'b', 'N', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 2207, '_regprocedure', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 2208, '_regoper', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 2209, '_regoperator', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 2210, '_regclass', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 2211, '_regtype', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 2249, 'record', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'p', 'P', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 2275, 'cstring', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -2, false, 'p', 'P', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 2276, 'any', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, 4, true, 'p', 'P', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 2277, 'anyarray', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'p', 'P', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 2278, 'void', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, 4, true, 'p', 'P', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 2279, 'trigger', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, 4, true, 'p', 'P', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 2280, 'language_handler', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, 4, true, 'p', 'P', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 2281, 'internal', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, 8, true, 'p', 'P', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 2283, 'anyelement', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, 4, true, 'p', 'P', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 2287, '_record', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'p', 'P', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 2776, 'anynonarray', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, 4, true, 'p', 'P', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 2842, 'pg_authid', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'c', 'C', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 2843, 'pg_auth_members', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'c', 'C', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 2949, '_txid_snapshot', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 2951, '_uuid', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 2970, 'txid_snapshot', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'U', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 3115, 'fdw_handler', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, 4, true, 'p', 'P', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 3220, 'pg_lsn', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, 8, true, 'b', 'U', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 3221, '_pg_lsn', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 3310, 'tsm_handler', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, 4, true, 'p', 'P', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 3361, 'pg_ndistinct', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'Z', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 3402, 'pg_dependencies', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'Z', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 3500, 'anyenum', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, 4, true, 'p', 'P', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 3614, 'tsvector', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'U', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 3615, 'tsquery', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'U', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 3642, 'gtsvector', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'U', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 3643, '_tsvector', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 3644, '_gtsvector', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 3645, '_tsquery', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 3734, 'regconfig', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, 4, true, 'b', 'N', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 3735, '_regconfig', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 3769, 'regdictionary', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, 4, true, 'b', 'N', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 3770, '_regdictionary', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 3802, 'jsonb', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'U', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 3807, '_jsonb', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 3831, 'anyrange', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'p', 'P', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 3838, 'event_trigger', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, 4, true, 'p', 'P', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 3904, 'int4range', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'r', 'R', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 3905, '_int4range', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 3906, 'numrange', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'r', 'R', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 3907, '_numrange', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 3908, 'tsrange', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'r', 'R', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 3909, '_tsrange', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 3910, 'tstzrange', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'r', 'R', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 3911, '_tstzrange', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 3912, 'daterange', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'r', 'R', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 3913, '_daterange', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 3926, 'int8range', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'r', 'R', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 3927, '_int8range', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 4066, 'pg_shseclabel', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'c', 'C', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 4072, 'jsonpath', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'U', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 4073, '_jsonpath', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 4089, 'regnamespace', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, 4, true, 'b', 'N', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 4090, '_regnamespace', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 4096, 'regrole', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, 4, true, 'b', 'N', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 4097, '_regrole', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 4191, 'regcollation', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, 4, true, 'b', 'N', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 4192, '_regcollation', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 4451, 'int4multirange', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'm', 'R', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 4532, 'nummultirange', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'm', 'R', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 4533, 'tsmultirange', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'm', 'R', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 4534, 'tstzmultirange', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'm', 'R', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 4535, 'datemultirange', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'm', 'R', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 4536, 'int8multirange', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'm', 'R', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 4537, 'anymultirange', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'p', 'P', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 4538, 'anycompatiblemultirange', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'p', 'P', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 4600, 'pg_brin_bloom_summary', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'Z', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 4601, 'pg_brin_minmax_multi_summary', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'Z', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 5017, 'pg_mcv_list', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'Z', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 5038, 'pg_snapshot', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'U', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 5039, '_pg_snapshot', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 5069, 'xid8', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, 8, true, 'b', 'U', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 5077, 'anycompatible', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, 4, true, 'p', 'P', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 5078, 'anycompatiblearray', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'p', 'P', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 5079, 'anycompatiblenonarray', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, 4, true, 'p', 'P', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 5080, 'anycompatiblerange', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'p', 'P', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 6101, 'pg_subscription', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'c', 'C', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 6150, '_int4multirange', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 6151, '_nummultirange', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 6152, '_tsmultirange', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 6153, '_tstzmultirange', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 6155, '_datemultirange', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			UNION ALL
			SELECT 6157, '_int8multirange', (SELECT typnamespace FROM pg_catalog.pg_type WHERE typname = 'bool'), 0, -1, false, 'b', 'A', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
		`,
	}
	PG_CATALOG_TABLE_NAMES = extractTableNames(result)
	return result
}

func CreateInformationSchemaTableQueries(config *Config) []string {
	result := []string{
		// Dynamic views
		// DuckDB does not support udt_catalog, udt_schema, udt_name
		`CREATE VIEW ` + PG_TABLE_COLUMNS + ` AS
		SELECT
			table_catalog, table_schema, table_name, column_name, ordinal_position, column_default, is_nullable, data_type, character_maximum_length, character_octet_length, numeric_precision, numeric_precision_radix, numeric_scale, datetime_precision, interval_type, interval_precision, character_set_catalog, character_set_schema, character_set_name, collation_catalog, collation_schema, collation_name, domain_catalog, domain_schema, domain_name,
			'` + config.Database + `' AS udt_catalog,
			'pg_catalog' AS udt_schema,
			CASE data_type
				WHEN 'BIGINT' THEN 'int8'
				WHEN 'BIGINT[]' THEN '_int8'
				WHEN 'BLOB' THEN 'bytea'
				WHEN 'BLOB[]' THEN '_bytea'
				WHEN 'BOOLEAN' THEN 'bool'
				WHEN 'BOOLEAN[]' THEN '_bool'
				WHEN 'DATE' THEN 'date'
				WHEN 'DATE[]' THEN '_date'
				WHEN 'FLOAT' THEN 'float4'
				WHEN 'FLOAT[]' THEN '_float4'
				WHEN 'DOUBLE' THEN 'float8'
				WHEN 'DOUBLE[]' THEN '_float8'
				WHEN 'DECIMAL' THEN 'numeric'
				WHEN 'DECIMAL[]' THEN '_numeric'
				WHEN 'INTEGER' THEN 'int4'
				WHEN 'INTEGER[]' THEN '_int4'
				WHEN 'VARCHAR' THEN 'text'
				WHEN 'VARCHAR[]' THEN '_text'
				WHEN 'TIME' THEN 'time'
				WHEN 'TIME[]' THEN '_time'
				WHEN 'TIMESTAMP' THEN 'timestamp'
				WHEN 'TIMESTAMP[]' THEN '_timestamp'
				WHEN 'TIMESTAMP WITH TIME ZONE' THEN 'timestamptz'
				WHEN 'TIMESTAMP WITH TIME ZONE[]' THEN '_timestamptz'
				WHEN 'UUID' THEN 'uuid'
				WHEN 'UUID[]' THEN '_uuid'
				WHEN 'JSON' THEN 'json'
				WHEN 'JSON[]' THEN '_json'
			ELSE
				CASE
					WHEN starts_with(data_type, 'DECIMAL') THEN 'numeric'
		        ELSE 'unknown'
				END
			END AS udt_name,
			scope_catalog, scope_schema, scope_name, maximum_cardinality, dtd_identifier, is_self_referencing, is_identity, identity_generation, identity_start, identity_increment, identity_maximum, identity_minimum, identity_cycle, is_generated, generation_expression, is_updatable
		FROM information_schema.columns`,
		`CREATE VIEW ` + PG_TABLE_TABLES + ` AS SELECT
			table_catalog,
			table_schema,
			table_name,
			table_type,
			self_referencing_column_name,
			reference_generation,
			user_defined_type_catalog,
			user_defined_type_schema,
			user_defined_type_name,
			is_insertable_into,
			is_typed,
			commit_action
		FROM information_schema.tables
		WHERE table_type != 'VIEW' AND table_schema != 'main'`,
	}
	return result
}
