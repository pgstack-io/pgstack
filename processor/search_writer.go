package main

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	SEARCH_INPUT_FORMAT_VERSION = 2
	SEARCH_CHUNK_OVERLAP_BYTES  = 400
	VECTOR_INDEX_ROW_THRESHOLD  = 100_000
	SEARCH_VERSION_RETENTION    = time.Hour
	SEARCH_MAINTENANCE_INTERVAL = time.Hour

	PGSTACK_TEXT_COLUMN        = "__pgstack_text"
	PGSTACK_TEXT_INDEX         = "__pgstack_text_idx"
	PGSTACK_CHUNK_INDEX_COLUMN = "__pgstack_chunk_index"
	PGSTACK_SOURCE_KEY_COLUMN  = "__pgstack_source_key"
	PGSTACK_UPDATED_AT_COLUMN  = "__pgstack_updated_at"
	PGSTACK_POSITION_COLUMN    = "__pgstack_position"
)

type searchEmbeddingConfig struct {
	ID      string
	Name    string
	Columns []string
}

type SearchTableConfig struct {
	Name         string   `json:"name"`
	IndexColumns []string `json:"indexColumns"`
	StoreColumns []string `json:"storeColumns"`
	KeywordOnly  bool     `json:"keywordOnly,omitempty"`
}

// Keyword-only tables need no vector columns or embedding requests.
func (table SearchTableConfig) embeddings() []searchEmbeddingConfig {
	if table.KeywordOnly {
		return nil
	}
	return []searchEmbeddingConfig{{ID: "embedding_hash", Name: "__pgstack_embedding", Columns: table.IndexColumns}}
}

type searchRow struct {
	table             SearchTableConfig
	message           *ChangeMessage
	values            map[string]interface{}
	chunkIndex        int
	text              interface{}
	existing          bool
	hashes            map[string]string
	changedEmbeddings []searchEmbeddingConfig
	vectors           map[string][]float32
}

type searchColumn struct {
	name     string
	dataType string
}

type embeddingTask struct {
	// row and embedding identify where the vector returned for this flattened
	// API input must be stored after the batched embedding request completes.
	row       *searchRow
	embedding searchEmbeddingConfig
	input     string
}

type searchCleanup struct {
	table      SearchTableConfig
	message    *ChangeMessage
	values     map[string]interface{}
	chunkCount int
}

type searchApplyPlan struct {
	rows       []*searchRow
	tasks      []embeddingTask
	cleanups   []searchCleanup
	sourceRows int
}

type existingSearchRows struct {
	hashes   map[int]map[string]string
	position uint64
}

type SearchWriter struct {
	config                      *Config
	duckdb                      *DuckDBClient
	tables                      map[string]SearchTableConfig
	embeddings                  *EmbeddingClient
	cleaned                     bool
	lastMaintainedAtByTableName map[string]time.Time
}

func NewSearchWriter(config *Config) (*SearchWriter, error) {
	var tables []SearchTableConfig
	if err := json.Unmarshal([]byte(config.SearchTablesJSON), &tables); err != nil {
		return nil, fmt.Errorf("failed to parse SEARCH_TABLES_JSON: %w", err)
	}
	byName := make(map[string]SearchTableConfig, len(tables))
	physicalNames := make(map[string]string, len(tables))
	for _, table := range tables {
		if _, exists := byName[table.Name]; exists {
			return nil, fmt.Errorf("duplicate Search table: %s", table.Name)
		}
		physicalName := searchPhysicalTableName(table.Name)
		if previous, exists := physicalNames[physicalName]; exists {
			return nil, fmt.Errorf("search table name collision: %s and %s", previous, table.Name)
		}
		if err := validateSearchTableConfig(table); err != nil {
			return nil, err
		}
		byName[table.Name] = table
		physicalNames[physicalName] = table.Name
	}
	writer := &SearchWriter{config: config, tables: byName, lastMaintainedAtByTableName: make(map[string]time.Time)}
	needsEmbeddings := false
	for _, table := range tables {
		needsEmbeddings = needsEmbeddings || !table.KeywordOnly
	}
	if needsEmbeddings {
		client, err := NewEmbeddingClient(config)
		if err != nil {
			return nil, err
		}
		writer.embeddings = client
	}
	return writer, nil
}

func validateSearchTableConfig(table SearchTableConfig) error {
	columnNames := make(map[string]bool, len(table.StoreColumns)+len(table.embeddings()))
	for _, column := range table.StoreColumns {
		if column == "" || columnNames[column] || strings.HasPrefix(column, "__pgstack_") {
			return fmt.Errorf("invalid or duplicate Search column: %s.%s", table.Name, column)
		}
		columnNames[column] = true
	}
	if len(table.IndexColumns) == 0 {
		return fmt.Errorf("search table %s must have at least one index column", table.Name)
	}
	indexColumns := make(map[string]bool)
	for _, column := range table.IndexColumns {
		if column == "" || indexColumns[column] || strings.HasPrefix(column, "__pgstack_") {
			return fmt.Errorf("invalid or duplicate Search index column: %s.%s", table.Name, column)
		}
		indexColumns[column] = true
	}

	return nil
}

func (writer *SearchWriter) Enabled() bool {
	return len(writer.tables) > 0
}

func (writer *SearchWriter) SetDuckDB(duckdb *DuckDBClient) {
	writer.duckdb = duckdb
}

func (writer *SearchWriter) ReconcileTables(ctx context.Context) error {
	if !writer.Enabled() {
		return nil
	}
	if writer.duckdb == nil {
		return errors.New("duckdb client is not set")
	}
	if err := writer.dropRemovedTables(ctx); err != nil {
		return err
	}
	for _, table := range writer.tables {
		if err := writer.ensureTable(ctx, table, table.StoreColumns); err != nil {
			return err
		}
		if err := writer.maintainTableIndexes(ctx, table, false); err != nil {
			return err
		}
	}
	writer.cleaned = true
	return nil
}

func (writer *SearchWriter) Apply(ctx context.Context, records []*FetchedRecord) error {
	if len(records) == 0 || !writer.Enabled() {
		return nil
	}
	if writer.duckdb == nil {
		return errors.New("duckdb client is not set")
	}
	if !writer.cleaned {
		if err := writer.dropRemovedTables(ctx); err != nil {
			return err
		}
		writer.cleaned = true
	}

	plan, err := writer.buildApplyPlan(ctx, writer.latestRecords(records))
	if err != nil {
		return err
	}
	if err := writer.populateEmbeddings(ctx, plan.tasks); err != nil {
		return err
	}
	if err := writer.persistApplyPlan(ctx, plan); err != nil {
		return err
	}
	if err := writer.maintainTables(ctx, plan); err != nil {
		return err
	}

	LogInfo(writer.config, fmt.Sprintf("Applied %d Search source rows as %d chunks with %d embedding inputs", plan.sourceRows, len(plan.rows), len(plan.tasks)))
	return nil
}

func (writer *SearchWriter) buildApplyPlan(ctx context.Context, records []*FetchedRecord) (*searchApplyPlan, error) {
	plan := &searchApplyPlan{
		rows:     make([]*searchRow, 0, len(records)),
		cleanups: make([]searchCleanup, 0, len(records)),
	}
	for _, record := range records {
		table, ok := writer.tables[record.Message.Source.Schema+"."+record.Message.Source.Table]
		if !ok {
			continue
		}
		if err := writer.ensureTable(ctx, table, record.Message.Source.Pk); err != nil {
			return nil, err
		}
		if record.Message.Op != OPERATION_TRUNCATE && len(record.Message.Source.Pk) == 0 {
			return nil, fmt.Errorf("search table %s has no primary key metadata", table.Name)
		}
		if record.Message.Op == OPERATION_DELETE || record.Message.Op == OPERATION_TRUNCATE {
			plan.rows = append(plan.rows, &searchRow{table: table, message: record.Message, values: record.Message.Before})
			plan.sourceRows++
			continue
		}

		existingRows, err := writer.existingHashes(ctx, table, record.Message, record.Message.After)
		if err != nil {
			return nil, err
		}
		if existingRows.position > record.Message.Source.Lsn {
			continue
		}

		chunksByEmbedding := make(map[string][]string, len(table.embeddings()))
		chunkCount := 1
		for _, embedding := range table.embeddings() {
			chunks := chunkEmbeddingInput(searchEmbeddingInput(embedding, record.Message.After))
			chunksByEmbedding[embedding.ID] = chunks
			chunkCount = max(chunkCount, len(chunks))
		}

		for chunkIndex := 0; chunkIndex < chunkCount; chunkIndex++ {
			row := &searchRow{
				table:      table,
				message:    record.Message,
				values:     record.Message.After,
				chunkIndex: chunkIndex,
				hashes:     make(map[string]string),
				vectors:    make(map[string][]float32),
			}
			existingHashes, existing := existingRows.hashes[chunkIndex]
			row.existing = existing
			if chunkIndex == 0 {
				// Keep full source text once, without embedding field labels or overlap.
				// This preserves keyword frequencies and phrases across vector chunks.
				row.text = searchKeywordInput(table, record.Message.After)
			}
			for _, embedding := range table.embeddings() {
				chunks := chunksByEmbedding[embedding.ID]
				if chunkIndex >= len(chunks) {
					if existingHashes[embedding.ID] != "" {
						row.changedEmbeddings = append(row.changedEmbeddings, embedding)
					}
					continue
				}
				input := chunks[chunkIndex]
				hash := writer.embeddingHash(embedding, input)
				row.hashes[embedding.ID] = hash
				if !existing || existingHashes[embedding.ID] != hash {
					row.changedEmbeddings = append(row.changedEmbeddings, embedding)
					plan.tasks = append(plan.tasks, embeddingTask{row: row, embedding: embedding, input: input})
				}
			}
			plan.rows = append(plan.rows, row)
		}
		plan.cleanups = append(plan.cleanups, searchCleanup{table: table, message: record.Message, values: record.Message.After, chunkCount: chunkCount})
		plan.sourceRows++
	}
	return plan, nil
}

func (writer *SearchWriter) populateEmbeddings(ctx context.Context, tasks []embeddingTask) error {
	inputs := make([]string, len(tasks))
	for i := range tasks {
		inputs[i] = tasks[i].input
	}
	if len(inputs) > 0 {
		vectors, err := writer.embeddings.Embed(ctx, inputs)
		if err != nil {
			return err
		}
		for i, vector := range vectors {
			tasks[i].row.vectors[tasks[i].embedding.ID] = vector
		}
	}
	return nil
}

func (writer *SearchWriter) persistApplyPlan(ctx context.Context, plan *searchApplyPlan) error {
	for _, row := range plan.rows {
		if err := writer.applyRow(ctx, row); err != nil {
			return err
		}
	}
	for _, cleanup := range plan.cleanups {
		if err := writer.deleteStaleChunks(ctx, cleanup.table, cleanup.message, cleanup.values, cleanup.chunkCount); err != nil {
			return err
		}
	}
	return nil
}

// maintainTables runs once after a CDC batch has been persisted. It compacts
// table fragments and creates missing indices after the row threshold. Once an
// hour, it also indexes accumulated rows in merge mode and vacuums old versions.
func (writer *SearchWriter) maintainTables(ctx context.Context, plan *searchApplyPlan) error {
	tables := make(map[string]SearchTableConfig)
	for _, row := range plan.rows {
		tables[row.table.Name] = row.table
	}
	for _, cleanup := range plan.cleanups {
		tables[cleanup.table.Name] = cleanup.table
	}
	for _, table := range tables {
		maintenanceDue := searchMaintenanceDue(writer.lastMaintainedAtByTableName[table.Name], time.Now())
		if err := writer.compactTable(ctx, table); err != nil {
			return err
		}
		if err := writer.maintainTableIndexes(ctx, table, maintenanceDue); err != nil {
			return err
		}
		if maintenanceDue {
			if err := writer.vacuumTable(ctx, table); err != nil {
				// Vacuum only reclaims old versions. A cleanup failure must not
				// reject an otherwise persisted CDC batch and cause redelivery.
				LogWarn(writer.config, err)
			}
			if writer.lastMaintainedAtByTableName == nil {
				writer.lastMaintainedAtByTableName = make(map[string]time.Time)
			}
			writer.lastMaintainedAtByTableName[table.Name] = time.Now()
		}
	}
	return nil
}

func (writer *SearchWriter) maintainTableIndexes(ctx context.Context, table SearchTableConfig, merge bool) error {
	datasetPath := writer.searchDatasetPath(table)
	indices, err := writer.vectorIndexes(ctx, datasetPath)
	if err != nil {
		return fmt.Errorf("failed to inspect Search indices for %s: %w", table.Name, err)
	}

	if indices[PGSTACK_TEXT_INDEX] {
		if merge {
			query := searchOptimizeIndexQuery(PGSTACK_TEXT_INDEX, datasetPath)
			if err := writer.executeLanceMaintenance(ctx, query); err != nil {
				return fmt.Errorf("failed to optimize Search text index on %s: %w", table.Name, err)
			}
		}
	} else {
		query := searchTextIndexQuery(datasetPath)
		if _, err := writer.duckdb.ExecContext(ctx, query); err != nil {
			return fmt.Errorf("failed to create Search text index on %s: %w", table.Name, err)
		}
	}

	missingIndex := false
	for _, embedding := range table.embeddings() {
		if !indices[vectorIndexName(embedding.ID)] {
			missingIndex = true
			break
		}
	}
	var rowCount int64
	if missingIndex {
		qualified := "search_store.main." + quoteIdentifier(searchPhysicalTableName(table.Name))
		if err := writer.duckdb.QueryRowContext(ctx, "SELECT count(*) FROM "+qualified).Scan(&rowCount); err != nil {
			return fmt.Errorf("failed to count Search rows for %s: %w", table.Name, err)
		}
	}

	for _, embedding := range table.embeddings() {
		indexName := vectorIndexName(embedding.ID)
		if indices[indexName] {
			if merge {
				query := searchOptimizeIndexQuery(indexName, datasetPath)
				if err := writer.executeLanceMaintenance(ctx, query); err != nil {
					return fmt.Errorf("failed to optimize Search index %s on %s: %w", indexName, table.Name, err)
				}
			}
			continue
		}
		if rowCount < VECTOR_INDEX_ROW_THRESHOLD {
			continue
		}

		partitions := max(1, int(math.Sqrt(float64(rowCount))))
		query := searchVectorIndexQuery(datasetPath, embedding, partitions)
		if _, err := writer.duckdb.ExecContext(ctx, query); err != nil {
			return fmt.Errorf("failed to create Search index %s on %s: %w", indexName, table.Name, err)
		}
		LogInfo(writer.config, fmt.Sprintf("Created L2 vector index %s on %s.%s with %d partitions for %d rows", indexName, table.Name, embedding.Name, partitions, rowCount))
	}
	return nil
}

func (writer *SearchWriter) vectorIndexes(ctx context.Context, datasetPath string) (map[string]bool, error) {
	rows, err := writer.duckdb.QueryContext(ctx, "SHOW INDEXES ON '"+escapeSQLString(datasetPath)+"'")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	indices := make(map[string]bool)
	for rows.Next() {
		var indexName, indexType, fields string
		var rowsIndexed uint64
		var details sql.NullString
		if err := rows.Scan(&indexName, &indexType, &fields, &rowsIndexed, &details); err != nil {
			return nil, err
		}
		indices[indexName] = true
	}
	return indices, rows.Err()
}

func (writer *SearchWriter) compactTable(ctx context.Context, table SearchTableConfig) error {
	query := "OPTIMIZE '" + escapeSQLString(writer.searchDatasetPath(table)) + "'"
	if err := writer.executeLanceMaintenance(ctx, query); err != nil {
		return fmt.Errorf("failed to compact Search table %s: %w", table.Name, err)
	}
	return nil
}

func (writer *SearchWriter) vacuumTable(ctx context.Context, table SearchTableConfig) error {
	query := fmt.Sprintf(
		"VACUUM LANCE '%s' WITH (older_than_seconds = %d, retain_n_versions = 1)",
		escapeSQLString(writer.searchDatasetPath(table)),
		SEARCH_VERSION_RETENTION/time.Second,
	)
	if err := writer.executeLanceMaintenance(ctx, query); err != nil {
		return fmt.Errorf("failed to clean up Search versions for %s: %w", table.Name, err)
	}
	return nil
}

func (writer *SearchWriter) executeLanceMaintenance(ctx context.Context, query string) error {
	var operation, target, metrics string
	return writer.duckdb.QueryRowContext(ctx, query).Scan(&operation, &target, &metrics)
}

func searchMaintenanceDue(lastMaintainedAt time.Time, now time.Time) bool {
	return lastMaintainedAt.IsZero() || now.Sub(lastMaintainedAt) >= SEARCH_MAINTENANCE_INTERVAL
}

func (writer *SearchWriter) searchDatasetPath(table SearchTableConfig) string {
	return fmt.Sprintf("s3://%s/%s/%s.lance", writer.config.S3Bucket, strings.Trim(writer.config.SearchBasePath, "/"), searchPhysicalTableName(table.Name))
}

// Lance index DDL uses its own parser: quoted index names are rejected, and
// quotes around column names become part of the field name. These identifiers
// are generated internally, so emit them bare while still escaping dataset URIs.
func searchTextIndexQuery(datasetPath string) string {
	return fmt.Sprintf("CREATE INDEX %s ON '%s' (%s) USING INVERTED", PGSTACK_TEXT_INDEX, escapeSQLString(datasetPath), PGSTACK_TEXT_COLUMN)
}

func searchVectorIndexQuery(datasetPath string, embedding searchEmbeddingConfig, partitions int) string {
	return fmt.Sprintf("CREATE INDEX %s ON '%s' (%s) USING IVF_FLAT WITH (num_partitions = %d, metric_type = 'l2')",
		vectorIndexName(embedding.ID), escapeSQLString(datasetPath), embedding.Name, partitions)
}

func searchOptimizeIndexQuery(indexName, datasetPath string) string {
	return "ALTER INDEX " + indexName + " ON '" + escapeSQLString(datasetPath) + "' OPTIMIZE WITH (mode = 'merge')"
}

func vectorIndexName(embeddingID string) string {
	return "__pgstack_vector_" + embeddingID + "_idx"
}

// latestRecords collapses a batch to its newest Search mutation per configured
// source table and primary key. The result contains source records, not chunks,
// and is sorted by source position so Apply handles them deterministically.
//
// Example input:  id=1 UPDATE@10, id=2 DELETE@12, id=1 UPDATE@14
// Example output: id=2 DELETE@12, id=1 UPDATE@14
func (writer *SearchWriter) latestRecords(records []*FetchedRecord) []*FetchedRecord {
	latest := make(map[string]*FetchedRecord)
	for _, record := range records {
		message := record.Message
		if !message.IsForProduct(PRODUCT_SEARCH) {
			continue
		}
		table, ok := writer.tables[message.Source.Schema+"."+message.Source.Table]
		if !ok {
			continue
		}
		keyValues := make([]string, 0, len(message.Source.Pk))
		values := message.After
		if message.Op == OPERATION_DELETE {
			values = message.Before
		}
		for _, column := range message.Source.Pk {
			keyValues = append(keyValues, fmt.Sprint(values[column]))
		}
		key := table.Name + "\x00" + strings.Join(keyValues, "\x00")
		if previous := latest[key]; previous == nil || previous.Message.Source.Lsn <= message.Source.Lsn {
			latest[key] = record
		}
	}
	result := make([]*FetchedRecord, 0, len(latest))
	for _, record := range latest {
		result = append(result, record)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Message.Source.Lsn < result[j].Message.Source.Lsn })
	return result
}

func (writer *SearchWriter) ensureTable(ctx context.Context, table SearchTableConfig, primaryKeys []string) error {
	name := searchPhysicalTableName(table.Name)
	qualified := "search_store.main." + quoteIdentifier(name)
	rows, err := writer.duckdb.QueryContext(ctx, "DESCRIBE "+qualified)
	if err == nil {
		var columns []searchColumn
		for rows.Next() {
			var columnName, columnType string
			var nullable, key, defaultValue, extra sql.NullString
			if err := rows.Scan(&columnName, &columnType, &nullable, &key, &defaultValue, &extra); err != nil {
				rows.Close()
				return err
			}
			columns = append(columns, searchColumn{name: columnName, dataType: columnType})
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return err
		}
		rows.Close()
		return writer.ensureColumns(ctx, qualified, table, primaryKeys, columns)
	}
	if len(primaryKeys) == 0 {
		return fmt.Errorf("search table %s must have a primary key", table.Name)
	}

	columns := writer.desiredColumns(table)
	definitions := make([]string, len(columns))
	for i, column := range columns {
		definitions[i] = quoteIdentifier(column.name) + " " + column.dataType
	}
	_, err = writer.duckdb.ExecContext(ctx, "CREATE TABLE "+qualified+" ("+strings.Join(definitions, ", ")+")")
	if err != nil {
		return fmt.Errorf("failed to create Search table %s: %w", table.Name, err)
	}
	return nil
}

func (writer *SearchWriter) ensureColumns(ctx context.Context, qualified string, table SearchTableConfig, primaryKeys []string, existing []searchColumn) error {
	desired := writer.desiredColumns(table)
	if searchColumnsEqual(existing, desired) {
		return nil
	}

	if _, err := writer.duckdb.ExecContext(ctx, "DROP TABLE "+qualified); err != nil {
		return err
	}
	return writer.ensureTable(ctx, table, primaryKeys)
}

func searchColumnsEqual(left []searchColumn, right []searchColumn) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i].name != right[i].name || normalizeSearchType(left[i].dataType) != normalizeSearchType(right[i].dataType) {
			return false
		}
	}
	return true
}

func (writer *SearchWriter) desiredColumns(table SearchTableConfig) []searchColumn {
	columns := make([]searchColumn, 0, len(table.StoreColumns)+len(table.embeddings())*2+4)
	for _, column := range table.StoreColumns {
		columns = append(columns, searchColumn{name: column, dataType: "VARCHAR"})
	}
	for _, embedding := range table.embeddings() {
		columns = append(columns, searchColumn{name: embedding.Name, dataType: fmt.Sprintf("FLOAT[%d]", OPENAI_EMBEDDING_DIMENSIONS)})
	}
	columns = append(columns,
		searchColumn{name: PGSTACK_UPDATED_AT_COLUMN, dataType: "TIMESTAMPTZ"},
		searchColumn{name: PGSTACK_POSITION_COLUMN, dataType: "UBIGINT"},
	)
	for _, embedding := range table.embeddings() {
		columns = append(columns, searchColumn{name: "__pgstack_" + embedding.ID, dataType: "VARCHAR"})
	}
	columns = append(columns,
		searchColumn{name: PGSTACK_CHUNK_INDEX_COLUMN, dataType: "UINTEGER"},
		searchColumn{name: PGSTACK_SOURCE_KEY_COLUMN, dataType: "VARCHAR"},
	)
	columns = append(columns, searchColumn{name: PGSTACK_TEXT_COLUMN, dataType: "VARCHAR"})
	return columns
}

func normalizeSearchType(dataType string) string {
	normalized := strings.ToUpper(strings.Join(strings.Fields(dataType), " "))
	if normalized == "TIMESTAMP WITH TIME ZONE" {
		return "TIMESTAMPTZ"
	}
	return normalized
}

func (writer *SearchWriter) dropRemovedTables(ctx context.Context) error {
	rows, err := writer.duckdb.QueryContext(ctx, "SHOW TABLES FROM search_store.main")
	if err != nil {
		return err
	}
	defer rows.Close()
	desired := make(map[string]bool, len(writer.tables))
	for name := range writer.tables {
		desired[searchPhysicalTableName(name)] = true
	}
	var removed []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return err
		}
		if !desired[name] {
			removed = append(removed, name)
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	for _, name := range removed {
		if _, err := writer.duckdb.ExecContext(ctx, "DROP TABLE search_store.main."+quoteIdentifier(name)); err != nil {
			return err
		}
	}
	return nil
}

// existingHashes loads the stored input hash for every embedding on every Lance
// chunk belonging to one source row. Apply uses the hashes to embed only changed
// inputs, and the greatest stored position to reject an older CDC mutation.
//
// Conceptually, it returns:
//
//	hashes: {
//	  chunk 0: {embedding_hash: hash1},
//	  chunk 1: {embedding_hash: hash2},
//	}
//	position: greatest stored source LSN
func (writer *SearchWriter) existingHashes(ctx context.Context, table SearchTableConfig, message *ChangeMessage, values map[string]interface{}) (existingSearchRows, error) {
	result := existingSearchRows{hashes: make(map[int]map[string]string)}
	if len(message.Source.Pk) == 0 {
		return result, fmt.Errorf("search table %s has no primary key metadata", table.Name)
	}
	columns := []string{quoteIdentifier(PGSTACK_CHUNK_INDEX_COLUMN), quoteIdentifier(PGSTACK_POSITION_COLUMN)}
	for _, embedding := range table.embeddings() {
		columns = append(columns, quoteIdentifier("__pgstack_"+embedding.ID))
	}
	where, args := primaryKeyPredicate(message.Source.Pk, values)
	query := "SELECT " + strings.Join(columns, ", ") + " FROM search_store.main." + quoteIdentifier(searchPhysicalTableName(table.Name)) + " WHERE " + where
	rows, err := writer.duckdb.QueryContext(ctx, query, args...)
	if err != nil {
		return result, err
	}
	defer rows.Close()
	for rows.Next() {
		var chunkIndex int
		var position uint64
		hashes := make([]sql.NullString, len(table.embeddings()))
		destinations := make([]interface{}, 0, len(hashes)+2)
		destinations = append(destinations, &chunkIndex, &position)
		for i := range hashes {
			destinations = append(destinations, &hashes[i])
		}
		if err := rows.Scan(destinations...); err != nil {
			return result, err
		}
		chunkHashes := make(map[string]string, len(hashes))
		for i, hash := range hashes {
			if hash.Valid {
				chunkHashes[table.embeddings()[i].ID] = hash.String
			}
		}
		result.hashes[chunkIndex] = chunkHashes
		result.position = max(result.position, position)
	}
	return result, rows.Err()
}

func (writer *SearchWriter) deleteStaleChunks(ctx context.Context, table SearchTableConfig, message *ChangeMessage, values map[string]interface{}, chunkCount int) error {
	_, args := primaryKeyPredicate(message.Source.Pk, values)
	args = append(args, chunkCount, message.Source.Lsn)
	qualified := "search_store.main." + quoteIdentifier(searchPhysicalTableName(table.Name))
	query := searchMergeDeleteQuery(qualified, message.Source.Pk, true)
	_, err := writer.duckdb.ExecContext(ctx, query, args...)
	return err
}

// applyRow persists one physical Lance chunk. It truncates or deletes for those
// CDC operations, updates an existing chunk (including only changed embeddings),
// or inserts a new chunk. Position guards prevent stale CDC data from winning.
func (writer *SearchWriter) applyRow(ctx context.Context, row *searchRow) error {
	qualified := "search_store.main." + quoteIdentifier(searchPhysicalTableName(row.table.Name))
	if row.message.Op == OPERATION_TRUNCATE {
		_, err := writer.duckdb.ExecContext(ctx, "TRUNCATE TABLE "+qualified)
		return err
	}
	if row.message.Op == OPERATION_DELETE {
		_, primaryArgs := primaryKeyPredicate(row.message.Source.Pk, row.values)
		args := append(primaryArgs, row.message.Source.Lsn)
		_, err := writer.duckdb.ExecContext(ctx, searchMergeDeleteQuery(qualified, row.message.Source.Pk, false), args...)
		return err
	}

	if row.existing {
		args := make([]interface{}, 0, len(row.table.StoreColumns)+len(row.changedEmbeddings)*2+4)
		for _, column := range row.table.StoreColumns {
			args = append(args, searchStoredValue(row.values[column]))
		}
		args = append(args, row.text)
		for _, embedding := range row.changedEmbeddings {
			args = append(args, searchEmbeddingValue(row, embedding.ID), searchEmbeddingHashValue(row, embedding.ID))
		}
		args = append(args, searchSourceKey(row.message.Source.Pk, row.values), time.UnixMicro(row.message.Source.TsUs), row.message.Source.Lsn)
		args = append(args, row.chunkIndex)
		query := searchMergeUpdateQuery(qualified, row.table, row.changedEmbeddings, row.message.Source.Pk, OPENAI_EMBEDDING_DIMENSIONS)
		_, err := writer.duckdb.ExecContext(ctx, query, args...)
		return err
	}

	columns := make([]string, 0, len(row.table.StoreColumns)+len(row.table.embeddings())*2+4)
	args := make([]interface{}, 0, cap(columns))
	for _, column := range row.table.StoreColumns {
		columns = append(columns, quoteIdentifier(column))
		args = append(args, searchStoredValue(row.values[column]))
	}
	columns = append(columns, quoteIdentifier(PGSTACK_TEXT_COLUMN))
	args = append(args, row.text)
	for _, embedding := range row.table.embeddings() {
		columns = append(columns, quoteIdentifier(embedding.Name))
		args = append(args, searchEmbeddingValue(row, embedding.ID))
	}
	columns = append(columns,
		quoteIdentifier(PGSTACK_CHUNK_INDEX_COLUMN),
		quoteIdentifier(PGSTACK_SOURCE_KEY_COLUMN),
		quoteIdentifier(PGSTACK_UPDATED_AT_COLUMN),
		quoteIdentifier(PGSTACK_POSITION_COLUMN),
	)
	args = append(args, row.chunkIndex, searchSourceKey(row.message.Source.Pk, row.values), time.UnixMicro(row.message.Source.TsUs), row.message.Source.Lsn)
	for _, embedding := range row.table.embeddings() {
		columns = append(columns, quoteIdentifier("__pgstack_"+embedding.ID))
		args = append(args, searchEmbeddingHashValue(row, embedding.ID))
	}
	placeholders := make([]string, len(columns))
	for i := range placeholders {
		placeholders[i] = "?"
	}
	_, err := writer.duckdb.ExecContext(ctx, "INSERT INTO "+qualified+" ("+strings.Join(columns, ", ")+") VALUES ("+strings.Join(placeholders, ", ")+")", args...)
	return err
}

func searchEmbeddingValue(row *searchRow, embeddingID string) interface{} {
	vector, ok := row.vectors[embeddingID]
	if !ok {
		return nil
	}
	return vector
}

func searchEmbeddingHashValue(row *searchRow, embeddingID string) interface{} {
	hash, ok := row.hashes[embeddingID]
	if !ok {
		return nil
	}
	return hash
}

func (writer *SearchWriter) embeddingHash(embedding searchEmbeddingConfig, input string) string {
	fingerprint := fmt.Sprintf("%s\x00%s\x00%d\x00%d\x00%s", OPENAI_EMBEDDING_MODEL, embedding.ID, OPENAI_EMBEDDING_DIMENSIONS, SEARCH_INPUT_FORMAT_VERSION, strings.Join(embedding.Columns, "\x00"))
	sum := sha256.Sum256([]byte(fingerprint + "\x00" + input))
	return hex.EncodeToString(sum[:])
}

func searchEmbeddingInput(embedding searchEmbeddingConfig, values map[string]interface{}) string {
	parts := make([]string, 0, len(embedding.Columns))
	for _, column := range embedding.Columns {
		encoded, _ := json.Marshal(values[column])
		parts = append(parts, column+":"+string(encoded))
	}
	return strings.Join(parts, "\n")
}

func chunkEmbeddingInput(input string) []string {
	if len(input) <= EMBEDDING_MAX_INPUT_BYTES {
		return []string{input}
	}

	chunks := make([]string, 0, len(input)/EMBEDDING_MAX_INPUT_BYTES+1)
	for start := 0; start < len(input); {
		end := min(start+EMBEDDING_MAX_INPUT_BYTES, len(input))
		for end > start && !utf8.ValidString(input[start:end]) {
			end--
		}
		if end == start {
			_, size := utf8.DecodeRuneInString(input[start:])
			end = start + size
		}
		chunks = append(chunks, input[start:end])
		if end == len(input) {
			break
		}

		start = max(start+1, end-SEARCH_CHUNK_OVERLAP_BYTES)
		for start < end && !utf8.RuneStart(input[start]) {
			start++
		}
	}
	return chunks
}

func searchSourceKey(primaryKeys []string, values map[string]interface{}) string {
	hash := sha256.New()
	for _, column := range primaryKeys {
		hash.Write([]byte(column))
		hash.Write([]byte{0})
		encoded, _ := json.Marshal(values[column])
		hash.Write(encoded)
		hash.Write([]byte{0})
	}
	return hex.EncodeToString(hash.Sum(nil))
}

func searchKeywordInput(table SearchTableConfig, values map[string]interface{}) string {
	parts := make([]string, 0, len(table.IndexColumns))
	for _, column := range table.IndexColumns {
		if value := searchStoredValue(values[column]); value != nil {
			parts = append(parts, fmt.Sprint(value))
		}
	}
	return strings.Join(parts, "\n")
}

func searchStoredValue(value interface{}) interface{} {
	if value == nil {
		return nil
	}
	switch typed := value.(type) {
	case string:
		return typed
	case json.Number:
		return typed.String()
	default:
		encoded, _ := json.Marshal(value)
		return string(encoded)
	}
}

func primaryKeyPredicate(columns []string, values map[string]interface{}) (string, []interface{}) {
	predicates := make([]string, len(columns))
	args := make([]interface{}, len(columns))
	for i, column := range columns {
		predicates[i] = quoteIdentifier(column) + " = ?"
		args[i] = searchStoredValue(values[column])
	}
	return strings.Join(predicates, " AND "), args
}

// searchMergeDeleteQuery uses MERGE for predicate deletes because the bundled
// Lance extension can apply DELETE FROM predicates as an unfiltered delete.
func searchMergeDeleteQuery(qualified string, primaryKeys []string, staleChunksOnly bool) string {
	sourceColumns := make([]string, 0, len(primaryKeys)+2)
	conditions := make([]string, 0, len(primaryKeys)+2)
	for _, column := range primaryKeys {
		quoted := quoteIdentifier(column)
		sourceColumns = append(sourceColumns, "?::VARCHAR AS "+quoted)
		conditions = append(conditions, "target."+quoted+" = source."+quoted)
	}
	if staleChunksOnly {
		quoted := quoteIdentifier(PGSTACK_CHUNK_INDEX_COLUMN)
		sourceColumns = append(sourceColumns, "?::UINTEGER AS "+quoted)
		conditions = append(conditions, "target."+quoted+" >= source."+quoted)
	}
	quotedPosition := quoteIdentifier(PGSTACK_POSITION_COLUMN)
	sourceColumns = append(sourceColumns, "?::UBIGINT AS "+quotedPosition)
	conditions = append(conditions, "target."+quotedPosition+" <= source."+quotedPosition)

	return "MERGE INTO " + qualified + " AS target USING (SELECT " + strings.Join(sourceColumns, ", ") +
		") AS source ON " + strings.Join(conditions, " AND ") + " WHEN MATCHED THEN DELETE"
}

// searchMergeUpdateQuery uses MERGE because Lance UPDATE cannot execute plans
// where DuckDB leaves one or more predicates as pushed table filters. MERGE
// applies the same primary-key, chunk, and source-position guards in its join.
func searchMergeUpdateQuery(qualified string, table SearchTableConfig, changedEmbeddings []searchEmbeddingConfig, primaryKeys []string, embeddingDimensions int) string {
	sourceColumns := make([]string, 0, len(table.StoreColumns)+len(changedEmbeddings)*2+4)
	sets := make([]string, 0, len(table.StoreColumns)+len(changedEmbeddings)*2+3)
	addSet := func(column string, dataType string) {
		quoted := quoteIdentifier(column)
		sourceColumns = append(sourceColumns, "?::"+dataType+" AS "+quoted)
		sets = append(sets, quoted+" = source."+quoted)
	}
	for _, column := range table.StoreColumns {
		addSet(column, "VARCHAR")
	}
	addSet(PGSTACK_TEXT_COLUMN, "VARCHAR")
	for _, embedding := range changedEmbeddings {
		addSet(embedding.Name, fmt.Sprintf("FLOAT[%d]", embeddingDimensions))
		addSet("__pgstack_"+embedding.ID, "VARCHAR")
	}
	addSet(PGSTACK_SOURCE_KEY_COLUMN, "VARCHAR")
	addSet(PGSTACK_UPDATED_AT_COLUMN, "TIMESTAMPTZ")
	addSet(PGSTACK_POSITION_COLUMN, "UBIGINT")

	conditions := make([]string, 0, len(primaryKeys)+2)
	for _, column := range primaryKeys {
		quoted := quoteIdentifier(column)
		conditions = append(conditions, "target."+quoted+" = source."+quoted)
	}
	quotedChunk := quoteIdentifier(PGSTACK_CHUNK_INDEX_COLUMN)
	quotedPosition := quoteIdentifier(PGSTACK_POSITION_COLUMN)
	sourceColumns = append(sourceColumns, "?::UINTEGER AS "+quotedChunk)
	conditions = append(conditions,
		"target."+quotedChunk+" = source."+quotedChunk,
		"target."+quotedPosition+" <= source."+quotedPosition,
	)

	return "MERGE INTO " + qualified + " AS target USING (SELECT " + strings.Join(sourceColumns, ", ") +
		") AS source ON " + strings.Join(conditions, " AND ") + " WHEN MATCHED THEN UPDATE SET " + strings.Join(sets, ", ")
}

func searchPhysicalTableName(schemaTable string) string {
	return strings.ReplaceAll(schemaTable, ".", "_")
}

func quoteIdentifier(value string) string {
	return `"` + strings.ReplaceAll(value, `"`, `""`) + `"`
}
