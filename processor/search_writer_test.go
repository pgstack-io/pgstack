package main

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

func TestSearchPhysicalTableName(t *testing.T) {
	if got := searchPhysicalTableName("public.documents"); got != "public_documents" {
		t.Fatalf("unexpected physical table name: %s", got)
	}
}

func TestSearchDatasetAndIndexNames(t *testing.T) {
	writer := &SearchWriter{config: &Config{S3Bucket: "bucket", SearchBasePath: "/org/project/search/"}}
	table := SearchTableConfig{Name: "public.documents"}
	if got := writer.searchDatasetPath(table); got != "s3://bucket/org/project/search/public_documents.lance" {
		t.Fatalf("unexpected Search dataset path: %s", got)
	}
	if got := vectorIndexName("default"); got != "__pgstack_vector_default_idx" {
		t.Fatalf("unexpected Search index name: %s", got)
	}
}

func TestSearchMaintenanceDue(t *testing.T) {
	now := time.Date(2026, time.August, 27, 12, 0, 0, 0, time.UTC)
	if !searchMaintenanceDue(time.Time{}, now) {
		t.Fatal("expected the first maintenance to be due")
	}
	if searchMaintenanceDue(now.Add(-SEARCH_MAINTENANCE_INTERVAL+time.Second), now) {
		t.Fatal("expected maintenance within the interval to be skipped")
	}
	if !searchMaintenanceDue(now.Add(-SEARCH_MAINTENANCE_INTERVAL), now) {
		t.Fatal("expected maintenance at the interval boundary to be due")
	}
}

func TestChunkEmbeddingInputPreservesLongUTF8Input(t *testing.T) {
	input := strings.Repeat("é", EMBEDDING_MAX_INPUT_BYTES/2+100)
	chunks := chunkEmbeddingInput(input)
	if len(chunks) < 2 {
		t.Fatalf("expected multiple chunks, got %d", len(chunks))
	}
	for _, chunk := range chunks {
		if len(chunk) > EMBEDDING_MAX_INPUT_BYTES {
			t.Fatalf("chunk exceeds byte limit: %d", len(chunk))
		}
		if !utf8.ValidString(chunk) {
			t.Fatalf("chunk is not valid UTF-8")
		}
	}
	withoutOverlap := chunks[0] + chunks[1][min(SEARCH_CHUNK_OVERLAP_BYTES, len(chunks[1])):]
	if withoutOverlap != input {
		t.Fatal("chunking lost input content")
	}
}

func TestEmbeddingBatchEndHonorsByteBudget(t *testing.T) {
	inputs := make([]string, 100)
	for i := range inputs {
		inputs[i] = strings.Repeat("c", EMBEDDING_MAX_INPUT_BYTES)
	}
	end, err := embeddingBatchEnd(inputs, 0)
	if err != nil {
		t.Fatal(err)
	}
	if end != EMBEDDING_BATCH_MAX_INPUT_BYTES/EMBEDDING_MAX_INPUT_BYTES {
		t.Fatalf("unexpected byte-budgeted batch end: %d", end)
	}
}

func TestSearchMergeDeleteQuery(t *testing.T) {
	qualified := `search_store.main."public_documents"`
	query := searchMergeDeleteQuery(qualified, []string{"tenant_id", "id"}, true)
	for _, expected := range []string{
		`MERGE INTO search_store.main."public_documents" AS target`,
		`?::VARCHAR AS "tenant_id"`,
		`target."tenant_id" = source."tenant_id"`,
		`target."id" = source."id"`,
		`target."__pgstack_chunk_index" >= source."__pgstack_chunk_index"`,
		`target."__pgstack_position" <= source."__pgstack_position"`,
		`WHEN MATCHED THEN DELETE`,
	} {
		if !strings.Contains(query, expected) {
			t.Fatalf("expected %q in MERGE delete query: %s", expected, query)
		}
	}
}

func TestSearchMergeUpdateQuery(t *testing.T) {
	qualified := `search_store.main."public_documents"`
	table := SearchTableConfig{
		StoreColumns: []string{"tenant_id", "id", "body"},
	}
	changedEmbeddings := []searchEmbeddingConfig{{ID: "default", Name: "embedding"}}
	query := searchMergeUpdateQuery(qualified, table, changedEmbeddings, []string{"tenant_id", "id"}, 1536)
	for _, expected := range []string{
		`MERGE INTO search_store.main."public_documents" AS target`,
		`?::VARCHAR AS "tenant_id"`,
		`?::FLOAT[1536] AS "embedding"`,
		`"embedding" = source."embedding"`,
		`"__pgstack_default" = source."__pgstack_default"`,
		`target."tenant_id" = source."tenant_id"`,
		`target."id" = source."id"`,
		`target."__pgstack_chunk_index" = source."__pgstack_chunk_index"`,
		`target."__pgstack_position" <= source."__pgstack_position"`,
		`WHEN MATCHED THEN UPDATE SET`,
	} {
		if !strings.Contains(query, expected) {
			t.Fatalf("expected %q in MERGE update query: %s", expected, query)
		}
	}
}

func TestSearchColumnsPutMetadataLast(t *testing.T) {
	writer := &SearchWriter{config: &Config{}}
	columns := writer.desiredColumns(SearchTableConfig{
		StoreColumns: []string{"id", "body"},
		IndexColumns: []string{"body"},
	})
	names := make([]string, len(columns))
	for i, column := range columns {
		names[i] = column.name
	}
	want := []string{"id", "body", "__pgstack_embedding", "__pgstack_updated_at", "__pgstack_position", "__pgstack_embedding_hash", "__pgstack_chunk_index", "__pgstack_source_key", "__pgstack_text"}
	if !reflect.DeepEqual(names, want) {
		t.Fatalf("unexpected Search column order: %#v", names)
	}
	if columns[2].dataType != "FLOAT[1536]" {
		t.Fatalf("unexpected embedding type: %s", columns[2].dataType)
	}
}

func TestSearchSourceKeyUsesPrimaryKeyColumns(t *testing.T) {
	values := map[string]interface{}{"id": "1", "tenant_id": "2", "body": "ignored"}
	original := searchSourceKey([]string{"tenant_id", "id"}, values)
	values["body"] = "updated"
	if original != searchSourceKey([]string{"tenant_id", "id"}, values) {
		t.Fatal("non-primary-key change affected source key")
	}
	values["id"] = "3"
	if original == searchSourceKey([]string{"tenant_id", "id"}, values) {
		t.Fatal("primary-key change did not affect source key")
	}
}

func TestEmbeddingHashChangesWithSettingsAndInput(t *testing.T) {
	embedding := searchEmbeddingConfig{ID: "id", Columns: []string{"body"}}
	writer := &SearchWriter{config: &Config{}}
	original := writer.embeddingHash(embedding, "body:\"hello\"")
	if original == writer.embeddingHash(embedding, "body:\"updated\"") {
		t.Fatal("input change did not change embedding hash")
	}
	embedding.Columns = []string{"title"}
	if original == writer.embeddingHash(embedding, "body:\"hello\"") {
		t.Fatal("embedding settings change did not change embedding hash")
	}
}

func TestSearchWriterConsumesIndexColumnsAPI(t *testing.T) {
	writer, err := NewSearchWriter(&Config{OpenAIAPIKey: "test", SearchTablesJSON: `[{"name":"public.documents","indexColumns":["title","body"],"storeColumns":["id","title"]}]`})
	if err != nil {
		t.Fatal(err)
	}
	table := writer.tables["public.documents"]
	embeddings := table.embeddings()
	if len(embeddings) != 1 || embeddings[0].Name != "__pgstack_embedding" {
		t.Fatalf("unexpected managed index: %#v", embeddings)
	}
	input := searchEmbeddingInput(embeddings[0], map[string]interface{}{"title": "Jacket", "body": "Waterproof", "id": "ignored"})
	if input != "title:\"Jacket\"\nbody:\"Waterproof\"" {
		t.Fatalf("unexpected indexed input: %s", input)
	}
	for _, config := range []string{
		`[{"name":"public.documents","storeColumns":["id"]}]`,
		`[{"name":"public.documents","indexColumns":["body","body"],"storeColumns":["id"]}]`,
		`[{"name":"public.documents","indexColumns":["__pgstack_embedding"],"storeColumns":["id"]}]`,
	} {
		if _, err := NewSearchWriter(&Config{OpenAIAPIKey: "test", SearchTablesJSON: config}); err == nil {
			t.Fatalf("expected invalid configuration: %s", config)
		}
	}
}

func TestSearchKeywordInputUsesValuesOnly(t *testing.T) {
	table := SearchTableConfig{IndexColumns: []string{"title", "body", "optional", "count"}, StoreColumns: []string{"id"}}
	input := searchKeywordInput(table, map[string]interface{}{"title": "Rain jacket", "body": "Waterproof gear", "count": 42, "id": "ignored"})
	if input != "Rain jacket\nWaterproof gear\n42" {
		t.Fatalf("unexpected full-text input: %q", input)
	}
}

func TestSearchTextPersistence(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	db.SetMaxOpenConns(1)
	ctx := context.Background()
	if _, err := db.ExecContext(ctx, "ATTACH ':memory:' AS search_store"); err != nil {
		t.Fatal(err)
	}
	writer := &SearchWriter{config: &Config{}, duckdb: &DuckDBClient{db: db, config: &Config{}}}
	table := SearchTableConfig{Name: "public.documents", StoreColumns: []string{"id"}, IndexColumns: []string{"body"}}
	if err := writer.ensureTable(ctx, table, []string{"id"}); err != nil {
		t.Fatal(err)
	}
	vector := make([]float32, OPENAI_EMBEDDING_DIMENSIONS)
	vector[0] = 1
	row := &searchRow{table: table, message: &ChangeMessage{Op: OPERATION_CREATE, Source: SourceMetadata{Pk: []string{"id"}, Lsn: 1}}, values: map[string]interface{}{"id": "1"}, text: "rain jacket", vectors: map[string][]float32{"embedding_hash": vector}, hashes: map[string]string{"embedding_hash": "unchanged"}}
	if err := writer.applyRow(ctx, row); err != nil {
		t.Fatal(err)
	}
	var count int
	var text sql.NullString
	if err := db.QueryRowContext(ctx, "SELECT __pgstack_text FROM search_store.main.public_documents").Scan(&text); err != nil || text.String != "rain jacket" {
		t.Fatalf("insert did not persist indexed text: %v %v", text, err)
	}
	// Update text while retaining the existing embedding and its hash.
	row.existing = true
	row.message.Op = OPERATION_UPDATE
	row.message.Source.Lsn = 2
	row.text = "waterproof coat"
	if err := writer.applyRow(ctx, row); err != nil {
		t.Fatal(err)
	}
	var hash string
	if err := db.QueryRowContext(ctx, "SELECT __pgstack_text, __pgstack_embedding_hash FROM search_store.main.public_documents").Scan(&text, &hash); err != nil || text.String != "waterproof coat" || hash != "unchanged" {
		t.Fatalf("text update changed embedding or failed: %v %s %v", text, hash, err)
	}
	row.message.Source.Lsn = 1
	row.text = "stale text"
	if err := writer.applyRow(ctx, row); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, "SELECT __pgstack_text FROM search_store.main.public_documents").Scan(&text); err != nil || text.String != "waterproof coat" {
		t.Fatalf("stale text replaced current data: %v %v", text, err)
	}
	row.message.Op = OPERATION_DELETE
	row.message.Source.Lsn = 3
	if err := writer.applyRow(ctx, row); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, "SELECT count(*) FROM search_store.main.public_documents").Scan(&count); err != nil || count != 0 {
		t.Fatalf("delete retained text: %d %v", count, err)
	}
}

func TestKeywordOnlySearchWithoutEmbeddingKey(t *testing.T) {
	writer, err := NewSearchWriter(&Config{SearchTablesJSON: `[{"name":"public.documents","indexColumns":["body"],"storeColumns":["id","body"],"keywordOnly":true}]`})
	if err != nil {
		t.Fatal(err)
	}
	if writer.embeddings != nil {
		t.Fatal("keyword-only indexing must not initialize an embedding client")
	}
	table := writer.tables["public.documents"]
	for _, column := range writer.desiredColumns(table) {
		if strings.Contains(column.name, "embedding") {
			t.Fatal("unexpected vector column", column.name)
		}
	}
	if got := searchKeywordInput(table, map[string]interface{}{"body": "rain jacket"}); got != "rain jacket" {
		t.Fatal(got)
	}
	if _, err := NewSearchWriter(&Config{SearchTablesJSON: `[{"name":"public.documents","indexColumns":["body"],"storeColumns":["id"]}]`}); err == nil {
		t.Fatal("semantic indexing must still require credentials")
	}
}

// Exercise the exact production DDL against Lance's custom parser. DuckDB's
// normal quoted-identifier rules do not apply to these index statements.
func TestSearchIndexDDLWithLance(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	db.SetMaxOpenConns(1)
	ctx := context.Background()
	if _, err := db.ExecContext(ctx, "LOAD lance"); err != nil {
		t.Skipf("Lance extension unavailable: %v", err)
	}
	datasetPath := t.TempDir() + "/documents.lance"
	query := fmt.Sprintf("COPY (SELECT 'rain jacket' AS %s, [1.0,0.0]::FLOAT[2] AS __pgstack_embedding UNION ALL SELECT 'sun hat', [0.0,1.0]::FLOAT[2]) TO '%s' (FORMAT lance, MODE overwrite)", PGSTACK_TEXT_COLUMN, escapeSQLString(datasetPath))
	if _, err := db.ExecContext(ctx, query); err != nil {
		t.Fatal(err)
	}
	embedding := SearchTableConfig{IndexColumns: []string{"body"}}.embeddings()[0]
	for _, query := range []string{searchTextIndexQuery(datasetPath), searchVectorIndexQuery(datasetPath, embedding, 1)} {
		if _, err := db.ExecContext(ctx, query); err != nil {
			t.Fatalf("%s: %v", query, err)
		}
	}
	writer := &SearchWriter{duckdb: &DuckDBClient{db: db}}
	indices, err := writer.vectorIndexes(ctx, datasetPath)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{PGSTACK_TEXT_INDEX, vectorIndexName(embedding.ID)} {
		if !indices[name] {
			t.Fatalf("missing index %s: %v", name, indices)
		}
		query := searchOptimizeIndexQuery(name, datasetPath)
		if err := writer.executeLanceMaintenance(ctx, query); err != nil {
			t.Fatalf("%s: %v", query, err)
		}
	}
	var count int
	if err := db.QueryRowContext(ctx, "SELECT count(*) FROM lance_fts('"+escapeSQLString(datasetPath)+"', '"+PGSTACK_TEXT_COLUMN+"', 'rain')").Scan(&count); err != nil || count != 1 {
		t.Fatalf("text index search: count=%d, error=%v", count, err)
	}
}
