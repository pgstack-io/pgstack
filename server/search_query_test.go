package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgproto3"
)

// Exercise the full SQL remapper and wire handler with real DuckDB execution.
// Only external embedding generation and the Lance candidate source are stubbed.
func TestSearchRankQueryExecution(t *testing.T) {
	handler := initQueryHandlerForTest(t)
	handler.Config.SearchTablesJSON = searchTestConfig().SearchTablesJSON
	handler.Config.SearchBasePath = "search"
	vector := make([]float32, OPENAI_EMBEDDING_DIMENSIONS)
	vector[0] = 1
	requests := 0
	embeddingServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		var request embeddingRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Error(err)
		}
		if len(request.Input) != 1 || request.Input[0] != "rain jacket" {
			t.Errorf("unexpected embedding input: %#v", request.Input)
		}
		_ = json.NewEncoder(w).Encode(map[string]interface{}{"data": []interface{}{map[string]interface{}{"index": 0, "embedding": vector}}})
	}))
	t.Cleanup(embeddingServer.Close)
	handler.EmbeddingClient.apiKey = "test"
	handler.EmbeddingClient.url = embeddingServer.URL
	ctx := context.Background()
	for _, sql := range []string{
		`ATTACH ':memory:' AS search_store`,
		`CREATE TABLE search_store.main.public_documents (id VARCHAR, body VARCHAR, __pgstack_embedding FLOAT[1536], __pgstack_source_key VARCHAR, __pgstack_chunk_index INTEGER)`,
		`INSERT INTO search_store.main.public_documents VALUES
   ('1','waterproof', array_resize([1.0],1536,0)::FLOAT[1536], '1', 0),
   ('1','waterproof', array_resize([0.9,0.1],1536,0)::FLOAT[1536], '1', 1),
   ('2','waterproof', array_resize([0.6,0.8],1536,0)::FLOAT[1536], '2', 0),
   ('3','excluded', array_resize([1.0],1536,0)::FLOAT[1536], '3', 0)`,
		// Deliberately incomplete candidates verify the fallback after filtering/deduplication.
		`CREATE MACRO lance_vector_search(dataset, embedding_column, query_vector, k := 10, prefilter := false) AS TABLE SELECT * FROM search_store.main.public_documents WHERE id IN ('1','3')`,
		`CREATE MACRO lance_fts(dataset, text_column, query_text, k := 10, prefilter := false) AS TABLE SELECT *, CASE id WHEN '2' THEN 8.0 ELSE 2.0 END AS _score FROM search_store.main.public_documents WHERE query_text = 'rain jacket' AND id IN ('1','2') AND __pgstack_chunk_index = 0 ORDER BY _score DESC LIMIT k`,
	} {
		if _, err := handler.ServerDuckdbClient.ExecContext(ctx, sql); err != nil {
			t.Fatal(err)
		}
	}
	for _, function := range []string{"semantic_rank", "keyword_rank", "hybrid_rank"} {
		t.Run(function, func(t *testing.T) {
			before := requests
			messages, err := handler.HandleSimpleQuery(fmt.Sprintf(`SELECT * FROM search.public_documents AS d WHERE d.body = 'waterproof' ORDER BY %s('rain jacket') LIMIT 2`, function))
			if err != nil {
				t.Fatal(err)
			}
			expectedRequests := 1
			if function == "keyword_rank" {
				expectedRequests = 0
			}
			if requests != before+expectedRequests {
				t.Fatalf("expected one embedding call, got %d", requests-before)
			}
			assertMessageTypes(t, messages, "*pgproto3.RowDescription", "*pgproto3.DataRow", "*pgproto3.DataRow", "*pgproto3.CommandComplete")
			description := messages[0].(*pgproto3.RowDescription)
			if len(description.Fields) != 2 || string(description.Fields[0].Name) != "id" || string(description.Fields[1].Name) != "body" {
				t.Fatalf("internal columns leaked: %#v", description.Fields)
			}
			first, second := "1", "2"
			if function == "keyword_rank" {
				first, second = "2", "1"
			}
			assertDataRow(t, messages[1], []string{first, "waterproof"})
			assertDataRow(t, messages[2], []string{second, "waterproof"})
		})
	}
	for _, input := range []struct {
		query      string
		parameters [][]byte
	}{
		{`SELECT id FROM search.public_documents WHERE body = $1 ORDER BY semantic_rank($2) LIMIT 1 OFFSET 1`, [][]byte{[]byte("waterproof"), []byte("rain jacket")}},
		{`SELECT id FROM search.public_documents WHERE body = $1 ORDER BY semantic_rank('rain jacket') LIMIT 1 OFFSET 1`, [][]byte{[]byte("waterproof")}},
	} {
		_, prepared, err := handler.HandleParseQuery(&pgproto3.Parse{Query: input.query})
		if err != nil {
			t.Fatal(err)
		}
		_, prepared, err = handler.HandleBindQuery(&pgproto3.Bind{Parameters: input.parameters}, prepared)
		if err != nil {
			t.Fatal(err)
		}
		messages, err := handler.HandleExecuteQuery(&pgproto3.Execute{}, prepared)
		if err != nil {
			t.Fatal(err)
		}
		assertDataRow(t, messages[0], []string{"2"})
		prepared.Statement.Close()
	}
	for _, function := range []string{"keyword_rank", "hybrid_rank"} {
		for _, argument := range []string{"$2", "'rain jacket'"} {
			parameters := [][]byte{[]byte("waterproof")}
			if argument == "$2" {
				parameters = append(parameters, []byte("rain jacket"))
			}
			_, prepared, err := handler.HandleParseQuery(&pgproto3.Parse{Query: fmt.Sprintf("SELECT id, %s(%s) AS rank FROM search.public_documents WHERE body = $1 ORDER BY %s(%s) LIMIT 1 OFFSET 1", function, argument, function, argument)})
			if err != nil {
				t.Fatal(err)
			}
			_, prepared, err = handler.HandleBindQuery(&pgproto3.Bind{Parameters: parameters}, prepared)
			if err != nil {
				t.Fatal(err)
			}
			messages, err := handler.HandleExecuteQuery(&pgproto3.Execute{}, prepared)
			if err != nil {
				t.Fatal(err)
			}
			expected := "1"
			if function == "hybrid_rank" {
				expected = "2"
			}
			if string(messages[0].(*pgproto3.DataRow).Values[0]) != expected {
				t.Fatalf("unexpected %s results: %#v", function, messages)
			}
			prepared.Statement.Close()
		}
	}
	before := requests
	messages, err := handler.HandleSimpleQuery(`SELECT id FROM search.public_documents ORDER BY keyword_rank('missing')`)
	if err != nil {
		t.Fatal(err)
	}
	assertMessageTypes(t, messages, "*pgproto3.RowDescription", "*pgproto3.CommandComplete")
	if requests != before {
		t.Fatal("keyword search generated embeddings")
	}
	// The ANN-only branch must still collapse duplicate chunks and expose an explicit rank.
	ranked, err := handler.HandleSimpleQuery(`SELECT d.id, semantic_rank('rain jacket') AS rank FROM search.public_documents AS d WHERE d.body = 'waterproof' ORDER BY semantic_rank('rain jacket') LIMIT 1`)
	if err != nil {
		t.Fatal(err)
	}
	assertDataRow(t, ranked[1], []string{"1", "0"})
	if string(ranked[0].(*pgproto3.RowDescription).Fields[1].Name) != "rank" {
		t.Fatal("rank alias was lost")
	}
	for _, sql := range []string{
		`SELECT id FROM search.public_documents ORDER BY embedding <=> '[1,2]'`,
		`SELECT id FROM search.public_documents ORDER BY embedding <=> pgstack.embed('rain jacket')`,
	} {
		if _, err := handler.HandleSimpleQuery(sql); err == nil {
			t.Fatalf("obsolete API unexpectedly supported: %s", sql)
		}
	}
	messages, err = handler.HandleSimpleQuery(`SELECT * FROM search.public_documents ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	if len(messages[0].(*pgproto3.RowDescription).Fields) != 2 {
		t.Fatal("table scan exposes internal columns")
	}
	for _, field := range messages[0].(*pgproto3.RowDescription).Fields {
		if strings.HasPrefix(string(field.Name), "__pgstack_") {
			t.Fatal("internal field exposed")
		}
	}
}

// Verify generated SQL against the real extension when it is locally available.
// A local dataset replaces only the S3 URI; ranking and binding are unchanged.
func TestSearchRankLanceExecution(t *testing.T) {
	handler := initQueryHandlerForTest(t)
	ctx := context.Background()
	if _, err := handler.ServerDuckdbClient.ExecContext(ctx, "LOAD lance"); err != nil {
		t.Skipf("Lance extension unavailable: %v", err)
	}
	handler.Config.SearchTablesJSON = searchTestConfig().SearchTablesJSON
	handler.Config.SearchBasePath = "search"
	vector := make([]float32, OPENAI_EMBEDDING_DIMENSIONS)
	vector[0] = 1
	requests := 0
	embeddingServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		_ = json.NewEncoder(w).Encode(map[string]interface{}{"data": []interface{}{map[string]interface{}{"index": 0, "embedding": vector}}})
	}))
	t.Cleanup(embeddingServer.Close)
	handler.EmbeddingClient.apiKey = "test"
	handler.EmbeddingClient.url = embeddingServer.URL
	root := t.TempDir()
	dataset := root + "/public_documents.lance"
	for _, query := range []string{
		fmt.Sprintf("ATTACH '%s' AS search_store (TYPE lance)", strings.ReplaceAll(root, "'", "''")),
		`CREATE TABLE search_store.main.public_documents AS
		 SELECT '1' AS id, 'waterproof' AS body, array_resize([1.0],1536,0)::FLOAT[1536] AS __pgstack_embedding, '1' AS __pgstack_source_key, 0::UINTEGER AS __pgstack_chunk_index, 'rain jacket waterproof' AS __pgstack_text
		 UNION ALL SELECT '2','waterproof',array_resize([0.6,0.8],1536,0)::FLOAT[1536],'2',0,'rain jacket rain jacket'
		 UNION ALL SELECT '3','excluded',array_resize([0.9,0.43589],1536,0)::FLOAT[1536],'3',0,'sun hat'`,
		fmt.Sprintf("CREATE INDEX text_idx ON '%s' (__pgstack_text) USING INVERTED", strings.ReplaceAll(dataset, "'", "''")),
	} {
		if _, err := handler.ServerDuckdbClient.ExecContext(ctx, query); err != nil {
			t.Fatal(err)
		}
	}
	for _, function := range []string{"semantic_rank", "keyword_rank", "hybrid_rank"} {
		for _, argument := range []string{"'rain jacket'", "$1"} {
			original := fmt.Sprintf("SELECT id, %s(%s) AS rank FROM search.public_documents WHERE body = 'waterproof' ORDER BY %s(%s) LIMIT 2", function, argument, function, argument)
			remapped, err := handler.QueryRemapper.ParseAndRemapQuery(original)
			if err != nil {
				t.Fatal(err)
			}
			query := strings.ReplaceAll(remapped.Statements[0], "s3://test/search/public_documents.lance", dataset)
			before := requests
			var variables []interface{}
			if argument == "$1" {
				statement, err := handler.ServerDuckdbClient.PrepareContext(ctx, query)
				if err != nil {
					t.Fatal(err)
				}
				prepared := &PreparedStatement{OriginalQuery: original, Query: query, Statement: statement, VectorParameters: remapped.VectorParameters}
				_, prepared, err = handler.HandleBindQuery(&pgproto3.Bind{Parameters: [][]byte{[]byte("rain jacket")}}, prepared)
				if err != nil {
					t.Fatal(err)
				}
				variables = prepared.Variables
				statement.Close()
			} else {
				variables, err = handler.resolveEmbeddedLiteralParameters(ctx, remapped.VectorParameters)
				if err != nil {
					t.Fatal(err)
				}
			}
			rows, err := handler.ServerDuckdbClient.QueryContext(ctx, query, variables...)
			if err != nil {
				t.Fatal(err)
			}
			var ids []string
			for rows.Next() {
				var id string
				var rank float64
				if err := rows.Scan(&id, &rank); err != nil {
					t.Fatal(err)
				}
				if function != "semantic_rank" && rank >= 0 {
					t.Fatalf("expected negated relevance score: %s %g", function, rank)
				}
				ids = append(ids, id)
			}
			if err := rows.Err(); err != nil {
				t.Fatal(err)
			}
			rows.Close()
			if len(ids) != 2 {
				t.Fatalf("%s: unexpected results: %v", function, ids)
			}
			if function == "semantic_rank" && ids[0] != "1" || function == "keyword_rank" && ids[0] != "2" {
				t.Fatalf("%s: incorrect ranking: %v", function, ids)
			}
			expectedRequests := 1
			if function == "keyword_rank" {
				expectedRequests = 0
			}
			if requests-before != expectedRequests {
				t.Fatalf("%s: unexpected embedding requests: %d", function, requests-before)
			}
		}
	}
	// A selective filter leaves fewer documents than LIMIT. Keyword search returns
	// its direct matches; hybrid search exercises its fallback.
	for _, function := range []string{"keyword_rank", "hybrid_rank"} {
		original := fmt.Sprintf("SELECT id FROM search.public_documents WHERE id = '2' ORDER BY %s('rain jacket') LIMIT 2", function)
		remapped, err := handler.QueryRemapper.ParseAndRemapQuery(original)
		if err != nil {
			t.Fatal(err)
		}
		variables, err := handler.resolveEmbeddedLiteralParameters(ctx, remapped.VectorParameters)
		if err != nil {
			t.Fatal(err)
		}
		query := strings.ReplaceAll(remapped.Statements[0], "s3://test/search/public_documents.lance", dataset)
		rows, err := handler.ServerDuckdbClient.QueryContext(ctx, query, variables...)
		if err != nil {
			t.Fatal(err)
		}
		if !rows.Next() {
			t.Fatalf("%s fallback lost matching document: %v", function, rows.Err())
		}
		var id string
		if err := rows.Scan(&id); err != nil || id != "2" {
			t.Fatalf("%s fallback returned %s: %v", function, id, err)
		}
		if rows.Next() {
			t.Fatalf("%s fallback leaked nonmatches", function)
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		rows.Close()
	}

	// Exercise native prefiltering, bound filters, and expressions that require
	// DuckDB evaluation, rather than only checking generated SQL.
	for _, test := range []struct {
		filter     string
		parameters [][]byte
		want       int
	}{
		{"d.id = '2'", nil, 1},
		{"'2' = d.id", nil, 1},
		{"d.id >= '2' AND d.body = 'waterproof'", nil, 1},
		{"d.body IS NOT NULL", nil, 3},
		{"d.id = 2", nil, 1},
		{"CAST(d.id AS INTEGER) = 2", nil, 1},
		{"length(d.body) > 3", nil, 3},
		{"d.id = '1' OR d.id = '2'", nil, 2},
		{"d.id IN ('1', '2')", nil, 2},
		{"d.body LIKE 'water%'", nil, 2},
		{"d.body IS NULL", nil, 0},
		{"d.id = $1", [][]byte{[]byte("2")}, 1},
	} {
		for _, function := range []string{"semantic_rank", "keyword_rank", "hybrid_rank"} {
			original := fmt.Sprintf("SELECT id FROM search.public_documents AS d WHERE %s ORDER BY %s('rain jacket') LIMIT 3", test.filter, function)
			remapped, err := handler.QueryRemapper.ParseAndRemapQuery(original)
			if err != nil {
				t.Fatal(err)
			}
			query := strings.ReplaceAll(remapped.Statements[0], "s3://test/search/public_documents.lance", dataset)
			var variables []interface{}
			if len(test.parameters) > 0 {
				statement, err := handler.ServerDuckdbClient.PrepareContext(ctx, query)
				if err != nil {
					t.Fatal(err)
				}
				prepared := &PreparedStatement{OriginalQuery: original, Query: query, Statement: statement, VectorParameters: remapped.VectorParameters}
				_, prepared, err = handler.HandleBindQuery(&pgproto3.Bind{Parameters: test.parameters}, prepared)
				if err != nil {
					t.Fatal(err)
				}
				variables = prepared.Variables
				statement.Close()
			} else {
				variables, err = handler.resolveEmbeddedLiteralParameters(ctx, remapped.VectorParameters)
				if err != nil {
					t.Fatal(err)
				}
			}
			rows, err := handler.ServerDuckdbClient.QueryContext(ctx, query, variables...)
			if err != nil {
				t.Fatalf("%s WHERE %s: %v", function, test.filter, err)
			}
			count := 0
			for rows.Next() {
				count++
			}
			if err := rows.Err(); err != nil {
				t.Fatal(err)
			}
			rows.Close()
			want := test.want
			if function == "keyword_rank" && want == 3 {
				want = 2
			}
			if count != want {
				t.Fatalf("%s WHERE %s: expected %d, got %d", function, test.filter, want, count)
			}
		}
	}

	// Populate more excluded top matches than the candidate budget, then query
	// the candidate CTE directly so an exhaustive fallback cannot hide failed pushdown.
	if _, err := handler.ServerDuckdbClient.ExecContext(ctx, `INSERT INTO search_store.main.public_documents
		SELECT 'blocked-' || CAST(i AS VARCHAR), 'excluded', array_resize([1.0],1536,0)::FLOAT[1536], 'blocked-' || CAST(i AS VARCHAR), 0, repeat('rain jacket ', 100)
		FROM range(40) AS blocked(i)`); err != nil {
		t.Fatal(err)
	}
	for _, function := range []string{"semantic_rank", "keyword_rank", "hybrid_rank"} {
		remapped, err := handler.QueryRemapper.ParseAndRemapQuery(fmt.Sprintf("SELECT id FROM search.public_documents AS d WHERE d.body = $1 ORDER BY %s('rain jacket') LIMIT 2", function))
		if err != nil {
			t.Fatal(err)
		}
		query := strings.ReplaceAll(remapped.Statements[0], "s3://test/search/public_documents.lance", dataset)
		query = strings.Replace(query, "FROM __pgstack_search_docs", "FROM __pgstack_candidate_docs", 1)
		statement, err := handler.ServerDuckdbClient.PrepareContext(ctx, query)
		if err != nil {
			t.Fatal(err)
		}
		prepared := &PreparedStatement{Query: query, Statement: statement, VectorParameters: remapped.VectorParameters}
		_, prepared, err = handler.HandleBindQuery(&pgproto3.Bind{Parameters: [][]byte{[]byte("waterproof")}}, prepared)
		if err != nil {
			t.Fatal(err)
		}
		rows, err := statement.QueryContext(ctx, prepared.Variables...)
		if err != nil {
			t.Fatal(err)
		}
		count := 0
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil || (id != "1" && id != "2") {
				t.Fatalf("prefilter returned %s: %v", id, err)
			}
			count++
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		rows.Close()
		statement.Close()
		if count != 2 {
			t.Fatalf("%s prefilter lost a top matching document: got %d", function, count)
		}
	}
	// The whole-document keyword match lives on chunk 0, while the strongest
	// semantic match is a later chunk. Duplicate semantic chunks must not boost RRF.
	for _, query := range []string{
		`DELETE FROM search_store.main.public_documents WHERE id = '1'`,
		`INSERT INTO search_store.main.public_documents VALUES ('1','waterproof',array_resize([0.0,1.0],1536,0)::FLOAT[1536],'1',0,'rain jacket rain jacket rain jacket')`,
		`UPDATE search_store.main.public_documents SET __pgstack_text = 'rain jacket' WHERE id = '2'`,
		`INSERT INTO search_store.main.public_documents VALUES ('1','waterproof',array_resize([1.0],1536,0)::FLOAT[1536],'1',1,NULL), ('1','waterproof',array_resize([1.0],1536,0)::FLOAT[1536],'1',2,NULL)`,
	} {
		if _, err := handler.ServerDuckdbClient.ExecContext(ctx, query); err != nil {
			t.Fatal(err)
		}
	}
	remapped, err := handler.QueryRemapper.ParseAndRemapQuery("SELECT id, hybrid_rank('rain jacket') AS rank FROM search.public_documents WHERE body = 'waterproof' ORDER BY hybrid_rank('rain jacket') LIMIT 2")
	if err != nil {
		t.Fatal(err)
	}
	variables, err := handler.resolveEmbeddedLiteralParameters(ctx, remapped.VectorParameters)
	if err != nil {
		t.Fatal(err)
	}
	query := strings.ReplaceAll(remapped.Statements[0], "s3://test/search/public_documents.lance", dataset)
	rows, err := handler.ServerDuckdbClient.QueryContext(ctx, query, variables...)
	if err != nil {
		t.Fatal(err)
	}
	for i, wantID := range []string{"1", "2"} {
		if !rows.Next() {
			t.Fatalf("missing hybrid document %s: %v", wantID, rows.Err())
		}
		var id string
		var rank float64
		if err := rows.Scan(&id, &rank); err != nil {
			t.Fatal(err)
		}
		wantRank := -2.0 / float64(HYBRID_RRF_CONSTANT+i+1)
		if id != wantID || math.Abs(rank-wantRank) > 1e-9 {
			t.Fatalf("incorrect document fusion: id=%s rank=%g, expected %s %g", id, rank, wantID, wantRank)
		}
	}
	if rows.Next() {
		t.Fatal("duplicate chunks leaked through fusion")
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	rows.Close()

	// Only chunk 0 is indexed for FTS. Direct keyword retrieval must still return
	// each source document once when it has multiple semantic chunks.
	remapped, err = handler.QueryRemapper.ParseAndRemapQuery("SELECT id FROM search.public_documents WHERE body = 'waterproof' ORDER BY keyword_rank('rain jacket') LIMIT 10")
	if err != nil {
		t.Fatal(err)
	}
	variables, err = handler.resolveEmbeddedLiteralParameters(ctx, remapped.VectorParameters)
	if err != nil {
		t.Fatal(err)
	}
	query = strings.ReplaceAll(remapped.Statements[0], "s3://test/search/public_documents.lance", dataset)
	rows, err = handler.ServerDuckdbClient.QueryContext(ctx, query, variables...)
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		if seen[id] {
			t.Fatalf("keyword retrieval duplicated source document %s", id)
		}
		seen[id] = true
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	rows.Close()
	if len(seen) != 2 || !seen["1"] || !seen["2"] {
		t.Fatalf("incorrect keyword documents: %v", seen)
	}

}
