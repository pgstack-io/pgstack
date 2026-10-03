package main

import (
	"fmt"
	"strings"
	"testing"

	pgQuery "github.com/pganalyze/pg_query_go/v6"
)

func searchTestConfig() *Config {
	return &Config{AwsS3Bucket: "bucket", SearchBasePath: "org/project/search", SearchTablesJSON: `[{"name":"public.documents","indexColumns":["title","body"],"storeColumns":["id","body"]}]`}
}

func remapSearchForTest(t *testing.T, query string) (string, map[int]SearchParameterSpec, error) {
	t.Helper()
	parsed, err := pgQuery.Parse(query)
	if err != nil {
		t.Fatal(err)
	}
	parameters := map[int]SearchParameterSpec{}
	err = (&QueryRemapper{config: searchTestConfig()}).remapSearchRank(parsed.Stmts[0].Stmt.GetSelectStmt(), parameters)
	if err != nil {
		return "", parameters, err
	}
	sql, err := pgQuery.Deparse(parsed)
	return sql, parameters, err
}

func TestSemanticRank(t *testing.T) {
	for _, function := range []string{"semantic_rank"} {
		t.Run(function, func(t *testing.T) {
			query, parameters, err := remapSearchForTest(t, `SELECT *, `+function+`('rain jacket') AS rank FROM search.public_documents ORDER BY `+function+`('rain jacket') LIMIT 5`)
			if err != nil {
				t.Fatal(err)
			}
			spec := parameters[1]
			if len(parameters) != 1 || spec.LiteralText == nil || *spec.LiteralText != "rain jacket" || !spec.RequireNonZero {
				t.Fatalf("unexpected rank parameters: %#v", parameters)
			}
			for _, expected := range []string{"lance_vector_search", "org/project/search/public_documents.lance", "array_cosine_distance", "__pgstack_embedding", "k := 50", "__pgstack_rank = 1", "__pgstack_fallback_docs", "__pgstack_distance AS rank", "$1::real[1536]"} {
				if !strings.Contains(query, expected) {
					t.Fatalf("missing %q: %s", expected, query)
				}
			}
			if !strings.HasPrefix(query, "SELECT public_documents.id, public_documents.body,") {
				t.Fatalf("SELECT * exposes internal columns: %s", query)
			}
			if strings.Contains(query, "rain jacket") || strings.Contains(query, function+"(") {
				t.Fatalf("unmapped rank expression: %s", query)
			}
		})
	}
}

func TestSearchRankParametersAndFilters(t *testing.T) {
	for _, input := range []struct {
		argument, cast string
		parameter      int
	}{
		{"'rain jacket'", "$2::real[1536]", 2},
		{"$2", "$2::real[1536]", 2},
	} {
		query, parameters, err := remapSearchForTest(t, `SELECT documents.id, semantic_rank(`+input.argument+`) AS rank FROM search.public_documents AS documents WHERE documents.id = $1 ORDER BY semantic_rank(`+input.argument+`) LIMIT 5 OFFSET 2`)
		if err != nil {
			t.Fatal(err)
		}
		if _, ok := parameters[input.parameter]; !ok {
			t.Fatalf("missing embedding parameter: %#v", parameters)
		}
		for _, expected := range []string{input.cast, "k := 70", "count(*) FROM __pgstack_candidate_docs) >= 7", "OFFSET 2", "__pgstack_distance AS rank"} {
			if !strings.Contains(query, expected) {
				t.Fatalf("missing %q: %s", expected, query)
			}
		}
		if strings.Count(query, "documents.id = $1") != 2 {
			t.Fatalf("filter must apply before candidate/fallback choice: %s", query)
		}
	}
	query, _, err := remapSearchForTest(t, `SELECT id FROM search.public_documents ORDER BY semantic_rank($1)`)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(query, "LIMIT 10") || !strings.Contains(query, "k := 100") {
		t.Fatalf("missing default limit: %s", query)
	}
}

func TestSearchRankValidation(t *testing.T) {
	for _, test := range []struct{ query, want string }{
		{`SELECT * FROM search.public_documents ORDER BY semantic_rank('')`, "cannot be empty"},
		{`SELECT * FROM search.public_documents ORDER BY semantic_rank('` + strings.Repeat("x", EMBEDDING_MAX_INPUT_BYTES+1) + `')`, "cannot exceed"},
		{`SELECT * FROM search.public_documents ORDER BY semantic_rank(NULL)`, "text literal or bind parameter"},
		{`SELECT * FROM search.public_documents ORDER BY semantic_rank(body)`, "text literal or bind parameter"},
		{`SELECT * FROM search.public_documents ORDER BY semantic_rank()`, "exactly one"},
		{`SELECT * FROM search.public_documents ORDER BY semantic_rank('one', 'two')`, "exactly one"},
		{`SELECT * FROM search.public_documents ORDER BY semantic_rank('one') DESC`, "ascending"},
		{`SELECT * FROM search.public_documents ORDER BY semantic_rank('one') LIMIT 0`, "positive"},
		{`SELECT * FROM search.public_documents ORDER BY semantic_rank('one') LIMIT $1`, "integer"},
		{`SELECT * FROM search.public_documents ORDER BY semantic_rank('one') OFFSET -1`, "non-negative"},
		{`SELECT * FROM search.public_documents ORDER BY semantic_rank(DISTINCT 'one')`, "aggregate"},
		{`SELECT * FROM search.unknown ORDER BY semantic_rank('one')`, "unknown or unindexed"},
		{`SELECT * FROM search.public_documents WHERE body = $1 ORDER BY semantic_rank($1)`, "separate parameter"},
		{`SELECT semantic_rank('two') FROM search.public_documents ORDER BY semantic_rank('one')`, "must match"},
		{`SELECT * FROM search.public_documents ORDER BY semantic_rank('one'), id`, "one ORDER BY"},
	} {
		t.Run(test.want+test.query[:min(35, len(test.query))], func(t *testing.T) {
			_, _, err := remapSearchForTest(t, test.query)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("expected %q, got %v for %s", test.want, err, test.query)
			}
		})
	}
}

func TestSearchTableScanExposesStoredColumns(t *testing.T) {
	config := searchTestConfig()
	remapper := &QueryRemapperTable{parserTable: NewParserTable(config), config: config}
	parsed, err := pgQuery.Parse(`SELECT * FROM search.public_documents AS documents`)
	if err != nil {
		t.Fatal(err)
	}
	statement := parsed.Stmts[0].Stmt.GetSelectStmt()
	statement.FromClause[0] = remapper.RemapTable(statement.FromClause[0])
	query, err := pgQuery.Deparse(parsed)
	if err != nil {
		t.Fatal(err)
	}
	for _, expected := range []string{"SELECT id, body FROM search_store.main.public_documents", "__pgstack_chunk_index = 0", "documents"} {
		if !strings.Contains(query, expected) {
			t.Fatalf("missing %q: %s", expected, query)
		}
	}
}

func TestKeywordAndHybridRankRemapping(t *testing.T) {
	for _, function := range []string{"keyword_rank", "hybrid_rank"} {
		query, specs, err := remapSearchForTest(t, "SELECT id, "+function+"($2) AS rank FROM search.public_documents AS d WHERE d.id = $1 ORDER BY "+function+"($2) LIMIT 2 OFFSET 1")
		if err != nil {
			t.Fatal(err)
		}
		want := "lance_fts"
		if function == "hybrid_rank" {
			want = "lance_vector_search"
		}
		if !strings.Contains(query, want) || (function == "keyword_rank" && strings.Contains(query, "array_cosine_distance")) || !strings.Contains(query, "__pgstack_text") {
			t.Fatalf("incorrect search source: %s", query)
		}
		if specs[2].TextOnly != (function == "keyword_rank") {
			t.Fatalf("incorrect binding: %#v", specs)
		}
		if function == "hybrid_rank" && specs[3].SourceParameter != 2 {
			t.Fatalf("missing original query text: %#v", specs)
		}
		expectedFilters := 1
		if function == "hybrid_rank" {
			expectedFilters = 4
		}
		if strings.Count(query, "d.id = $1") != expectedFilters {
			t.Fatalf("missing filtered fallback: %s", query)
		}
	}
	for _, query := range []string{
		"SELECT semantic_rank('rain') FROM search.public_documents ORDER BY keyword_rank('rain')",
		"SELECT keyword_rank('rain') FROM search.public_documents ORDER BY hybrid_rank('rain')",
		"SELECT id FROM search.public_documents WHERE body = $1 ORDER BY keyword_rank($1)",
	} {
		if _, _, err := remapSearchForTest(t, query); err == nil {
			t.Fatalf("expected mismatched rank/binding rejection: %s", query)
		}
	}
}

func TestSearchFilterPlacement(t *testing.T) {
	for _, test := range []struct {
		filter    string
		prefilter bool
	}{
		{"d.body = $1", true},
		{"d.body = 'waterproof' AND d.id >= '2'", true},
		{"d.body IS NOT NULL", true},
		{"length(d.body) > 3", false},
		{"d.id = 2", false},
		{"d.body = 'waterproof' OR d.body = 'excluded'", false},
	} {
		for _, function := range []string{"semantic_rank", "keyword_rank", "hybrid_rank"} {
			query, _, err := remapSearchForTest(t, "SELECT id FROM search.public_documents AS d WHERE "+test.filter+" ORDER BY "+function+"('rain jacket') LIMIT 2")
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(query, fmt.Sprintf("prefilter := %t", test.prefilter)) {
				t.Fatalf("incorrect prefilter for %s: %s", test.filter, query)
			}
			if !strings.Contains(query, " d WHERE ") || strings.Contains(query, "__pgstack_rank = 1 AND") {
				t.Fatalf("filter was applied after deduplication: %s", query)
			}
			if function == "hybrid_rank" && (!strings.Contains(query, "__pgstack_signals") || !strings.Contains(query, "rank() OVER") || strings.Contains(query, "lance_hybrid_search")) {
				t.Fatalf("missing document fusion: %s", query)
			}
		}
	}
}

func TestKeywordRankDirectRetrieval(t *testing.T) {
	for _, filter := range []string{"", "WHERE d.body = 'waterproof'", "WHERE d.body = $1", "WHERE d.body IS NOT NULL"} {
		query, specs, err := remapSearchForTest(t, "SELECT *, keyword_rank('rain jacket') AS rank FROM search.public_documents AS d "+filter+" ORDER BY keyword_rank('rain jacket') LIMIT 5 OFFSET 2")
		if err != nil {
			t.Fatal(err)
		}
		if strings.Count(query, "lance_fts(") != 1 || !strings.Contains(query, "k := 7") || !strings.Contains(query, "OFFSET 2") {
			t.Fatalf("incorrect direct retrieval: %s", query)
		}
		for _, unnecessary := range []string{"__pgstack_fallback_docs", "row_number()", "__pgstack_candidate_docs", "count(*)"} {
			if strings.Contains(query, unnecessary) {
				t.Fatalf("direct keyword retrieval contains %s: %s", unnecessary, query)
			}
		}
		if len(specs) != 1 {
			t.Fatalf("unexpected hidden parameters: %#v", specs)
		}
		for _, spec := range specs {
			if !spec.TextOnly || spec.RowCountTable != "" {
				t.Fatalf("unexpected binding: %#v", spec)
			}
		}
		if !strings.HasPrefix(query, "SELECT d.id, d.body, __pgstack_distance AS rank") {
			t.Fatalf("incorrect public projection: %s", query)
		}
	}
	query, specs, err := remapSearchForTest(t, "SELECT id FROM search.public_documents WHERE length(body) > 3 ORDER BY keyword_rank('rain jacket') LIMIT 5")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(query, "__pgstack_fallback_docs") || !strings.Contains(query, "k := 50") || len(specs) != 2 {
		t.Fatalf("complex filter lost fallback: %s %#v", query, specs)
	}
}

func TestKeywordOnlyRankModes(t *testing.T) {
	config := searchTestConfig()
	config.SearchTablesJSON = strings.Replace(config.SearchTablesJSON, `"name":`, `"keywordOnly":true,"name":`, 1)
	for _, function := range []string{"keyword_rank", "semantic_rank", "hybrid_rank"} {
		parsed, err := pgQuery.Parse(`SELECT id FROM search.public_documents ORDER BY ` + function + `('rain jacket') LIMIT 5`)
		if err != nil {
			t.Fatal(err)
		}
		parameters := map[int]SearchParameterSpec{}
		err = (&QueryRemapper{config: config}).remapSearchRank(parsed.Stmts[0].Stmt.GetSelectStmt(), parameters)
		if function == "keyword_rank" {
			if err != nil {
				t.Fatal(err)
			}
			for _, parameter := range parameters {
				if !parameter.TextOnly {
					t.Fatal("keyword query requested embeddings")
				}
			}
		} else if err == nil || !strings.Contains(err.Error(), "requires semantic indexing") {
			t.Fatalf("unexpected error: %v", err)
		}
	}
}
