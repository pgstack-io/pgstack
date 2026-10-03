package main

import (
	"bytes"
	"log"
	"strings"
	"testing"
)

func TestRedactVectorLiterals(t *testing.T) {
	vector := "[-0.003017425537109375,-0.026702880859375,0.0050506591796875]"
	tests := []struct {
		query string
		want  string
	}{
		{
			query: `SELECT * FROM search.test ORDER BY embedding <=> '` + vector + `' LIMIT 5`,
			want:  `embedding <=> '[vector omitted]'`,
		},
		{
			query: `SELECT array_distance(embedding, ARRAY[-0.003017425537109375,-0.026702880859375]::REAL[1536])`,
			want:  `ARRAY[vector omitted]::REAL[1536]`,
		},
		{
			query: `SELECT array_cosine_distance(embedding, ARRAY[-0.003017425537109375,-0.026702880859375]::float4[1536])`,
			want:  `ARRAY[vector omitted]::float4[1536]`,
		},
	}
	for _, test := range tests {
		redacted := redactVectorLiterals(test.query)
		if strings.Contains(redacted, "-0.003017425537109375") || !strings.Contains(redacted, test.want) {
			t.Fatalf("unexpected redacted query: %s", redacted)
		}
	}
}

func TestLoggerRedactsRemappedVectorQuery(t *testing.T) {
	var output bytes.Buffer
	previousOutput := log.Writer()
	log.SetOutput(&output)
	t.Cleanup(func() { log.SetOutput(previousOutput) })

	LogInfo(&Config{LogLevel: LOG_LEVEL_INFO}, "Remapped query:", `SELECT array_cosine_distance(embedding, ARRAY[-0.003017425537109375,-0.026702880859375]::float4[1536])`)
	logged := output.String()
	if strings.Contains(logged, "-0.003017425537109375") || !strings.Contains(logged, "Remapped query: SELECT array_cosine_distance(embedding, ARRAY[vector omitted]::float4[1536])") {
		t.Fatalf("unexpected remapped-query log: %s", logged)
	}
}

func TestRedactVectorVariables(t *testing.T) {
	variables := []interface{}{"user", "[0.1,0.2]"}
	redacted := redactVectorVariables(variables, map[int]SearchParameterSpec{
		2: {Dimensions: 1536},
	})
	if redacted[0] != "user" || redacted[1] != "[vector omitted: 1536 dimensions]" {
		t.Fatalf("unexpected redacted variables: %#v", redacted)
	}
	if variables[1] != "[0.1,0.2]" {
		t.Fatalf("redaction mutated bound variables: %#v", variables)
	}
}
