package main

import (
	"bytes"
	"context"
	"encoding/json"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgproto3"
)

func TestEmbeddingClientAndLiteralCache(t *testing.T) {
	var logs bytes.Buffer
	previousOutput := log.Writer()
	log.SetOutput(&logs)
	t.Cleanup(func() { log.SetOutput(previousOutput) })

	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		requests++
		if request.Header.Get("Authorization") != "Bearer test-key" {
			t.Errorf("unexpected authorization header: %q", request.Header.Get("Authorization"))
		}
		var body embeddingRequest
		if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		if body.Model != OPENAI_EMBEDDING_MODEL || body.Dimensions != 2 || len(body.Input) != 1 || body.Input[0] != "dog" {
			t.Errorf("unexpected embedding request: %#v", body)
		}
		response.Header().Set("Content-Type", "application/json")
		_, _ = response.Write([]byte(`{"data":[{"embedding":[0.6,0.8],"index":0}],"usage":{"prompt_tokens":7}}`))
	}))
	t.Cleanup(server.Close)

	client := NewEmbeddingClient(&Config{LogLevel: LOG_LEVEL_INFO, OpenAIAPIKey: "test-key"})
	client.dimensions = 2
	client.url = server.URL
	queryHandler := &QueryHandler{Config: &Config{}, EmbeddingClient: client}
	dog := "dog"
	specs := map[int]SearchParameterSpec{
		1: {Dimensions: 2, RequireNonZero: true, LiteralText: &dog},
		2: {Dimensions: 2, RequireNonZero: true, LiteralText: &dog},
	}
	variables, err := queryHandler.resolveEmbeddedLiteralParameters(context.Background(), specs)
	if err != nil {
		t.Fatal(err)
	}
	if requests != 1 {
		t.Fatalf("embedding requests = %d, want 1", requests)
	}
	if !strings.Contains(logs.String(), "Built embeddings via OpenAI | Input tokens: 7") {
		t.Fatalf("expected embedding usage log, got %q", logs.String())
	}
	for _, variable := range variables {
		vector := variable.([]float32)
		if len(vector) != 2 || vector[0] != 0.6 || vector[1] != 0.8 {
			t.Fatalf("unexpected embedded vector: %#v", vector)
		}
	}
	prepared := &PreparedStatement{VectorParameters: map[int]SearchParameterSpec{
		1: {Dimensions: 2, RequireNonZero: true},
	}}
	_, prepared, err = queryHandler.HandleBindQuery(&pgproto3.Bind{Parameters: [][]byte{[]byte("dog")}}, prepared)
	if err != nil {
		t.Fatal(err)
	}
	vector := prepared.Variables[0].([]float32)
	if len(vector) != 2 || vector[0] != 0.6 || vector[1] != 0.8 {
		t.Fatalf("unexpected bound embedding: %#v", vector)
	}
	prepared = &PreparedStatement{VectorParameters: map[int]SearchParameterSpec{
		1: {Dimensions: 2, RequireNonZero: true, LiteralText: &dog},
	}}
	_, prepared, err = queryHandler.HandleBindQuery(&pgproto3.Bind{}, prepared)
	if err != nil {
		t.Fatal(err)
	}
	vector = prepared.Variables[0].([]float32)
	if len(vector) != 2 || vector[0] != 0.6 || vector[1] != 0.8 {
		t.Fatalf("unexpected hidden literal embedding: %#v", vector)
	}
}

func TestEmbeddingClientValidatesInput(t *testing.T) {
	client := NewEmbeddingClient(&Config{})
	if _, err := client.Embed(context.Background(), "dog"); err == nil {
		t.Fatal("expected missing API key error")
	}
	client.apiKey = "test-key"
	if _, err := client.Embed(context.Background(), ""); err == nil {
		t.Fatal("expected empty input error")
	}
	if _, err := client.Embed(context.Background(), strings.Repeat("x", EMBEDDING_MAX_INPUT_BYTES+1)); err == nil || !strings.Contains(err.Error(), "cannot exceed 3200 bytes") {
		t.Fatalf("expected oversized input error, got %v", err)
	}
}

func TestQueryHandlerHidesEmbeddingProviderError(t *testing.T) {
	var logs bytes.Buffer
	previousOutput := log.Writer()
	log.SetOutput(&logs)
	t.Cleanup(func() { log.SetOutput(previousOutput) })

	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		response.WriteHeader(http.StatusBadRequest)
		_, _ = response.Write([]byte("sensitive upstream error"))
	}))
	t.Cleanup(server.Close)
	config := &Config{OpenAIAPIKey: "test-key"}
	client := NewEmbeddingClient(config)
	client.dimensions = 2
	client.url = server.URL
	queryHandler := &QueryHandler{Config: config, EmbeddingClient: client}

	_, err := queryHandler.embedVectorParameter(context.Background(), "dog", SearchParameterSpec{Dimensions: 2}, nil)
	if err == nil || err.Error() != "search rank failed" {
		t.Fatalf("unexpected public error: %v", err)
	}
	if strings.Contains(err.Error(), "upstream") || !strings.Contains(logs.String(), "sensitive upstream error") {
		t.Fatalf("expected provider detail only in server log; error=%q logs=%q", err, logs.String())
	}
}
