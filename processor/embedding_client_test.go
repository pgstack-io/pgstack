package main

import (
	"bytes"
	"context"
	"io"
	"log"
	"net/http"
	"strings"
	"testing"
)

type embeddingRoundTripperFunc func(*http.Request) (*http.Response, error)

func (fn embeddingRoundTripperFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return fn(request)
}

func TestEmbeddingClientLogsOpenAIUsage(t *testing.T) {
	var logs bytes.Buffer
	previousOutput := log.Writer()
	log.SetOutput(&logs)
	t.Cleanup(func() { log.SetOutput(previousOutput) })

	client := &EmbeddingClient{
		config:     &Config{LogLevel: LOG_LEVEL_INFO},
		apiKey:     "test-key",
		model:      "text-embedding-3-small",
		dimensions: 2,
		httpClient: &http.Client{Transport: embeddingRoundTripperFunc(func(*http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusOK,
				Body: io.NopCloser(strings.NewReader(`{
					"data":[{"embedding":[0.1,0.2],"index":0}],
					"usage":{"prompt_tokens":7,"total_tokens":7}
				}`)),
			}, nil
		})},
	}

	if _, err := client.Embed(context.Background(), []string{"hello"}); err != nil {
		t.Fatal(err)
	}
	if expected := "Built embeddings via OpenAI | Input tokens: 7"; !strings.Contains(logs.String(), expected) {
		t.Fatalf("expected log to contain %q, got %q", expected, logs.String())
	}
	if strings.Contains(logs.String(), client.model) {
		t.Fatalf("expected log to omit model, got %q", logs.String())
	}
}
