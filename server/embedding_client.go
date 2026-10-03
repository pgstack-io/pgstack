package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

const (
	OPENAI_EMBEDDING_MODEL      = "text-embedding-3-small"
	OPENAI_EMBEDDING_DIMENSIONS = 1536
	OPENAI_EMBEDDINGS_URL       = "https://api.openai.com/v1/embeddings"
	EMBEDDING_MAX_INPUT_BYTES   = 3200
)

type EmbeddingClient struct {
	config     *Config
	apiKey     string
	model      string
	dimensions int
	url        string
	httpClient *http.Client
}

type embeddingRequest struct {
	Model      string   `json:"model"`
	Input      []string `json:"input"`
	Dimensions int      `json:"dimensions"`
}

type embeddingResponse struct {
	Data []struct {
		Embedding []float32 `json:"embedding"`
		Index     int       `json:"index"`
	} `json:"data"`
	Usage struct {
		PromptTokens int `json:"prompt_tokens"`
	} `json:"usage"`
}

func NewEmbeddingClient(config *Config) *EmbeddingClient {
	return &EmbeddingClient{
		config:     config,
		apiKey:     config.OpenAIAPIKey,
		model:      OPENAI_EMBEDDING_MODEL,
		dimensions: OPENAI_EMBEDDING_DIMENSIONS,
		url:        OPENAI_EMBEDDINGS_URL,
		httpClient: &http.Client{Timeout: 30 * time.Second},
	}
}

func (client *EmbeddingClient) Embed(ctx context.Context, input string) ([]float32, error) {
	if err := validateEmbeddingInput(input); err != nil {
		return nil, err
	}
	if client.apiKey == "" {
		return nil, fmt.Errorf("OPENAI_API_KEY is required to use search rank")
	}
	body, err := json.Marshal(embeddingRequest{
		Model:      client.model,
		Input:      []string{input},
		Dimensions: client.dimensions,
	})
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, client.url, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+client.apiKey)
	req.Header.Set("Content-Type", "application/json")

	var response *http.Response
	for attempt := 0; attempt < 4; attempt++ {
		response, err = client.httpClient.Do(req)
		if err == nil && response.StatusCode != http.StatusTooManyRequests && response.StatusCode < 500 {
			break
		}
		if attempt == 3 {
			break
		}
		if response != nil {
			response.Body.Close()
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(time.Duration(1<<attempt) * time.Second):
		}
		req.Body = io.NopCloser(bytes.NewReader(body))
	}
	if err != nil {
		return nil, fmt.Errorf("embedding request failed: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		message, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
		return nil, fmt.Errorf("embedding request returned %s: %s", response.Status, string(message))
	}

	var payload embeddingResponse
	if err := json.NewDecoder(response.Body).Decode(&payload); err != nil {
		return nil, fmt.Errorf("invalid embedding response: %w", err)
	}
	if len(payload.Data) != 1 || payload.Data[0].Index != 0 || len(payload.Data[0].Embedding) != client.dimensions {
		return nil, fmt.Errorf("invalid embedding response item")
	}
	LogInfo(client.config, fmt.Sprintf("Built embeddings via OpenAI | Input tokens: %d", payload.Usage.PromptTokens))
	return payload.Data[0].Embedding, nil
}

func validateEmbeddingInput(input string) error {
	if input == "" {
		return fmt.Errorf("search rank input cannot be empty")
	}
	if len(input) > EMBEDDING_MAX_INPUT_BYTES {
		return fmt.Errorf("search rank input cannot exceed %d bytes", EMBEDDING_MAX_INPUT_BYTES)
	}
	return nil
}
