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
	EMBEDDING_BATCH_SIZE            = 256
	EMBEDDING_MAX_INPUT_BYTES       = 3200
	EMBEDDING_BATCH_MAX_INPUT_BYTES = 250000
	OPENAI_EMBEDDING_MODEL          = "text-embedding-3-small"
	OPENAI_EMBEDDING_DIMENSIONS     = 1536
)

type EmbeddingClient struct {
	config     *Config
	apiKey     string
	model      string
	dimensions int
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

func NewEmbeddingClient(config *Config) (*EmbeddingClient, error) {
	if config.OpenAIAPIKey == "" {
		return nil, fmt.Errorf("OPENAI_API_KEY is required when Search is enabled")
	}
	return &EmbeddingClient{
		config:     config,
		apiKey:     config.OpenAIAPIKey,
		model:      OPENAI_EMBEDDING_MODEL,
		dimensions: OPENAI_EMBEDDING_DIMENSIONS,
		httpClient: &http.Client{Timeout: 2 * time.Minute},
	}, nil
}

func (client *EmbeddingClient) Embed(ctx context.Context, inputs []string) ([][]float32, error) {
	result := make([][]float32, 0, len(inputs))
	for start := 0; start < len(inputs); {
		end, err := embeddingBatchEnd(inputs, start)
		if err != nil {
			return nil, err
		}
		batch, err := client.embedBatch(ctx, inputs[start:end])
		if err != nil {
			return nil, err
		}
		result = append(result, batch...)
		start = end
	}
	return result, nil
}

func embeddingBatchEnd(inputs []string, start int) (int, error) {
	bytes := 0
	end := start
	for end < len(inputs) && end-start < EMBEDDING_BATCH_SIZE {
		inputBytes := len(inputs[end])
		if inputBytes == 0 {
			return 0, fmt.Errorf("embedding input cannot be empty")
		}
		if inputBytes > EMBEDDING_MAX_INPUT_BYTES {
			return 0, fmt.Errorf("embedding input exceeds %d-byte safety limit", EMBEDDING_MAX_INPUT_BYTES)
		}
		if end > start && bytes+inputBytes > EMBEDDING_BATCH_MAX_INPUT_BYTES {
			break
		}
		bytes += inputBytes
		end++
	}
	return end, nil
}

func (client *EmbeddingClient) embedBatch(ctx context.Context, inputs []string) ([][]float32, error) {
	body, err := json.Marshal(embeddingRequest{Model: client.model, Input: inputs, Dimensions: client.dimensions})
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://api.openai.com/v1/embeddings", bytes.NewReader(body))
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
		return nil, err
	}
	if len(payload.Data) != len(inputs) {
		return nil, fmt.Errorf("embedding response returned %d vectors for %d inputs", len(payload.Data), len(inputs))
	}
	vectors := make([][]float32, len(inputs))
	for _, item := range payload.Data {
		if item.Index < 0 || item.Index >= len(vectors) || len(item.Embedding) != client.dimensions {
			return nil, fmt.Errorf("invalid embedding response item")
		}
		vectors[item.Index] = item.Embedding
	}
	LogInfo(client.config, fmt.Sprintf("Built embeddings via OpenAI | Input tokens: %d", payload.Usage.PromptTokens))
	return vectors, nil
}
