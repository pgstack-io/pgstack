package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	MAX_NATS_BATCH_MESSAGES = 5000
	PRODUCT_AUDIT           = "audit"
	PRODUCT_SEARCH          = "search"
)

type ChangeMessage struct {
	Before   map[string]interface{} `json:"before,omitempty"`
	After    map[string]interface{} `json:"after,omitempty"`
	Source   SourceMetadata         `json:"source"`
	Op       string                 `json:"op"`
	TsUs     int64                  `json:"ts_us"`
	Message  *MessagePayload        `json:"message,omitempty"`
	Context  map[string]interface{} `json:"-"` // Populated during stitching
	Products []string               `json:"products,omitempty"`
}

func (message *ChangeMessage) IsForProduct(product string) bool {
	// Fallback to audit product, legacy messages may not have products configured
	if len(message.Products) == 0 {
		return product == PRODUCT_AUDIT
	}
	for _, configured := range message.Products {
		if configured == product {
			return true
		}
	}
	return false
}

type MessagePayload struct {
	Prefix  string `json:"prefix"`
	Content string `json:"content"`
}

type SourceMetadata struct {
	TsUs   int64    `json:"ts_us"`
	Db     string   `json:"db"`
	Schema string   `json:"schema"`
	Table  string   `json:"table"`
	TxId   uint32   `json:"txId"`
	Lsn    uint64   `json:"lsn"`
	Pk     []string `json:"pk,omitempty"`
}

type NatsConsumer struct {
	nc       *nats.Conn
	js       jetstream.JetStream
	consumer jetstream.Consumer
	config   *Config
}

type MessageBatch struct {
	Messages []*ChangeMessage
	NatsMsg  []jetstream.Msg
}

func NewNatsConsumer(config *Config) (*NatsConsumer, error) {
	nc, err := nats.Connect(config.NatsURL)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to NATS: %w", err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("failed to create JetStream context: %w", err)
	}

	// Use project-specific consumer name so multiple processor instances
	// for different projects don't conflict
	consumerName := config.ConsumerName

	consumer, err := js.CreateOrUpdateConsumer(context.Background(), config.NatsStreamName, jetstream.ConsumerConfig{
		// Durable consumer name - persists across restarts
		// Format: [projectId]-processor (e.g., "proj_abc123-processor")
		Durable: consumerName,

		// Only consume messages matching this subject pattern
		FilterSubject: config.NatsSubject,

		// MaxAckPending: -1 means unlimited unacknowledged messages
		// Default is 1000, but we batch process so we want flexibility
		// The consumer won't stop delivering if we have many in-flight
		MaxAckPending: -1,

		// AckExplicitPolicy: Messages must be explicitly acknowledged
		// We ack after successfully writing to Parquet/Iceberg
		// This ensures at-least-once delivery semantics
		AckPolicy: jetstream.AckExplicitPolicy,

		// DeliverAllPolicy: Start from the beginning of the stream
		// On first connection, deliver all messages (not just new ones)
		// Good for initial sync or recovery scenarios
		DeliverPolicy: jetstream.DeliverAllPolicy,

		// AckWait needs to be longer than the fetch window plus write time.
		// Otherwise messages fetched early in the batch can redeliver before
		// the processor reaches its post-write ack path.
		AckWait: config.NatsBatchInterval * 2,

		// MaxDeliver: -1 means infinite redelivery attempts
		// If we fail to process a message, NATS will keep trying forever
		// Better than dropping messages; we can add DLQ later if needed
		MaxDeliver: -1,
	})
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("failed to create consumer: %w", err)
	}

	LogInfo(config, fmt.Sprintf(
		"Connected to NATS stream: %s, subject: %s, consumer: %s",
		config.NatsStreamName,
		config.NatsSubject,
		consumerName,
	))

	return &NatsConsumer{
		nc:       nc,
		js:       js,
		consumer: consumer,
		config:   config,
	}, nil
}

func (nc *NatsConsumer) ConsumeBatch(ctx context.Context, timeout time.Duration) (*MessageBatch, error) {
	messages := []*ChangeMessage{}
	natsMessages := []jetstream.Msg{}

	msgs, err := nc.consumer.Fetch(MAX_NATS_BATCH_MESSAGES, jetstream.FetchMaxWait(timeout))
	if err != nil {
		return &MessageBatch{
			Messages: messages,
			NatsMsg:  natsMessages,
		}, nil
	}

	for msg := range msgs.Messages() {
		var change ChangeMessage
		decoder := json.NewDecoder(bytes.NewReader(msg.Data()))
		decoder.UseNumber()
		if err := decoder.Decode(&change); err != nil {
			LogError(nc.config, "Failed to unmarshal message:", err)
			msg.Ack() // Ack bad messages immediately to avoid reprocessing
			continue
		}

		messages = append(messages, &change)
		natsMessages = append(natsMessages, msg) // Store for later ack
	}

	if msgs.Error() != nil {
		LogError(nc.config, "Error fetching messages:", msgs.Error())
	}

	return &MessageBatch{
		Messages: messages,
		NatsMsg:  natsMessages,
	}, nil
}

func (nc *NatsConsumer) Close() {
	if nc.nc != nil {
		nc.nc.Close()
	}
}
