package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	PRODUCT_AUDIT  = "audit"
	PRODUCT_SEARCH = "search"
)

type NatsPublisher struct {
	nc      *nats.Conn
	js      jetstream.JetStream
	subject string
	enabled bool
}

func NewNatsPublisher(config *Config) (*NatsPublisher, error) {
	if config.NatsURL == "" || config.NatsSubject == "" {
		LogInfo(config, "NATS URL or subject not configured, publishing disabled")
		return &NatsPublisher{enabled: false}, nil
	}

	nc, err := nats.Connect(config.NatsURL)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to NATS: %w", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("failed to create JetStream publisher: %w", err)
	}

	LogInfo(config, fmt.Sprintf("Connected to NATS at %s, publishing to subject: %s", config.NatsURL, config.NatsSubject))
	return &NatsPublisher{
		nc:      nc,
		js:      js,
		subject: config.NatsSubject,
		enabled: true,
	}, nil
}

func (np *NatsPublisher) PublishChange(change *ChangeMessage) error {
	if !np.enabled {
		return nil
	}

	data, err := json.Marshal(change)
	if err != nil {
		return fmt.Errorf("failed to marshal change: %w", err)
	}

	if _, err := np.js.Publish(context.Background(), np.subject, data); err != nil {
		return fmt.Errorf("failed to commit message to NATS JetStream: %w", err)
	}

	return nil
}

func (np *NatsPublisher) Flush() error {
	if !np.enabled || np.nc == nil {
		return nil
	}

	if err := np.nc.Flush(); err != nil {
		return fmt.Errorf("failed to flush NATS connection: %w", err)
	}

	return nil
}

func (np *NatsPublisher) Close() {
	if np.enabled && np.nc != nil {
		np.nc.Close()
	}
}

type ChangeMessage struct {
	Before   map[string]interface{} `json:"before,omitempty"`
	After    map[string]interface{} `json:"after,omitempty"`
	Source   SourceMetadata         `json:"source"`
	Op       string                 `json:"op"`
	TsUs     int64                  `json:"ts_us"`
	Message  *MessagePayload        `json:"message,omitempty"`
	Products []string               `json:"products,omitempty"`
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

func CreateChangeMessage(change Change, txn *Transaction, lsn uint64, dbName string, primaryKey []string) *ChangeMessage {
	now := time.Now()
	tsUs := now.UnixMicro()

	msg := &ChangeMessage{
		Before: change.Before,
		After:  change.After,
		Op:     getOpCode(change.Operation),
		TsUs:   tsUs,
		Source: SourceMetadata{
			TsUs:   txn.CommitTime.UnixMicro(),
			Db:     dbName,
			Schema: change.Schema,
			Table:  change.Table,
			TxId:   txn.XID,
			Lsn:    lsn,
			Pk:     primaryKey,
		},
	}

	return msg
}

func CreateLogicalMessage(logicalMsg LogicalMessage, txn *Transaction, lsn uint64, dbName string) *ChangeMessage {
	now := time.Now()
	tsUs := now.UnixMicro()
	content := normalizeLogicalMessageContent(logicalMsg.Content)

	msg := &ChangeMessage{
		Op:   OPERATION_MESSAGE,
		TsUs: tsUs,
		Source: SourceMetadata{
			TsUs:   txn.CommitTime.UnixMicro(),
			Db:     dbName,
			Schema: "",
			Table:  "",
			TxId:   txn.XID,
			Lsn:    lsn,
		},
		Message: &MessagePayload{
			Prefix:  logicalMsg.Prefix,
			Content: base64.StdEncoding.EncodeToString(content),
		},
	}

	return msg
}

func normalizeLogicalMessageContent(rawContent []byte) []byte {
	content := bytes.TrimSpace(rawContent)
	if json.Valid(content) {
		var value interface{}
		if err := json.Unmarshal(content, &value); err != nil {
			return content
		}

		cleaned, err := json.Marshal(removeNullBytes(value))
		if err != nil {
			return content
		}

		return cleaned
	}

	return bytes.ReplaceAll(content, []byte{0}, nil)
}

func removeNullBytes(value interface{}) interface{} {
	switch v := value.(type) {
	case nil:
		return nil
	case string:
		return string(bytes.ReplaceAll([]byte(v), []byte{0}, nil))
	case []interface{}:
		cleaned := make([]interface{}, len(v))
		for i, item := range v {
			cleaned[i] = removeNullBytes(item)
		}
		return cleaned
	case map[string]interface{}:
		cleaned := make(map[string]interface{}, len(v))
		for key, item := range v {
			cleaned[key] = removeNullBytes(item)
		}
		return cleaned
	default:
		return value
	}
}
