package main

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/nats-io/nats.go/jetstream"
)

const (
	MESSAGE_PREFIX_CONTEXT = "_bemi"
)

type FetchedRecord struct {
	Message        *ChangeMessage
	NatsMsg        jetstream.Msg
	StreamSequence uint64
	Subject        string
	MessagePrefix  string
	IsContextMsg   bool
	IsMutation     bool
}

type MessageBuffer struct {
	// Map: subject -> transactionId -> list of FetchedRecords
	store map[string]map[uint32][]*FetchedRecord
}

type StitchResult struct {
	StitchedRecords   []*FetchedRecord
	Buffer            *MessageBuffer
	AckMessages       []jetstream.Msg
	AckStreamSequence uint64
}

func NewMessageBuffer() *MessageBuffer {
	return &MessageBuffer{
		store: make(map[string]map[uint32][]*FetchedRecord),
	}
}

func (mb *MessageBuffer) Add(record *FetchedRecord) {
	subject := record.Subject
	txId := record.Message.Source.TxId

	if mb.store[subject] == nil {
		mb.store[subject] = make(map[uint32][]*FetchedRecord)
	}

	mb.store[subject][txId] = append(mb.store[subject][txId], record)
}

func (mb *MessageBuffer) GetByTransaction(subject string, txId uint32) []*FetchedRecord {
	if mb.store[subject] == nil {
		return nil
	}
	return mb.store[subject][txId]
}

func (mb *MessageBuffer) Size() int {
	total := 0
	for _, txMap := range mb.store {
		for _, records := range txMap {
			total += len(records)
		}
	}
	return total
}

// ForEach iterates over all records grouped by subject, sorted by transactionId and streamSequence.
func (mb *MessageBuffer) ForEach(callback func(subject string, records []*FetchedRecord)) {
	for subject, txMap := range mb.store {
		var allRecords []*FetchedRecord
		for _, records := range txMap {
			allRecords = append(allRecords, records...)
		}

		sort.Slice(allRecords, func(i, j int) bool {
			if allRecords[i].Message.Source.TxId != allRecords[j].Message.Source.TxId {
				return allRecords[i].Message.Source.TxId < allRecords[j].Message.Source.TxId
			}
			return allRecords[i].StreamSequence < allRecords[j].StreamSequence
		})

		callback(subject, allRecords)
	}
}

func NewFetchedRecord(config *Config, msg *ChangeMessage, natsMsg jetstream.Msg, subject string) *FetchedRecord {
	record := &FetchedRecord{
		Message: msg,
		NatsMsg: natsMsg,
		Subject: subject,
	}

	// Get metadata
	metadata, err := natsMsg.Metadata()
	if err != nil {
		LogError(config, "Failed to get message metadata:", err)
		return record
	}
	record.StreamSequence = metadata.Sequence.Stream

	// Determine message type
	if msg.Message != nil {
		record.MessagePrefix = msg.Message.Prefix
		if msg.Message.Prefix == MESSAGE_PREFIX_CONTEXT {
			record.IsContextMsg = true
		}
	}

	// Check if it's a mutation (c/u/d/t operations create data changes)
	if msg.Op == OPERATION_CREATE || msg.Op == OPERATION_UPDATE || msg.Op == OPERATION_DELETE || msg.Op == OPERATION_TRUNCATE {
		record.IsMutation = true
	}

	return record
}

// ParseContext extracts context from a context message
func ParseContext(config *Config, msg *ChangeMessage) map[string]interface{} {
	if msg.Message == nil || msg.Message.Prefix != MESSAGE_PREFIX_CONTEXT {
		return nil
	}

	content := []byte(msg.Message.Content)
	decoded, err := base64.StdEncoding.DecodeString(msg.Message.Content)
	if err == nil {
		content = decoded
	}

	var context map[string]interface{}
	if err := json.Unmarshal(content, &context); err != nil {
		LogWarn(config, fmt.Sprintf(
			"Failed to parse context JSON: %v (content_bytes=%d, base64_decoded=%t)",
			err,
			len(content),
			decoded != nil,
		))
		return nil
	}

	return context
}

// StitchMessages processes a batch of messages with context stitching and buffering
func StitchMessages(config *Config, batch *MessageBatch, buffer *MessageBuffer) *StitchResult {
	// Convert batch to FetchedRecords and add to buffer
	for i, msg := range batch.Messages {
		natsMsg := batch.NatsMsg[i]
		record := NewFetchedRecord(config, msg, natsMsg, natsMsg.Subject())
		buffer.Add(record)
	}

	stitched := []*FetchedRecord{}
	newBuffer := NewMessageBuffer()
	ackMessages := []jetstream.Msg{}
	ackedSequences := make(map[uint64]bool)
	maxAckSequence := uint64(0)

	addAck := func(record *FetchedRecord) {
		if record == nil || record.NatsMsg == nil || ackedSequences[record.StreamSequence] {
			return
		}
		ackedSequences[record.StreamSequence] = true
		ackMessages = append(ackMessages, record.NatsMsg)
		if record.StreamSequence > maxAckSequence {
			maxAckSequence = record.StreamSequence
		}
	}

	buffer.ForEach(func(subject string, records []*FetchedRecord) {
		if len(records) == 0 {
			return
		}

		for idx, record := range records {
			txId := record.Message.Source.TxId
			sameTransactionRecords := buffer.GetByTransaction(subject, txId)
			var contextRecord *FetchedRecord

			for _, r := range sameTransactionRecords {
				if r.IsContextMsg {
					contextRecord = r
					break
				}
			}

			// Keep only the current subject tail in the buffer. If its pair does
			// not arrive in the next non-empty batch, a later record will push it
			// out and it will be processed/acked without context.
			isLastMessage := idx == len(records)-1
			isUnpairedMutation := record.IsMutation && contextRecord == nil
			isUnpairedContext := record.IsContextMsg

			if isLastMessage && (isUnpairedMutation || isUnpairedContext) && len(sameTransactionRecords) == 1 {
				newBuffer.Add(record)
				if record.NatsMsg != nil {
					if err := record.NatsMsg.InProgress(); err != nil {
						LogError(config, "Failed to mark buffered message in progress:", err)
					}
				}
				continue
			}

			if record.IsContextMsg {
				addAck(record)
				continue
			}

			if record.IsMutation {
				if contextRecord != nil {
					record.Message.Context = ParseContext(config, contextRecord.Message)
					addAck(contextRecord)
				}
				stitched = append(stitched, record)
				addAck(record)
				continue
			}

			// Non-mutation messages are intentionally ignored, but still acked.
			addAck(record)
		}
	})

	LogDebug(config, fmt.Sprintf(
		"Stitching: fetched=%d, stitched=%d, buffered=%d, ack_count=%d, max_ack_seq=%d",
		len(batch.Messages),
		len(stitched),
		newBuffer.Size(),
		len(ackMessages),
		maxAckSequence,
	))

	return &StitchResult{
		StitchedRecords:   stitched,
		Buffer:            newBuffer,
		AckMessages:       ackMessages,
		AckStreamSequence: maxAckSequence,
	}
}
