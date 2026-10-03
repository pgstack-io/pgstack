package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type fakeJetStreamMsg struct {
	subject        string
	streamSequence uint64
	acked          bool
	inProgress     bool
}

func (m *fakeJetStreamMsg) Metadata() (*jetstream.MsgMetadata, error) {
	return &jetstream.MsgMetadata{
		Sequence: jetstream.SequencePair{Stream: m.streamSequence},
	}, nil
}

func (m *fakeJetStreamMsg) Data() []byte {
	return nil
}

func (m *fakeJetStreamMsg) Headers() nats.Header {
	return nil
}

func (m *fakeJetStreamMsg) Subject() string {
	return m.subject
}

func (m *fakeJetStreamMsg) Reply() string {
	return ""
}

func (m *fakeJetStreamMsg) Ack() error {
	m.acked = true
	return nil
}

func (m *fakeJetStreamMsg) DoubleAck(context.Context) error {
	return m.Ack()
}

func (m *fakeJetStreamMsg) Nak() error {
	return nil
}

func (m *fakeJetStreamMsg) NakWithDelay(time.Duration) error {
	return nil
}

func (m *fakeJetStreamMsg) InProgress() error {
	m.inProgress = true
	return nil
}

func (m *fakeJetStreamMsg) Term() error {
	return nil
}

func (m *fakeJetStreamMsg) TermWithReason(string) error {
	return nil
}

func TestStitchMessagesBuffersMutationUntilContextArrives(t *testing.T) {
	mutationMsg := &fakeJetStreamMsg{subject: "cdc.project.pipeline", streamSequence: 10}
	buffer := NewMessageBuffer()

	firstResult := StitchMessages(quietTestConfig(), &MessageBatch{
		Messages: []*ChangeMessage{changeMessage(1, OPERATION_CREATE)},
		NatsMsg:  []jetstream.Msg{mutationMsg},
	}, buffer)

	if len(firstResult.StitchedRecords) != 0 {
		t.Fatalf("expected no stitched records, got %d", len(firstResult.StitchedRecords))
	}
	if firstResult.Buffer.Size() != 1 {
		t.Fatalf("expected one buffered record, got %d", firstResult.Buffer.Size())
	}
	if len(firstResult.AckMessages) != 0 {
		t.Fatalf("expected no ackable messages, got %d", len(firstResult.AckMessages))
	}
	if !mutationMsg.inProgress {
		t.Fatal("expected buffered message to be marked in progress")
	}
}

func TestStitchMessagesStitchesBufferedMutationWhenContextArrives(t *testing.T) {
	subject := "cdc.project.pipeline"
	mutationMsg := &fakeJetStreamMsg{subject: subject, streamSequence: 10}
	contextMsg := &fakeJetStreamMsg{subject: subject, streamSequence: 11}
	buffer := NewMessageBuffer()

	firstResult := StitchMessages(quietTestConfig(), &MessageBatch{
		Messages: []*ChangeMessage{changeMessage(1, OPERATION_CREATE)},
		NatsMsg:  []jetstream.Msg{mutationMsg},
	}, buffer)

	secondResult := StitchMessages(quietTestConfig(), &MessageBatch{
		Messages: []*ChangeMessage{contextMessage(1, `{"user_id":"user_123"}`)},
		NatsMsg:  []jetstream.Msg{contextMsg},
	}, firstResult.Buffer)

	if secondResult.Buffer.Size() != 0 {
		t.Fatalf("expected empty buffer, got %d", secondResult.Buffer.Size())
	}
	if len(secondResult.StitchedRecords) != 1 {
		t.Fatalf("expected one stitched record, got %d", len(secondResult.StitchedRecords))
	}

	context := secondResult.StitchedRecords[0].Message.Context
	if context["user_id"] != "user_123" {
		t.Fatalf("expected stitched context user_id, got %#v", context)
	}
	if len(secondResult.AckMessages) != 2 {
		t.Fatalf("expected mutation and context messages to be ackable, got %d", len(secondResult.AckMessages))
	}
	if secondResult.AckStreamSequence != 11 {
		t.Fatalf("expected max ack stream sequence 11, got %d", secondResult.AckStreamSequence)
	}
}

func TestStitchMessagesReleasesBufferedMutationWithoutContextOnNextBatch(t *testing.T) {
	subject := "cdc.project.pipeline"
	firstMutationMsg := &fakeJetStreamMsg{subject: subject, streamSequence: 10}
	secondMutationMsg := &fakeJetStreamMsg{subject: subject, streamSequence: 11}
	buffer := NewMessageBuffer()

	firstResult := StitchMessages(quietTestConfig(), &MessageBatch{
		Messages: []*ChangeMessage{changeMessage(1, OPERATION_CREATE)},
		NatsMsg:  []jetstream.Msg{firstMutationMsg},
	}, buffer)

	secondResult := StitchMessages(quietTestConfig(), &MessageBatch{
		Messages: []*ChangeMessage{changeMessage(2, OPERATION_CREATE)},
		NatsMsg:  []jetstream.Msg{secondMutationMsg},
	}, firstResult.Buffer)

	if secondResult.Buffer.Size() != 1 {
		t.Fatalf("expected newest unpaired mutation to remain buffered, got %d", secondResult.Buffer.Size())
	}
	if len(secondResult.StitchedRecords) != 1 {
		t.Fatalf("expected older mutation to be released, got %d stitched records", len(secondResult.StitchedRecords))
	}
	if secondResult.StitchedRecords[0].Message.Context != nil {
		t.Fatalf("expected released mutation to have no context, got %#v", secondResult.StitchedRecords[0].Message.Context)
	}
	if len(secondResult.AckMessages) != 1 {
		t.Fatalf("expected only released mutation to be ackable, got %d", len(secondResult.AckMessages))
	}
	if secondResult.AckStreamSequence != 10 {
		t.Fatalf("expected max ack stream sequence 10, got %d", secondResult.AckStreamSequence)
	}
	if !secondMutationMsg.inProgress {
		t.Fatal("expected newest buffered mutation to be marked in progress")
	}
}

func changeMessage(txId uint32, op string) *ChangeMessage {
	return &ChangeMessage{
		After: map[string]interface{}{"id": 1},
		Op:    op,
		Source: SourceMetadata{
			TxId: txId,
		},
	}
}

func TestFormatPrimaryKeyValuePreservesJSONNumber(t *testing.T) {
	got := formatPrimaryKeyValue(json.Number("842545207"))
	if got != "842545207" {
		t.Fatalf("expected integer primary key without exponent, got %q", got)
	}
}

func TestFormatPrimaryKeyValueHandlesMixedTypes(t *testing.T) {
	tests := []struct {
		name  string
		value interface{}
		want  string
	}{
		{name: "string", value: "abc-123", want: "abc-123"},
		{name: "uuid", value: "4aa8716a-d66f-4d74-8cb2-bef3bf08957b", want: "4aa8716a-d66f-4d74-8cb2-bef3bf08957b"},
		{name: "float64", value: float64(842545207), want: "842545207"},
		{name: "bool", value: true, want: "true"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := formatPrimaryKeyValue(tt.value); got != tt.want {
				t.Fatalf("expected %q, got %q", tt.want, got)
			}
		})
	}
}

func TestMessageToRowPreservesCompoundPrimaryKeyValues(t *testing.T) {
	writer := &IcebergWriter{}
	msg := &ChangeMessage{
		After: map[string]interface{}{
			"id":      json.Number("842545207"),
			"account": "4aa8716a-d66f-4d74-8cb2-bef3bf08957b",
		},
		Op: OPERATION_CREATE,
		Source: SourceMetadata{
			TsUs: time.Now().UnixMicro(),
			Pk:   []string{"id", "account"},
		},
		TsUs: time.Now().UnixMicro(),
	}

	row := writer.messageToRow(msg)
	if !row.PrimaryKey.Valid {
		t.Fatal("expected primary key to be set")
	}

	want := `["842545207","4aa8716a-d66f-4d74-8cb2-bef3bf08957b"]`
	if row.PrimaryKey.String != want {
		t.Fatalf("expected compound primary key %s, got %s", want, row.PrimaryKey.String)
	}
}

func contextMessage(txId uint32, content string) *ChangeMessage {
	return &ChangeMessage{
		Op: OPERATION_MESSAGE,
		Source: SourceMetadata{
			TxId: txId,
		},
		Message: &MessagePayload{
			Prefix:  MESSAGE_PREFIX_CONTEXT,
			Content: base64.StdEncoding.EncodeToString([]byte(content)),
		},
	}
}
