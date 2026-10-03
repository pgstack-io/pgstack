package main

import (
	"reflect"
	"strings"
	"testing"

	"github.com/jackc/pglogrepl"
)

func TestShouldIgnoreLogicalMessage(t *testing.T) {
	handler := &ReplicationHandler{config: &Config{LogLevel: LOG_LEVEL_ERROR}}

	tests := []struct {
		name    string
		prefix  string
		content string
		want    bool
	}{
		{name: "small context", prefix: MESSAGE_PREFIX_CONTEXT, content: "{}", want: false},
		{name: "large context", prefix: MESSAGE_PREFIX_CONTEXT, content: strings.Repeat("a", MAX_LOGICAL_MESSAGE_SIZE_BYTES), want: true},
		{name: "non-context", prefix: "other", content: "{}", want: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logicalMessage := LogicalMessage{Prefix: tt.prefix, Content: []byte(tt.content)}
			if got := handler.shouldIgnoreLogicalMessage(logicalMessage); got != tt.want {
				t.Fatalf("shouldIgnoreLogicalMessage() = %t, want %t", got, tt.want)
			}
		})
	}
}

func TestResolveUnchangedToastFromBeforeCopiesFromFullOldTuple(t *testing.T) {
	before := map[string]interface{}{
		"id":      int64(1),
		"payload": "old toast value",
		"name":    "before",
	}
	after := map[string]interface{}{
		"id":      int64(1),
		"payload": UNCHANGED_TOAST,
		"name":    "after",
	}

	resolveUnchangedToastFromBefore(pglogrepl.UpdateMessageTupleTypeOld, before, after)

	want := map[string]interface{}{
		"id":      int64(1),
		"payload": "old toast value",
		"name":    "after",
	}
	if !reflect.DeepEqual(after, want) {
		t.Fatalf("resolved after mismatch:\nwant: %#v\ngot:  %#v", want, after)
	}
}

func TestResolveUnchangedToastFromBeforeRequiresFullOldTuple(t *testing.T) {
	before := map[string]interface{}{
		"payload": "old toast value",
	}
	after := map[string]interface{}{
		"payload": UNCHANGED_TOAST,
	}

	resolveUnchangedToastFromBefore(pglogrepl.UpdateMessageTupleTypeKey, before, after)

	if after["payload"] != UNCHANGED_TOAST {
		t.Fatalf("expected toast marker to remain without full old tuple, got %#v", after["payload"])
	}
}

func TestResolveUnchangedToastFromBeforeLeavesUnknownValues(t *testing.T) {
	tests := []struct {
		name   string
		before map[string]interface{}
		after  map[string]interface{}
	}{
		{
			name:   "missing before column",
			before: map[string]interface{}{},
			after: map[string]interface{}{
				"payload": UNCHANGED_TOAST,
			},
		},
		{
			name: "unchanged toast before value",
			before: map[string]interface{}{
				"payload": UNCHANGED_TOAST,
			},
			after: map[string]interface{}{
				"payload": UNCHANGED_TOAST,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resolveUnchangedToastFromBefore(pglogrepl.UpdateMessageTupleTypeOld, tt.before, tt.after)

			if tt.after["payload"] != UNCHANGED_TOAST {
				t.Fatalf("expected unresolved toast marker, got %#v", tt.after["payload"])
			}
		})
	}
}
