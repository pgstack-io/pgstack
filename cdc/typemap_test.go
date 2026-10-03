package main

import (
	"encoding/json"
	"testing"
)

func TestDecodeValueParsesTextInt4(t *testing.T) {
	typeMap := &TypeMap{types: map[uint32]string{23: "int4"}}

	got := typeMap.DecodeValue(23, []byte("6889"))

	if got != int32(6889) {
		t.Fatalf("expected text int4 to decode as 6889, got %#v", got)
	}
}

func TestDecodeValueParsesTextInt8(t *testing.T) {
	typeMap := &TypeMap{types: map[uint32]string{20: "int8"}}

	got := typeMap.DecodeValue(20, []byte("2579335"))

	if got != int64(2579335) {
		t.Fatalf("expected text int8 to decode as 2579335, got %#v", got)
	}
}

func TestDecodeValueParsesTextFloat4BeforeBinaryFallback(t *testing.T) {
	typeMap := &TypeMap{types: map[uint32]string{700: "float4"}}

	got := typeMap.DecodeValue(700, []byte("1.25"))

	if got != float32(1.25) {
		t.Fatalf("expected text float4 to decode as 1.25, got %#v", got)
	}
}

func TestDecodeValuePreservesNonFiniteTextFloatsAsStrings(t *testing.T) {
	typeMap := &TypeMap{types: map[uint32]string{
		700: "float4",
		701: "float8",
	}}

	tests := []struct {
		name     string
		dataType uint32
		value    string
	}{
		{name: "float4 infinity", dataType: 700, value: "Infinity"},
		{name: "float4 negative infinity", dataType: 700, value: "-Infinity"},
		{name: "float4 NaN", dataType: 700, value: "NaN"},
		{name: "float8 infinity", dataType: 701, value: "Infinity"},
		{name: "float8 negative infinity", dataType: 701, value: "-Infinity"},
		{name: "float8 NaN", dataType: 701, value: "NaN"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := typeMap.DecodeValue(tt.dataType, []byte(tt.value))

			if got != tt.value {
				t.Fatalf("expected %q to remain a string, got %#v", tt.value, got)
			}
			if _, err := json.Marshal(got); err != nil {
				t.Fatalf("expected decoded value to be JSON-compatible: %v", err)
			}
		})
	}
}
