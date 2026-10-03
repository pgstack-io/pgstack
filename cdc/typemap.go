package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"strconv"

	"github.com/jackc/pgx/v5"
)

type TypeMap struct {
	types map[uint32]string
}

func LoadTypeMap(ctx context.Context, databaseURL string) (*TypeMap, error) {
	conn, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		return nil, fmt.Errorf("failed to connect: %w", err)
	}
	defer conn.Close(ctx)

	rows, err := conn.Query(ctx, "SELECT oid, typname FROM pg_type")
	if err != nil {
		return nil, fmt.Errorf("failed to query types: %w", err)
	}
	defer rows.Close()

	types := make(map[uint32]string)
	for rows.Next() {
		var oid uint32
		var typname string
		if err := rows.Scan(&oid, &typname); err != nil {
			return nil, err
		}
		types[oid] = typname
	}

	return &TypeMap{types: types}, nil
}

func (tm *TypeMap) DecodeValue(dataType uint32, data []byte) interface{} {
	if data == nil {
		return nil
	}

	typeName, ok := tm.types[dataType]
	if !ok {
		return string(data)
	}

	switch typeName {
	case "bool":
		switch string(data) {
		case "t", "true":
			return true
		case "f", "false":
			return false
		}
		return data[0] == 1

	case "int2":
		if value, err := strconv.ParseInt(string(data), 10, 16); err == nil {
			return int16(value)
		}

	case "int4":
		if value, err := strconv.ParseInt(string(data), 10, 32); err == nil {
			return int32(value)
		}

	case "int8":
		if value, err := strconv.ParseInt(string(data), 10, 64); err == nil {
			return value
		}

	case "float4":
		if value, err := strconv.ParseFloat(string(data), 32); err == nil {
			if math.IsInf(value, 0) || math.IsNaN(value) {
				return string(data)
			}
			return float32(value)
		}
		if len(data) == 4 {
			bits := binary.BigEndian.Uint32(data)
			value := math.Float32frombits(bits)
			if math.IsInf(float64(value), 0) || math.IsNaN(float64(value)) {
				return strconv.FormatFloat(float64(value), 'g', -1, 32)
			}
			return value
		}

	case "float8":
		if value, err := strconv.ParseFloat(string(data), 64); err == nil {
			if math.IsInf(value, 0) || math.IsNaN(value) {
				return string(data)
			}
			return value
		}
		if len(data) == 8 {
			bits := binary.BigEndian.Uint64(data)
			value := math.Float64frombits(bits)
			if math.IsInf(value, 0) || math.IsNaN(value) {
				return strconv.FormatFloat(value, 'g', -1, 64)
			}
			return value
		}

	case "timestamp", "timestamptz":
		return string(data)

	case "date":
		return string(data)

	case "json", "jsonb":
		if typeName == "jsonb" && len(data) > 0 && data[0] == 1 {
			data = data[1:]
		}
		var result interface{}
		if err := json.Unmarshal(data, &result); err == nil {
			return result
		}
		return string(data)

	case "uuid":
		if len(data) == 16 {
			return fmt.Sprintf("%x-%x-%x-%x-%x",
				data[0:4], data[4:6], data[6:8], data[8:10], data[10:16])
		}

	case "bytea":
		return fmt.Sprintf("\\x%x", data)

	default:
		return string(data)
	}

	return string(data)
}
