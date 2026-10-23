package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"math"
	"math/big"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"
)

func TestBinaryBindRoundTripsByPostgresType(t *testing.T) {
	handler := initQueryHandlerForTest(t)
	types := pgtype.NewMap()
	stamp := time.Date(2024, 2, 29, 12, 34, 56, 123456000, time.UTC)
	number := new(big.Int)
	number.SetString("9007199254740993123456789", 10)
	for _, test := range []struct {
		name, query string
		oid         uint32
		value       interface{}
		want        string
	}{
		{"float4", "SELECT $1::real", pgtype.Float4OID, float32(1.5), "1.5"},
		{"float8", "SELECT $1::double precision", pgtype.Float8OID, float64(-123.125), "-123.125"},
		{"int2", "SELECT $1::smallint", pgtype.Int2OID, int16(-32768), "-32768"},
		{"int4", "SELECT $1::integer", pgtype.Int4OID, int32(-2147483648), "-2147483648"},
		{"int8", "SELECT $1::bigint", pgtype.Int8OID, int64(-9223372036854775808), "-9223372036854775808"},
		{"bool", "SELECT $1::boolean", pgtype.BoolOID, true, "t"},
		{"text", "SELECT $1::text", pgtype.TextOID, "four", "four"},
		{"bytea", "SELECT hex($1::bytea)", pgtype.ByteaOID, []byte{0, 1, 2, 255}, "000102FF"},
		{"uuid", "SELECT ($1::uuid)::text", pgtype.UUIDOID, pgtype.UUID{Bytes: [16]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}, Valid: true}, "00010203-0405-0607-0809-0a0b0c0d0e0f"},
		{"date", "SELECT $1::date", pgtype.DateOID, pgtype.Date{Time: stamp, Valid: true}, "2024-02-29"},
		{"timestamp", "SELECT $1::timestamp", pgtype.TimestampOID, pgtype.Timestamp{Time: stamp, Valid: true}, "2024-02-29 12:34:56.123456"},
		{"timestamptz", "SELECT $1::timestamptz", pgtype.TimestamptzOID, pgtype.Timestamptz{Time: stamp, Valid: true}, "2024-02-29 12:34:56.123456+00:00"},
		{"numeric", "SELECT ($1::numeric(30,5))::text", pgtype.NumericOID, pgtype.Numeric{Int: number, Exp: -5, Valid: true}, "90071992547409931234.56789"},
		{"jsonb", "SELECT ($1::json)::text", pgtype.JSONBOID, json.RawMessage(`{"amount":9007199254740993}`), `{"amount":9007199254740993}`},
		{"int-array", "SELECT list_sum($1::integer[])", pgtype.Int4ArrayOID, []int32{1, -2, 3}, "2"},
		{"nullable-text-array", "SELECT ($1::text[])[2] IS NULL AND ($1::text[])[1] = 'first' AND ($1::text[])[3] = 'sec,ond'", pgtype.TextArrayOID, []pgtype.Text{{String: "first", Valid: true}, {}, {String: "sec,ond", Valid: true}}, "t"},
		{"escaped-text-array", "SELECT ($1::text[])[1]", pgtype.TextArrayOID, []string{"a'b\\c, [NULL]"}, "a'b\\c, [NULL]"},
		{"empty-array", "SELECT len($1::integer[])", pgtype.Int4ArrayOID, []int32{}, "0"},
		{"cast-array", "SELECT list_sum($1::smallint[])", pgtype.Int4ArrayOID, []int32{1, 2}, "3"},
		{"json-array", "SELECT ($1::text[])[1]", pgtype.JSONBArrayOID, []json.RawMessage{json.RawMessage(`{"amount":9007199254740993}`)}, `{"amount":9007199254740993}`},
		{"numeric-array", "SELECT ($1::numeric(30,5)[])[1]::text", pgtype.NumericArrayOID, []pgtype.Numeric{{Int: number, Exp: -5, Valid: true}}, "90071992547409931234.56789"},
		{"date-array", "SELECT ($1::date[])[1]::text", pgtype.DateArrayOID, []pgtype.Date{{Time: stamp, Valid: true}}, "2024-02-29"},
		{"bytea-array", "SELECT hex(($1::bytea[])[1])", pgtype.ByteaArrayOID, [][]byte{{0, 1, 2, 255}}, "000102FF"},
		{"matrix", "SELECT ($1::double precision[][])[2][1]", pgtype.Float8ArrayOID, pgtype.Array[float64]{Elements: []float64{1.5, 2.5, 3.5, 4.5}, Dims: []pgtype.ArrayDimension{{Length: 2, LowerBound: 1}, {Length: 2, LowerBound: 1}}, Valid: true}, "3.5"},
	} {
		t.Run(test.name, func(t *testing.T) {
			data, err := types.Encode(test.oid, pgtype.BinaryFormatCode, test.value, nil)
			if err != nil {
				t.Fatal(err)
			}
			_, statement, err := handler.HandleParseQuery(context.Background(), &pgproto3.Parse{Query: test.query, ParameterOIDs: []uint32{test.oid}})
			if err != nil {
				t.Fatal(err)
			}
			_, portal, err := handler.HandleBindQuery(context.Background(), &pgproto3.Bind{Parameters: [][]byte{data}, ParameterFormatCodes: []int16{pgtype.BinaryFormatCode}}, statement)
			if err != nil {
				t.Fatal(err)
			}
			defer portal.Close()
			messages, err := handler.HandleExecuteQuery(context.Background(), &pgproto3.Execute{}, portal)
			if err != nil {
				t.Fatal(err)
			}
			assertMessageTypes(t, messages, "*pgproto3.DataRow", "*pgproto3.CommandComplete")
			if got := string(messages[0].(*pgproto3.DataRow).Values[0]); got != test.want {
				t.Fatalf("decoded %s as %q, want %q", test.name, got, test.want)
			}
		})
	}
}

func TestBinaryBindRejectsInvalidWireValues(t *testing.T) {
	handler := initQueryHandlerForTest(t)
	for _, test := range []struct {
		name   string
		oid    uint32
		format int16
		data   []byte
		code   string
	}{
		{"missing-type", 0, 1, binary.BigEndian.AppendUint32(nil, 1), PG_ERROR_CODE_PROTOCOL_VIOLATION},
		{"unknown-type", 999999, 1, []byte{1, 2, 3, 4}, PG_ERROR_CODE_INVALID_BINARY_REPRESENTATION},
		{"truncated-float", pgtype.Float4OID, 1, []byte{0, 1, 2}, PG_ERROR_CODE_INVALID_BINARY_REPRESENTATION},
		{"wrong-width-int", pgtype.Int8OID, 1, []byte{0, 0, 0, 1}, PG_ERROR_CODE_INVALID_BINARY_REPRESENTATION},
		{"truncated-uuid", pgtype.UUIDOID, 1, make([]byte, 8), PG_ERROR_CODE_INVALID_BINARY_REPRESENTATION},
		{"truncated-array-header", pgtype.Int4ArrayOID, 1, binary.BigEndian.AppendUint32(make([]byte, 8), pgtype.Int4OID), PG_ERROR_CODE_INVALID_BINARY_REPRESENTATION},
		{"invalid-format", pgtype.Float4OID, 2, nil, PG_ERROR_CODE_PROTOCOL_VIOLATION},
	} {
		t.Run(test.name, func(t *testing.T) {
			data := test.data
			if test.name == "truncated-array-header" {
				binary.BigEndian.PutUint32(data, 1)
			}
			statement := &PreparedStatement{ParameterOIDs: []uint32{test.oid}}
			_, _, err := handler.HandleBindQuery(context.Background(), &pgproto3.Bind{Parameters: [][]byte{data}, ParameterFormatCodes: []int16{test.format}}, statement)
			if err == nil {
				t.Fatal("invalid wire value accepted")
			}
			assertClientError(t, err, PG_ERROR_SEVERITY_ERROR, test.code, func() string {
				switch test.code {
				case PG_ERROR_CODE_PROTOCOL_VIOLATION:
					if test.format == 2 {
						return "invalid parameter format code"
					}
					return "binary parameter has no PostgreSQL type"
				default:
					return "invalid binary value for parameter $1"
				}
			}())
		})
	}
}

func TestBinaryBindPreservesNullAndMixedFormats(t *testing.T) {
	handler := initQueryHandlerForTest(t)
	_, statement, err := handler.HandleParseQuery(context.Background(), &pgproto3.Parse{Query: "SELECT $1::real, $2::text, $3::bigint", ParameterOIDs: []uint32{pgtype.Float4OID, pgtype.TextOID, pgtype.Int8OID}})
	if err != nil {
		t.Fatal(err)
	}
	_, portal, err := handler.HandleBindQuery(context.Background(), &pgproto3.Bind{Parameters: [][]byte{binary.BigEndian.AppendUint32(nil, math.Float32bits(1.5)), []byte("four"), nil}, ParameterFormatCodes: []int16{1, 0, 1}}, statement)
	if err != nil {
		t.Fatal(err)
	}
	defer portal.Close()
	messages, err := handler.HandleExecuteQuery(context.Background(), &pgproto3.Execute{}, portal)
	if err != nil {
		t.Fatal(err)
	}
	row := messages[0].(*pgproto3.DataRow)
	if string(row.Values[0]) != "1.5" || string(row.Values[1]) != "four" || row.Values[2] != nil {
		t.Fatalf("mixed parameters = %q", row.Values)
	}
}

func TestBinaryBindOverWireAndRecovery(t *testing.T) {
	frontend, _, _, _ := cursorTestConnection(t)
	sendCursorMessages(t, frontend,
		&pgproto3.Parse{Name: "float", Query: "SELECT $1::real"},
		&pgproto3.Describe{ObjectType: 'S', Name: "float"},
		&pgproto3.Bind{PreparedStatement: "float", Parameters: [][]byte{binary.BigEndian.AppendUint32(nil, math.Float32bits(1.5))}, ParameterFormatCodes: []int16{1}},
		&pgproto3.Execute{}, &pgproto3.Sync{},
	)
	described, rows := false, 0
	for {
		message := receiveCursorMessage(t, frontend)
		switch message := message.(type) {
		case *pgproto3.ErrorResponse:
			t.Fatalf("server error: %+v", message)
		case *pgproto3.ParameterDescription:
			if len(message.ParameterOIDs) != 1 || message.ParameterOIDs[0] != pgtype.Float4OID {
				t.Fatalf("parameter OIDs = %v", message.ParameterOIDs)
			}
			described = true
		case *pgproto3.DataRow:
			rows++
			if string(message.Values[0]) != "1.5" {
				t.Fatalf("binary float = %q", message.Values)
			}
		}
		if _, ok := message.(*pgproto3.ReadyForQuery); ok {
			break
		}
	}
	if !described || rows != 1 {
		t.Fatalf("described=%v rows=%d", described, rows)
	}
	sendCursorMessages(t, frontend,
		&pgproto3.Bind{PreparedStatement: "float", Parameters: [][]byte{{0, 1}}, ParameterFormatCodes: []int16{1}},
		&pgproto3.Execute{}, &pgproto3.Sync{},
	)
	failure, ok := receiveCursorMessage(t, frontend).(*pgproto3.ErrorResponse)
	if !ok || failure.Code != PG_ERROR_CODE_INVALID_BINARY_REPRESENTATION {
		t.Fatalf("invalid binary response = %+v", failure)
	}
	if ready, ok := receiveCursorMessage(t, frontend).(*pgproto3.ReadyForQuery); !ok || ready.TxStatus != 'I' {
		t.Fatalf("recovery response = %+v", ready)
	}
	// The prepared statement and connection remain usable after Sync.
	sendCursorMessages(t, frontend, &pgproto3.Bind{PreparedStatement: "float", Parameters: [][]byte{[]byte("2.5")}}, &pgproto3.Execute{}, &pgproto3.Sync{})
	if _, ok := receiveCursorMessage(t, frontend).(*pgproto3.BindComplete); !ok {
		t.Fatal("missing BindComplete")
	}
	if row, ok := receiveCursorMessage(t, frontend).(*pgproto3.DataRow); !ok || string(row.Values[0]) != "2.5" {
		t.Fatalf("recovered row = %+v", row)
	}
	if _, ok := receiveCursorMessage(t, frontend).(*pgproto3.CommandComplete); !ok {
		t.Fatal("missing CommandComplete")
	}
	if _, ok := receiveCursorMessage(t, frontend).(*pgproto3.ReadyForQuery); !ok {
		t.Fatal("missing ReadyForQuery")
	}
}

func TestBinaryArrayRejectsMalformedFrames(t *testing.T) {
	types := pgtype.NewMap()
	encoded, err := types.Encode(pgtype.Int4ArrayOID, pgtype.BinaryFormatCode, []int32{1, 2}, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name   string
		mutate func([]byte) []byte
	}{
		{"truncated-length", func(data []byte) []byte { return data[:22] }},
		{"truncated-value", func(data []byte) []byte { return data[:len(data)-1] }},
		{"trailing-bytes", func(data []byte) []byte { return append(data, 0) }},
		{"negative-length", func(data []byte) []byte { binary.BigEndian.PutUint32(data[20:], 0xfffffffe); return data }},
		{"oversized-length", func(data []byte) []byte { binary.BigEndian.PutUint32(data[20:], 0x7fffffff); return data }},
		{"oversized-dimension", func(data []byte) []byte { binary.BigEndian.PutUint32(data[12:], 0x7fffffff); return data }},
		{"negative-dimension", func(data []byte) []byte { binary.BigEndian.PutUint32(data[12:], 0xffffffff); return data }},
		{"excess-dimensions", func(data []byte) []byte { binary.BigEndian.PutUint32(data, 7); return data }},
		{"wrong-element-type", func(data []byte) []byte { binary.BigEndian.PutUint32(data[8:], pgtype.Float4OID); return data }},
		{"unsupported-lower-bound", func(data []byte) []byte { binary.BigEndian.PutUint32(data[16:], 0); return data }},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := decodeBinaryBindParameter(types, pgtype.Int4ArrayOID, test.mutate(append([]byte(nil), encoded...))); err == nil {
				t.Fatal("malformed or unsupported binary array accepted")
			}
		})
	}
}
