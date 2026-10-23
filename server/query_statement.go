package main

import (
	"context"
	"database/sql"
	"encoding/binary"
	"fmt"
	"strings"
	"time"

	"github.com/duckdb/duckdb-go/v2"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"
	pgQuery "github.com/pganalyze/pg_query_go/v6"
)

type QueryKind int

const (
	QUERY_OTHER QueryKind = iota
	QUERY_SELECT
	QUERY_BEGIN
	QUERY_COMMIT
	QUERY_ROLLBACK
	QUERY_DISCARD
	QUERY_UNSUPPORTED_TRANSACTION
)

type QueryClassification struct {
	Kind  QueryKind
	Chain bool
}

func classifyQuery(node *pgQuery.Node) QueryClassification {
	if node.GetSelectStmt() != nil {
		return QueryClassification{Kind: QUERY_SELECT}
	}
	if node.GetDiscardStmt() != nil {
		return QueryClassification{Kind: QUERY_DISCARD}
	}
	if tx := node.GetTransactionStmt(); tx != nil {
		kind := QUERY_UNSUPPORTED_TRANSACTION
		switch tx.Kind {
		case pgQuery.TransactionStmtKind_TRANS_STMT_BEGIN, pgQuery.TransactionStmtKind_TRANS_STMT_START:
			kind = QUERY_BEGIN
		case pgQuery.TransactionStmtKind_TRANS_STMT_COMMIT:
			kind = QUERY_COMMIT
		case pgQuery.TransactionStmtKind_TRANS_STMT_ROLLBACK:
			kind = QUERY_ROLLBACK
		}
		return QueryClassification{Kind: kind, Chain: tx.Chain}
	}
	return QueryClassification{Kind: QUERY_OTHER}
}

func (query QueryClassification) EndsTransaction() bool {
	return query.Kind == QUERY_COMMIT || query.Kind == QUERY_ROLLBACK
}

// PreparedStatement owns query metadata. Bind creates a separate Portal for
// each execution; no mutable execution resources are shared by portals.
type PreparedStatement struct {
	Name           string
	OriginalQuery  string
	Query          string
	ParameterOIDs  []uint32
	ParameterCount int
	QueryClassification
	VectorParameters    map[int]SearchParameterSpec
	TextArrayParameters map[int]bool
}

// ResultReader allows storage-specific batch readers behind a portal.
type ResultReader interface {
	ReadBatch(context.Context, uint32) (*sql.Rows, error)
	Close() error
}

type Portal struct {
	*PreparedStatement
	Name      string
	Variables []interface{}
	Reader    ResultReader
	Terminal  pgproto3.Message
	RowsSent  int64
}

func (portal *Portal) Close() error {
	if portal == nil {
		return nil
	}
	if portal.Reader == nil {
		return nil
	}
	err := portal.Reader.Close()
	portal.Reader = nil
	return err
}

func (portal *Portal) IsCursor(maxRows uint32) bool {
	return portal.Kind == QUERY_SELECT && (maxRows > 0 || portal.Reader != nil)
}

func completedSelect(rows int64) pgproto3.Message {
	return &pgproto3.CommandComplete{CommandTag: []byte("SELECT " + IntToString(int(rows)))}
}

const PREPARE_STATEMENT_TIMEOUT = 30 * time.Second

// Validate SQL and inspect parameters without retaining a driver statement in
// session metadata. Native handles stay on their own connection and are closed
// before its runtime lease can be released.
func (handler *QueryHandler) prepareStatement(ctx context.Context, statement *PreparedStatement) error {
	ctx, cancel := context.WithTimeout(ctx, PREPARE_STATEMENT_TIMEOUT)
	defer cancel()
	conn, err := handler.ServerDuckdbClient.Db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	supplied := statement.ParameterOIDs
	oids := make([]uint32, statement.ParameterCount)
	copy(oids, supplied)
	err = conn.Raw(func(raw interface{}) error {
		native, ok := raw.(*duckdb.Conn)
		if !ok {
			return fmt.Errorf("unexpected DuckDB connection %T", raw)
		}
		prepared, err := native.PrepareContext(ctx, statement.Query)
		if err != nil {
			return err
		}
		defer prepared.Close()
		metadata, ok := prepared.(*duckdb.Stmt)
		if !ok {
			return fmt.Errorf("unexpected DuckDB statement %T", prepared)
		}
		for index := range oids {
			if oids[index] != 0 {
				continue
			}
			if statement.TextArrayParameters[index+1] {
				oids[index] = pgtype.TextArrayOID
				continue
			}
			kind, err := metadata.ParamType(index + 1)
			if err != nil {
				return err
			}
			oids[index] = parameterTypeOID(kind)
		}
		return nil
	})
	if err == nil {
		statement.ParameterOIDs = oids
	}
	return err
}

func parameterTypeOID(kind duckdb.Type) uint32 {
	switch kind {
	case duckdb.TYPE_BOOLEAN:
		return pgtype.BoolOID
	case duckdb.TYPE_TINYINT, duckdb.TYPE_SMALLINT:
		return pgtype.Int2OID
	case duckdb.TYPE_INTEGER:
		return pgtype.Int4OID
	case duckdb.TYPE_BIGINT:
		return pgtype.Int8OID
	case duckdb.TYPE_UINTEGER:
		return pgtype.XIDOID
	case duckdb.TYPE_UBIGINT:
		return pgtype.XID8OID
	case duckdb.TYPE_FLOAT:
		return pgtype.Float4OID
	case duckdb.TYPE_DOUBLE:
		return pgtype.Float8OID
	case duckdb.TYPE_DECIMAL, duckdb.TYPE_HUGEINT, duckdb.TYPE_UHUGEINT:
		return pgtype.NumericOID
	case duckdb.TYPE_BLOB:
		return pgtype.ByteaOID
	case duckdb.TYPE_UUID:
		return pgtype.UUIDOID
	case duckdb.TYPE_DATE:
		return pgtype.DateOID
	case duckdb.TYPE_TIME:
		return pgtype.TimeOID
	case duckdb.TYPE_TIME_TZ:
		return pgtype.TimetzOID
	case duckdb.TYPE_INTERVAL:
		return pgtype.IntervalOID
	case duckdb.TYPE_TIMESTAMP, duckdb.TYPE_TIMESTAMP_S, duckdb.TYPE_TIMESTAMP_MS, duckdb.TYPE_TIMESTAMP_NS:
		return pgtype.TimestampOID
	case duckdb.TYPE_TIMESTAMP_TZ:
		return pgtype.TimestamptzOID
	default:
		return pgtype.TextOID
	}
}

// DecodeDatabaseSQLValue keeps scalar values compatible with DuckDB's driver:
// floats stay floats, timestamps become time.Time, UUID/numeric/interval values
// retain their exact text representation, and bytea remains bytes.
func decodeBinaryBindParameter(types *pgtype.Map, oid uint32, data []byte) (interface{}, error) {
	typ, ok := types.TypeForOID(oid)
	if !ok || !typ.Codec.FormatSupported(pgtype.BinaryFormatCode) {
		return nil, fmt.Errorf("no binary codec for PostgreSQL type %d", oid)
	}
	if array, ok := typ.Codec.(*pgtype.ArrayCodec); ok {
		if err := validateBinaryArray(data, array.ElementType.OID); err != nil {
			return nil, err
		}
		// Let pgtype decode both dimensions and each element through its SQL codec.
		// This avoids flattening multidimensional arrays or converting JSON numbers
		// through float64 on the way to DuckDB.
		var decoded pgtype.Array[binaryArrayValue]
		if err := types.Scan(oid, pgtype.BinaryFormatCode, data, &decoded); err != nil {
			return nil, err
		}
		if !decoded.Valid {
			return nil, nil
		}
		position := 0
		return binaryArrayValues(decoded.Elements, decoded.Dims, array.ElementType.OID, &position), nil
	}
	value, err := typ.Codec.DecodeDatabaseSQLValue(types, oid, pgtype.BinaryFormatCode, data)
	// DuckDB treats byte slices as BLOBs, so JSON must bind as text.
	if err == nil && (oid == pgtype.JSONOID || oid == pgtype.JSONBOID) {
		return string(value.([]byte)), nil
	}
	return value, err
}

type binaryArrayValue struct{ value interface{} }

func (value *binaryArrayValue) Scan(src interface{}) error { value.value = src; return nil }

// Bind arrays as DuckDB list text. Its native composite binder requires exact
// Go element types and can panic when SQL casts change the expected element type.
// Quoting each scalar preserves NULLs, dimensions and exact numeric/JSON values.
func binaryArrayValues(values []binaryArrayValue, dimensions []pgtype.ArrayDimension, oid uint32, position *int) string {
	if len(dimensions) == 0 {
		return "[]"
	}
	result := make([]string, int(dimensions[0].Length))
	for index := range result {
		if len(dimensions) > 1 {
			result[index] = binaryArrayValues(values, dimensions[1:], oid, position)
			continue
		}
		value := values[*position].value
		*position += 1
		if value == nil {
			result[index] = "NULL"
			continue
		}
		var scalar string
		switch typed := value.(type) {
		case []byte:
			if oid == pgtype.ByteaOID {
				var escaped strings.Builder
				for _, octet := range typed {
					fmt.Fprintf(&escaped, "\\x%02x", octet)
				}
				scalar = escaped.String()
			} else {
				scalar = string(typed)
			}
		case time.Time:
			scalar = typed.Format("2006-01-02 15:04:05.999999999Z07:00")
		default:
			scalar = fmt.Sprint(value)
		}
		result[index] = "'" + strings.ReplaceAll(strings.ReplaceAll(scalar, "\\", "\\\\"), "'", "\\'") + "'"
	}
	return "[" + strings.Join(result, ",") + "]"
}

// pgtype's array scanner assumes complete element frames. Check their bounds
// before scanning untrusted wire input so truncation returns a Bind error.
func validateBinaryArray(data []byte, elementOID uint32) error {
	if len(data) < 12 {
		return fmt.Errorf("binary array header is truncated")
	}
	dimensions := int32(binary.BigEndian.Uint32(data))
	if dimensions < 0 || dimensions > 6 || len(data) < 12+8*int(dimensions) {
		return fmt.Errorf("invalid binary array dimensions")
	}
	if binary.BigEndian.Uint32(data[8:12]) != elementOID {
		return fmt.Errorf("binary array element type does not match its declared type")
	}
	offset := 12 + 8*int(dimensions)
	elements := int64(0)
	if dimensions > 0 {
		elements = 1
	}
	for dimension := 0; dimension < int(dimensions); dimension++ {
		length := int64(int32(binary.BigEndian.Uint32(data[12+8*dimension:])))
		if length < 0 {
			return fmt.Errorf("negative binary array dimension")
		}
		if int32(binary.BigEndian.Uint32(data[16+8*dimension:])) != 1 {
			return fmt.Errorf("non-default array lower bounds are not supported")
		}
		// At least four bytes are required for every element's length prefix.
		if length > 0 && elements > int64(len(data))/4/length {
			return fmt.Errorf("binary array dimensions exceed its payload")
		}
		elements *= length
	}
	for element := int64(0); element < elements; element++ {
		if offset > len(data)-4 {
			return fmt.Errorf("binary array element length is truncated")
		}
		length := int32(binary.BigEndian.Uint32(data[offset:]))
		offset += 4
		if length == -1 {
			continue
		}
		if length < 0 || int64(length) > int64(len(data)-offset) {
			return fmt.Errorf("binary array element is truncated")
		}
		offset += int(length)
	}
	if offset != len(data) {
		return fmt.Errorf("unexpected trailing binary array data")
	}
	return nil
}
