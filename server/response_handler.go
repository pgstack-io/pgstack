package main

import (
	"bytes"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/duckdb/duckdb-go/v2"
	"github.com/jackc/pgx/v5/pgtype"
)

type ResponseHandler struct {
	Config *Config
}

func NewResponseHandler(config *Config) *ResponseHandler {
	return &ResponseHandler{
		Config: config,
	}
}

// https://pkg.go.dev/github.com/jackc/pgx/v5/pgtype#pkg-constants
func (responseHandler *ResponseHandler) ColumnDescriptionTypeOid(col *sql.ColumnType) uint32 {
	databaseTypeName := col.DatabaseTypeName()
	switch databaseTypeName {
	case "BOOLEAN":
		return pgtype.BoolOID
	case "BOOLEAN[]":
		return pgtype.BoolArrayOID
	case "SMALLINT":
		return pgtype.Int2OID
	case "SMALLINT[]":
		return pgtype.Int2ArrayOID
	case "INTEGER":
		return pgtype.Int4OID
	case "INTEGER[]":
		return pgtype.Int4ArrayOID
	case "UINTEGER":
		return pgtype.XIDOID
	case "UINTEGER[]":
		return pgtype.XIDArrayOID
	case "BIGINT":
		if responseHandler.isSystemTableOidColumn(col.Name()) {
			return pgtype.OIDOID
		}
		return pgtype.Int8OID
	case "BIGINT[]":
		return pgtype.Int8ArrayOID
	case "UBIGINT":
		return pgtype.XID8OID
	case "UBIGINT[]":
		return pgtype.XID8ArrayOID
	case "HUGEINT":
		return pgtype.NumericOID
	case "HUGEINT[]":
		return pgtype.NumericArrayOID
	case "FLOAT":
		return pgtype.Float4OID
	case "FLOAT[]":
		return pgtype.Float4ArrayOID
	case "DOUBLE":
		return pgtype.Float8OID
	case "DOUBLE[]":
		return pgtype.Float8ArrayOID
	case "VARCHAR":
		return pgtype.TextOID
	case "VARCHAR[]":
		return pgtype.TextArrayOID
	case "TIME":
		return pgtype.TimeOID
	case "TIME[]":
		return pgtype.TimeArrayOID
	case "DATE":
		return pgtype.DateOID
	case "DATE[]":
		return pgtype.DateArrayOID
	case "TIMESTAMP":
		return pgtype.TimestampOID
	case "TIMESTAMP[]":
		return pgtype.TimestampArrayOID
	case "TIMESTAMPTZ":
		return pgtype.TimestamptzOID
	case "TIMESTAMPTZ[]":
		return pgtype.TimestamptzArrayOID
	case "UUID":
		return pgtype.UUIDOID
	case "UUID[]":
		return pgtype.UUIDArrayOID
	case "INTERVAL":
		return pgtype.IntervalOID
	case "INTERVAL[]":
		return pgtype.IntervalArrayOID
	case "BLOB":
		return pgtype.ByteaOID
	case "JSON":
		return pgtype.JSONOID
	}

	// Lance embedding columns use DuckDB fixed-size array types such as
	// FLOAT[1536], which PostgreSQL clients should see as float4[].
	if strings.HasPrefix(databaseTypeName, "FLOAT[") && strings.HasSuffix(databaseTypeName, "]") {
		return pgtype.Float4ArrayOID
	}

	if strings.HasPrefix(databaseTypeName, "DECIMAL") {
		if strings.HasSuffix(databaseTypeName, "[]") {
			return pgtype.NumericArrayOID
		} else {
			return pgtype.NumericOID
		}
	}

	Panic(responseHandler.Config, "Unsupported serialized column type: "+databaseTypeName)
	return 0
}

func (responseHandler *ResponseHandler) RowValuePointer(col *sql.ColumnType) interface{} {
	if col.DatabaseTypeName() == "UINTEGER" || col.DatabaseTypeName() == "UBIGINT" {
		return new(NullUint64)
	}
	switch col.ScanType().String() {
	case "int16":
		return new(sql.NullInt16)
	case "int32": // ints, xid
		return new(sql.NullInt32)
	case "int64", "*big.Int":
		return new(sql.NullInt64)
	case "float64", "float32":
		return new(sql.NullFloat64)
	case "string":
		// return a *sql.NullString so Scan writes into the right type
		return new(sql.NullString)
	case "[]uint8": // uuid, bytea
		return new(sql.NullString)
	case "bool":
		return new(sql.NullBool)
	case "time.Time":
		return new(sql.NullTime)
	case "duckdb.Decimal": // xid8
		return new(NullDecimal)
	case "duckdb.Interval":
		return new(NullInterval)
	case "interface {}": // json, jsonb
		return new(NullJson)
	case "[]interface {}":
		return new(NullArray)
	}

	Panic(responseHandler.Config, "Unsupported data row type: "+col.ScanType().String())
	return nil
}

func (responseHandler *ResponseHandler) RowValueBytes(valuePtr interface{}, col *sql.ColumnType) []byte {
	switch value := valuePtr.(type) {
	case *sql.NullInt16:
		if value.Valid {
			return []byte(IntToString(int(value.Int16)))
		} else {
			return nil
		}
	case *sql.NullInt32:
		if value.Valid {
			return []byte(IntToString(int(value.Int32)))
		} else {
			return nil
		}
	case *sql.NullInt64:
		if value.Valid {
			return []byte(IntToString(int(value.Int64)))
		} else {
			return nil
		}
	case *NullUint64:
		if value.Valid {
			return []byte(strconv.FormatUint(value.Uint64, 10))
		} else {
			return nil
		}
	case *sql.NullFloat64:
		if value.Valid {
			return []byte(fmt.Sprintf("%v", value.Float64))
		} else {
			return nil
		}
	case *sql.NullString:
		if value.Valid {
			return []byte(value.String)
		} else {
			return nil
		}
	case *sql.NullBool:
		if value.Valid {
			return []byte(fmt.Sprintf("%v", value.Bool)[0:1])
		} else {
			return nil
		}
	case *sql.NullTime:
		if value.Valid {
			switch col.DatabaseTypeName() {
			case "DATE":
				return []byte(value.Time.Format("2006-01-02"))
			case "TIME":
				return []byte(value.Time.Format("15:04:05.999999"))
			case "TIMESTAMP":
				return []byte(value.Time.Format("2006-01-02 15:04:05.999999"))
			case "TIMESTAMPTZ":
				return []byte(value.Time.Format("2006-01-02 15:04:05.999999-07:00"))
			default:
				Panic(responseHandler.Config, "Unsupported scanned time type: "+col.DatabaseTypeName())
			}
		} else {
			return nil
		}
	case *NullDecimal:
		if value.Present {
			return []byte(value.String())
		} else {
			return nil
		}
	case *NullInterval:
		if value.Present {
			return []byte(value.String())
		} else {
			return nil
		}
	case *NullJson:
		if value.Present {
			return []byte(value.String())
		} else {
			return nil
		}
	case *NullArray:
		if value.Present {
			return []byte(value.String())
		} else {
			return nil
		}
	case *string:
		return []byte(*value)
	}

	Panic(responseHandler.Config, "Unsupported scanned row type: "+col.ScanType().Name())
	return nil
}

func (responseHandler *ResponseHandler) isSystemTableOidColumn(colName string) bool {
	oidColumns := map[string]bool{
		"oid":          true,
		"tableoid":     true,
		"relnamespace": true,
		"relowner":     true,
		"relfilenode":  true,
		"did":          true,
		"objoid":       true,
		"classoid":     true,
	}

	return oidColumns[colName]
}

////////////////////////////////////////////////////////////////////////////////////////////////////

type NullDecimal struct {
	Present bool
	Value   duckdb.Decimal
}

func (nullDecimal *NullDecimal) Scan(value interface{}) error {
	if value == nil {
		nullDecimal.Present = false
		return nil
	}

	nullDecimal.Present = true
	nullDecimal.Value = value.(duckdb.Decimal)
	return nil
}

func (nullDecimal NullDecimal) String() string {
	if nullDecimal.Present {
		return fmt.Sprintf("%v", nullDecimal.Value.Float64())
	}
	return ""
}

////////////////////////////////////////////////////////////////////////////////////////////////////

type NullInterval struct {
	Present bool
	Value   duckdb.Interval
}

func (nullInterval *NullInterval) Scan(value interface{}) error {
	if value == nil {
		nullInterval.Present = false
		return nil
	}

	nullInterval.Present = true
	nullInterval.Value = value.(duckdb.Interval)
	return nil
}

func (nullInterval NullInterval) String() string {
	if nullInterval.Present {
		return fmt.Sprintf("%d months %d days %d microseconds", nullInterval.Value.Months, nullInterval.Value.Days, nullInterval.Value.Micros)
	}
	return ""
}

////////////////////////////////////////////////////////////////////////////////////////////////////

type NullJson struct {
	Present bool
	Value   interface{}
}

func (nullJson *NullJson) Scan(value interface{}) error {
	if value == nil {
		nullJson.Present = false
		return nil
	}
	nullJson.Present = true
	nullJson.Value = value
	return nil
}

func (nullJson NullJson) String() string {
	if nullJson.Present {
		// Serialize the JSON value to a string with marshaling
		jsonBytes, err := json.Marshal(nullJson.Value)
		if err != nil {
			return ""
		}
		return string(jsonBytes)
	}

	return ""
}

////////////////////////////////////////////////////////////////////////////////////////////////////

type NullArray struct {
	Present bool
	Value   []interface{}
}

type NullUint64 struct {
	Uint64 uint64
	Valid  bool
}

func (value *NullUint64) Scan(source interface{}) error {
	if source == nil {
		value.Valid = false
		return nil
	}
	parsed, err := strconv.ParseUint(fmt.Sprint(source), 10, 64)
	if err != nil {
		return err
	}
	value.Uint64 = parsed
	value.Valid = true
	return nil
}

func (nullArray *NullArray) Scan(value interface{}) error {
	if value == nil {
		nullArray.Present = false
		return nil
	}

	nullArray.Present = true
	nullArray.Value = value.([]interface{})
	return nil
}

func (nullArray NullArray) String() string {
	if nullArray.Present {
		var stringVals []string
		for _, v := range nullArray.Value {
			switch v.(type) {
			case []uint8:
				stringVals = append(stringVals, fmt.Sprintf("%s", v))
			default:
				stringVals = append(stringVals, fmt.Sprintf("%v", v))
			}
		}
		buffer := &bytes.Buffer{}
		csvWriter := csv.NewWriter(buffer)
		err := csvWriter.Write(stringVals)
		if err != nil {
			return ""
		}
		csvWriter.Flush()
		return "{" + strings.TrimRight(buffer.String(), "\n") + "}"
	}
	return ""
}
