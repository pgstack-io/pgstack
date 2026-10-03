package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"time"

	"github.com/google/uuid"
	"github.com/linkedin/goavro"
)

const (
	ICEBERG_METADATA_FILE_NAME = "v1.metadata.json"
)

const MANIFEST_SCHEMA = `{
  "type": "record",
  "name": "manifest_entry",
  "fields": [
    {"name": "status", "type": "int", "field-id": 0},
    {"name": "snapshot_id", "type": ["null", "long"], "default": null, "field-id": 1},
    {"name": "sequence_number", "type": ["null", "long"], "default": null, "field-id": 3},
    {"name": "file_sequence_number", "type": ["null", "long"], "default": null, "field-id": 4},
    {
      "name": "data_file",
      "type": {
        "type": "record",
        "name": "data_file",
        "fields": [
          {"name": "content", "type": "int", "default": 0, "field-id": 134},
          {"name": "file_path", "type": "string", "field-id": 100},
          {"name": "file_format", "type": "string", "field-id": 101},
          {"name": "partition", "type": "null", "default": null, "field-id": 102},
          {"name": "record_count", "type": "long", "field-id": 103},
          {"name": "file_size_in_bytes", "type": "long", "field-id": 104},
          {"name": "column_sizes", "type": ["null", {"type": "array", "logicalType": "map", "items": {"type": "record", "name": "k117_v118", "fields": [{"name": "key", "type": "int", "field-id": 117}, {"name": "value", "type": "long", "field-id": 118}]}}], "default": null, "field-id": 108},
          {"name": "value_counts", "type": ["null", {"type": "array", "logicalType": "map", "items": {"type": "record", "name": "k119_v120", "fields": [{"name": "key", "type": "int", "field-id": 119}, {"name": "value", "type": "long", "field-id": 120}]}}], "default": null, "field-id": 109},
          {"name": "null_value_counts", "type": ["null", {"type": "array", "logicalType": "map", "items": {"type": "record", "name": "k121_v122", "fields": [{"name": "key", "type": "int", "field-id": 121}, {"name": "value", "type": "long", "field-id": 122}]}}], "default": null, "field-id": 110},
          {"name": "nan_value_counts", "type": ["null", {"type": "array", "logicalType": "map", "items": {"type": "record", "name": "k138_v139", "fields": [{"name": "key", "type": "int", "field-id": 138}, {"name": "value", "type": "long", "field-id": 139}]}}], "default": null, "field-id": 137},
          {"name": "lower_bounds", "type": ["null", {"type": "array", "logicalType": "map", "items": {"type": "record", "name": "k126_v127", "fields": [{"name": "key", "type": "int", "field-id": 126}, {"name": "value", "type": "bytes", "field-id": 127}]}}], "default": null, "field-id": 125},
          {"name": "upper_bounds", "type": ["null", {"type": "array", "logicalType": "map", "items": {"type": "record", "name": "k129_v130", "fields": [{"name": "key", "type": "int", "field-id": 129}, {"name": "value", "type": "bytes", "field-id": 130}]}}], "default": null, "field-id": 128},
          {"name": "key_metadata", "type": ["null", "bytes"], "default": null, "field-id": 131},
          {"name": "split_offsets", "type": ["null", {"type": "array", "items": "long", "element-id": 133}], "default": null, "field-id": 132},
          {"name": "equality_ids", "type": ["null", {"type": "array", "items": "int", "element-id": 136}], "default": null, "field-id": 135},
          {"name": "sort_order_id", "type": ["null", "int"], "default": null, "field-id": 140}
        ]
      },
      "field-id": 2
    }
  ]
}`

const MANIFEST_LIST_SCHEMA = `{
  "type": "record",
  "name": "manifest_file",
  "fields": [
    {"name": "manifest_path", "type": "string", "field-id": 500},
    {"name": "manifest_length", "type": "long", "field-id": 501},
    {"name": "partition_spec_id", "type": "int", "field-id": 502},
    {"name": "content", "type": "int", "default": 0, "field-id": 517},
    {"name": "sequence_number", "type": "long", "field-id": 515},
    {"name": "min_sequence_number", "type": "long", "field-id": 516},
    {"name": "added_snapshot_id", "type": "long", "field-id": 503},
    {"name": "added_files_count", "type": "int", "field-id": 504},
    {"name": "existing_files_count", "type": "int", "field-id": 505},
    {"name": "deleted_files_count", "type": "int", "field-id": 506},
    {"name": "added_rows_count", "type": "long", "field-id": 512},
    {"name": "existing_rows_count", "type": "long", "field-id": 513},
    {"name": "deleted_rows_count", "type": "long", "field-id": 514},
    {"name": "partitions", "type": ["null", {"type": "array", "element-id": 508, "items": {"type": "record", "name": "r508", "fields": [{"name": "contains_null", "type": "boolean", "field-id": 509}, {"name": "contains_nan", "type": ["null", "boolean"], "default": null, "field-id": 518}, {"name": "lower_bound", "type": ["null", "bytes"], "default": null, "field-id": 510}, {"name": "upper_bound", "type": ["null", "bytes"], "default": null, "field-id": 511}]}}], "default": null, "field-id": 507},
    {"name": "key_metadata", "type": ["null", "bytes"], "default": null, "field-id": 519}
  ]
}`

type ManifestFile struct {
	Path             string
	Size             int64
	TotalRecordCount int64
	TotalDataFiles   int32
}

type ManifestListFile struct {
	Path           string
	SnapshotId     int64
	SequenceNumber int
	TimestampMs    int64
	TotalFilesSize int64
	TotalDataFiles int64
	TotalRecords   int64
}

type IcebergMetadataWriter struct {
	config       *Config
	s3Client     *S3Client
	tableS3Path  string
	metadataPath string
}

func NewIcebergMetadataWriter(config *Config, s3Client *S3Client, tableS3Path, metadataPath string) *IcebergMetadataWriter {
	return &IcebergMetadataWriter{
		config:       config,
		s3Client:     s3Client,
		tableS3Path:  tableS3Path,
		metadataPath: metadataPath,
	}
}

func (w *IcebergMetadataWriter) WriteManifest(parquetFiles []ParquetFileInfo) (ManifestFile, error) {
	timestamp := time.Now().Format("20060102-150405")
	filename := fmt.Sprintf("%s_%s-m0.avro", timestamp, uuid.New().String()[:8])
	tempFile, err := os.CreateTemp("", "manifest-*.avro")
	if err != nil {
		return ManifestFile{}, fmt.Errorf("failed to create temp file: %w", err)
	}
	defer os.Remove(tempFile.Name())
	defer tempFile.Close()

	manifestEntries := make([]map[string]interface{}, len(parquetFiles))

	for i, pf := range parquetFiles {
		columnSizesArr := []interface{}{}
		for fieldId, size := range pf.Stats.ColumnSizes {
			columnSizesArr = append(columnSizesArr, map[string]interface{}{
				"key":   fieldId,
				"value": size,
			})
		}

		valueCountsArr := []interface{}{}
		for fieldId, count := range pf.Stats.ValueCounts {
			valueCountsArr = append(valueCountsArr, map[string]interface{}{
				"key":   fieldId,
				"value": count,
			})
		}

		nullValueCountsArr := []interface{}{}
		for fieldId, count := range pf.Stats.NullValueCounts {
			nullValueCountsArr = append(nullValueCountsArr, map[string]interface{}{
				"key":   fieldId,
				"value": count,
			})
		}

		lowerBoundsArr := []interface{}{}
		for fieldId, val := range pf.Stats.LowerBounds {
			lowerBoundsArr = append(lowerBoundsArr, map[string]interface{}{
				"key":   fieldId,
				"value": val,
			})
		}

		upperBoundsArr := []interface{}{}
		for fieldId, val := range pf.Stats.UpperBounds {
			upperBoundsArr = append(upperBoundsArr, map[string]interface{}{
				"key":   fieldId,
				"value": val,
			})
		}

		nanValueCountsArr := []interface{}{}

		dataFile := map[string]interface{}{
			"content":            0,
			"file_path":          pf.Path,
			"file_format":        "PARQUET",
			"partition":          nil,
			"record_count":       pf.RecordCount,
			"file_size_in_bytes": pf.Size,
			"column_sizes": map[string]interface{}{
				"array": columnSizesArr,
			},
			"value_counts": map[string]interface{}{
				"array": valueCountsArr,
			},
			"null_value_counts": map[string]interface{}{
				"array": nullValueCountsArr,
			},
			"nan_value_counts": map[string]interface{}{
				"array": nanValueCountsArr,
			},
			"lower_bounds": map[string]interface{}{
				"array": lowerBoundsArr,
			},
			"upper_bounds": map[string]interface{}{
				"array": upperBoundsArr,
			},
			"key_metadata": nil,
			"split_offsets": map[string]interface{}{
				"array": pf.Stats.SplitOffsets,
			},
			"equality_ids": nil,
			"sort_order_id": map[string]interface{}{
				"int": 0,
			},
		}

		snapshotId := time.Now().UnixNano()
		manifestEntries[i] = map[string]interface{}{
			"status":               1, // ADDED
			"snapshot_id":          map[string]interface{}{"long": snapshotId},
			"sequence_number":      map[string]interface{}{"long": int64(i + 1)},
			"file_sequence_number": map[string]interface{}{"long": int64(i + 1)},
			"data_file":            dataFile,
		}
	}

	codec, err := goavro.NewCodec(MANIFEST_SCHEMA)
	if err != nil {
		return ManifestFile{}, fmt.Errorf("failed to create avro codec: %w", err)
	}

	ocfWriter, err := goavro.NewOCFWriter(goavro.OCFConfig{
		W:      tempFile,
		Codec:  codec,
		Schema: MANIFEST_SCHEMA,
	})
	if err != nil {
		return ManifestFile{}, fmt.Errorf("failed to create OCF writer: %w", err)
	}

	if err := ocfWriter.Append(manifestEntries); err != nil {
		return ManifestFile{}, fmt.Errorf("failed to write manifest entries: %w", err)
	}

	tempFile.Close()

	// Read file and upload to S3
	data, err := os.ReadFile(tempFile.Name())
	if err != nil {
		return ManifestFile{}, fmt.Errorf("failed to read temp file: %w", err)
	}

	s3Key := path.Join(w.metadataPath, filename)
	if err := w.s3Client.Upload(context.TODO(), s3Key, data); err != nil {
		return ManifestFile{}, fmt.Errorf("failed to upload manifest: %w", err)
	}

	var totalRecords int64
	for _, pf := range parquetFiles {
		totalRecords += pf.RecordCount
	}

	return ManifestFile{
		Path:             w.config.S3Path(s3Key),
		Size:             int64(len(data)),
		TotalRecordCount: totalRecords,
		TotalDataFiles:   int32(len(parquetFiles)),
	}, nil
}

func (w *IcebergMetadataWriter) WriteManifestList(manifestFile ManifestFile, sequenceNumber int, totalDataFileSize int64) (ManifestListFile, error) {
	timestamp := time.Now().Format("20060102-150405")
	filename := fmt.Sprintf("snap-%s_%s.avro", timestamp, uuid.New().String()[:8])
	tempFile, err := os.CreateTemp("", "manifest-list-*.avro")
	if err != nil {
		return ManifestListFile{}, fmt.Errorf("failed to create temp file: %w", err)
	}
	defer os.Remove(tempFile.Name())
	defer tempFile.Close()

	snapshotId := time.Now().UnixNano()
	timestampMs := time.Now().UnixMilli()

	manifestListEntry := map[string]interface{}{
		"manifest_path":        manifestFile.Path,
		"manifest_length":      manifestFile.Size,
		"partition_spec_id":    0,
		"content":              0,
		"sequence_number":      int64(sequenceNumber),
		"min_sequence_number":  int64(1),
		"added_snapshot_id":    snapshotId,
		"added_files_count":    int32(0),
		"existing_files_count": manifestFile.TotalDataFiles,
		"deleted_files_count":  int32(0),
		"added_rows_count":     int64(0),
		"existing_rows_count":  manifestFile.TotalRecordCount,
		"deleted_rows_count":   int64(0),
		"partitions":           nil,
		"key_metadata":         nil,
	}

	codec, err := goavro.NewCodec(MANIFEST_LIST_SCHEMA)
	if err != nil {
		return ManifestListFile{}, fmt.Errorf("failed to create avro codec: %w", err)
	}

	ocfWriter, err := goavro.NewOCFWriter(goavro.OCFConfig{
		W:      tempFile,
		Codec:  codec,
		Schema: MANIFEST_LIST_SCHEMA,
	})
	if err != nil {
		return ManifestListFile{}, fmt.Errorf("failed to create OCF writer: %w", err)
	}

	if err := ocfWriter.Append([]map[string]interface{}{manifestListEntry}); err != nil {
		return ManifestListFile{}, fmt.Errorf("failed to write manifest list entry: %w", err)
	}

	tempFile.Close()

	// Read and upload
	data, err := os.ReadFile(tempFile.Name())
	if err != nil {
		return ManifestListFile{}, fmt.Errorf("failed to read temp file: %w", err)
	}

	s3Key := path.Join(w.metadataPath, filename)
	if err := w.s3Client.Upload(context.TODO(), s3Key, data); err != nil {
		return ManifestListFile{}, fmt.Errorf("failed to upload manifest list: %w", err)
	}

	return ManifestListFile{
		Path:           w.config.S3Path(s3Key),
		SnapshotId:     snapshotId,
		SequenceNumber: sequenceNumber,
		TimestampMs:    timestampMs,
		TotalFilesSize: totalDataFileSize,
		TotalDataFiles: int64(manifestFile.TotalDataFiles),
		TotalRecords:   manifestFile.TotalRecordCount,
	}, nil
}

func (w *IcebergMetadataWriter) WriteMetadata(manifestListFile ManifestListFile) error {
	tableUUID := uuid.New().String()

	metadata := map[string]interface{}{
		"format-version":  2,
		"table-uuid":      tableUUID,
		"location":        w.config.S3Path(w.tableS3Path),
		"last-updated-ms": manifestListFile.TimestampMs,
		"last-column-id":  14,
		"schemas": []interface{}{
			map[string]interface{}{
				"type":      "struct",
				"schema-id": 0,
				"fields": []interface{}{
					map[string]interface{}{"id": 1, "name": "id", "required": true, "type": "string"},
					map[string]interface{}{"id": 2, "name": "database", "required": true, "type": "string"},
					map[string]interface{}{"id": 3, "name": "schema", "required": true, "type": "string"},
					map[string]interface{}{"id": 4, "name": "table", "required": true, "type": "string"},
					map[string]interface{}{"id": 5, "name": "primary_key", "required": false, "type": "string"},
					map[string]interface{}{"id": 6, "name": "operation", "required": true, "type": "string"},
					map[string]interface{}{"id": 7, "name": "before", "required": false, "type": "string"},
					map[string]interface{}{"id": 8, "name": "after", "required": false, "type": "string"},
					map[string]interface{}{"id": 9, "name": "context", "required": false, "type": "string"},
					map[string]interface{}{"id": COMMITTED_AT_FIELD_ID, "name": "committed_at", "required": true, "type": "timestamptz"},
					map[string]interface{}{"id": 11, "name": "queued_at", "required": true, "type": "timestamptz"},
					map[string]interface{}{"id": 12, "name": "created_at", "required": true, "type": "timestamptz"},
					map[string]interface{}{"id": 13, "name": "transaction_id", "required": true, "type": "long"},
					map[string]interface{}{"id": 14, "name": "position", "required": true, "type": "long"},
				},
			},
		},
		"current-schema-id": 0,
		"partition-specs": []interface{}{
			map[string]interface{}{
				"spec-id": 0,
				"fields":  []interface{}{},
			},
		},
		"default-spec-id":       0,
		"last-partition-id":     999,
		"default-sort-order-id": 0,
		"sort-orders": []interface{}{
			map[string]interface{}{
				"order-id": 0,
				"fields":   []interface{}{},
			},
		},
		"properties":          map[string]string{},
		"current-snapshot-id": manifestListFile.SnapshotId,
		"refs": map[string]interface{}{
			"main": map[string]interface{}{
				"snapshot-id": manifestListFile.SnapshotId,
				"type":        "branch",
			},
		},
		"snapshots": []interface{}{
			map[string]interface{}{
				"snapshot-id":     manifestListFile.SnapshotId,
				"timestamp-ms":    manifestListFile.TimestampMs,
				"sequence-number": manifestListFile.SequenceNumber,
				"manifest-list":   manifestListFile.Path,
				"summary": map[string]interface{}{
					"operation":        "append",
					"total-data-files": fmt.Sprintf("%d", manifestListFile.TotalDataFiles),
					"total-records":    fmt.Sprintf("%d", manifestListFile.TotalRecords),
					"total-files-size": fmt.Sprintf("%d", manifestListFile.TotalFilesSize),
				},
			},
		},
		"snapshot-log": []interface{}{
			map[string]interface{}{
				"snapshot-id":  manifestListFile.SnapshotId,
				"timestamp-ms": manifestListFile.TimestampMs,
			},
		},
		"metadata-log": []interface{}{},
	}

	data, err := json.MarshalIndent(metadata, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal metadata: %w", err)
	}

	s3Key := path.Join(w.metadataPath, ICEBERG_METADATA_FILE_NAME)
	if err := w.s3Client.Upload(context.TODO(), s3Key, data); err != nil {
		return fmt.Errorf("failed to upload metadata: %w", err)
	}

	return nil
}
