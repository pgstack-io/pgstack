package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/linkedin/goavro"
)

var ErrIcebergMetadataNotFound = errors.New("iceberg metadata not found")

type StorageUtils struct {
	config   *Config
	s3Client *S3Client
}

func NewStorageUtils(config *Config, s3Client *S3Client) *StorageUtils {
	return &StorageUtils{
		config:   config,
		s3Client: s3Client,
	}
}

func (utils *StorageUtils) CurrentParquetFiles(ctx context.Context, metadataKey string) ([]ParquetFileInfo, error) {
	metadata, err := utils.parseMetadata(ctx, metadataKey)
	if err != nil {
		return nil, fmt.Errorf("failed to parse metadata: %w", err)
	}

	manifestListPath, err := utils.currentManifestListPath(metadata)
	if err != nil {
		return nil, err
	}

	manifestKey, err := utils.readManifestList(ctx, utils.s3PathToKey(manifestListPath))
	if err != nil {
		return nil, err
	}

	parquetFiles, err := utils.readManifestParquetFiles(ctx, manifestKey)
	if err != nil {
		return nil, err
	}

	return parquetFiles, nil
}

func (utils *StorageUtils) parseMetadata(ctx context.Context, metadataKey string) (*IcebergMetadata, error) {
	data, err := utils.s3Client.Download(ctx, metadataKey)
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok && awsErr.Code() == "NoSuchKey" {
			return nil, ErrIcebergMetadataNotFound
		}
		return nil, err
	}

	var metadata IcebergMetadata
	if err := json.Unmarshal(data, &metadata); err != nil {
		return nil, err
	}

	return &metadata, nil
}

func (utils *StorageUtils) currentManifestListPath(metadata *IcebergMetadata) (string, error) {
	if metadata == nil || len(metadata.Snapshots) == 0 {
		return "", fmt.Errorf("metadata has no snapshots")
	}

	snapshot := metadata.Snapshots[0]
	if snapshot.ManifestList == "" {
		return "", fmt.Errorf("snapshot %d has no manifest list", snapshot.SnapshotId)
	}
	return snapshot.ManifestList, nil
}

func (utils *StorageUtils) readManifestList(ctx context.Context, manifestListKey string) (string, error) {
	data, err := utils.s3Client.Download(ctx, manifestListKey)
	if err != nil {
		return "", fmt.Errorf("failed to download manifest list %s: %w", manifestListKey, err)
	}

	ocfReader, err := goavro.NewOCFReader(bytes.NewReader(data))
	if err != nil {
		return "", fmt.Errorf("failed to read manifest list %s: %w", manifestListKey, err)
	}

	if !ocfReader.Scan() {
		return "", fmt.Errorf("manifest list %s has no records", manifestListKey)
	}

	record, err := ocfReader.Read()
	if err != nil {
		return "", fmt.Errorf("failed to decode manifest list %s: %w", manifestListKey, err)
	}

	recordMap, ok := record.(map[string]interface{})
	if !ok {
		return "", fmt.Errorf("unexpected manifest list record in %s", manifestListKey)
	}

	manifestPath, ok := recordMap["manifest_path"].(string)
	if !ok || manifestPath == "" {
		return "", fmt.Errorf("manifest list %s contains invalid manifest_path", manifestListKey)
	}

	return utils.s3PathToKey(manifestPath), nil
}

func (utils *StorageUtils) readManifestParquetFiles(ctx context.Context, manifestKey string) ([]ParquetFileInfo, error) {
	data, err := utils.s3Client.Download(ctx, manifestKey)
	if err != nil {
		return nil, fmt.Errorf("failed to download manifest %s: %w", manifestKey, err)
	}

	ocfReader, err := goavro.NewOCFReader(bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("failed to read manifest %s: %w", manifestKey, err)
	}

	var parquetFiles []ParquetFileInfo
	for ocfReader.Scan() {
		record, err := ocfReader.Read()
		if err != nil {
			return nil, fmt.Errorf("failed to decode manifest %s: %w", manifestKey, err)
		}

		recordMap, ok := record.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("unexpected manifest record in %s", manifestKey)
		}

		dataFile, ok := recordMap["data_file"].(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("manifest %s contains invalid data_file", manifestKey)
		}

		filePath, ok := dataFile["file_path"].(string)
		if !ok || filePath == "" {
			return nil, fmt.Errorf("manifest %s contains invalid file_path", manifestKey)
		}

		parquetFiles = append(parquetFiles, ParquetFileInfo{
			Path:        filePath,
			Size:        avroInt64(dataFile["file_size_in_bytes"]),
			RecordCount: avroInt64(dataFile["record_count"]),
			Stats: ParquetFileStats{
				ColumnSizes:     avroInt64Map(dataFile["column_sizes"]),
				ValueCounts:     avroInt64Map(dataFile["value_counts"]),
				NullValueCounts: avroInt64Map(dataFile["null_value_counts"]),
				LowerBounds:     avroBytesMap(dataFile["lower_bounds"]),
				UpperBounds:     avroBytesMap(dataFile["upper_bounds"]),
				SplitOffsets:    avroInt64Slice(dataFile["split_offsets"]),
			},
		})
	}

	return parquetFiles, nil
}

func avroInt64(value interface{}) int64 {
	switch v := value.(type) {
	case int:
		return int64(v)
	case int32:
		return int64(v)
	case int64:
		return v
	case float64:
		return int64(v)
	default:
		return 0
	}
}

func avroInt64Map(value interface{}) map[int]int64 {
	result := make(map[int]int64)
	for _, entry := range avroArray(value) {
		entryMap, ok := entry.(map[string]interface{})
		if !ok {
			continue
		}
		result[int(avroInt64(entryMap["key"]))] = avroInt64(entryMap["value"])
	}
	return result
}

func avroBytesMap(value interface{}) map[int][]byte {
	result := make(map[int][]byte)
	for _, entry := range avroArray(value) {
		entryMap, ok := entry.(map[string]interface{})
		if !ok {
			continue
		}
		bytesValue, ok := entryMap["value"].([]byte)
		if !ok {
			continue
		}
		result[int(avroInt64(entryMap["key"]))] = bytesValue
	}
	return result
}

func avroInt64Slice(value interface{}) []int64 {
	values := avroArray(value)
	result := make([]int64, 0, len(values))
	for _, value := range values {
		result = append(result, avroInt64(value))
	}
	return result
}

func avroArray(value interface{}) []interface{} {
	unionMap, ok := value.(map[string]interface{})
	if !ok {
		return nil
	}
	array, ok := unionMap["array"].([]interface{})
	if !ok {
		return nil
	}
	return array
}

func (utils *StorageUtils) s3PathToKey(s3Path string) string {
	prefix := fmt.Sprintf("s3://%s/", utils.config.S3Bucket)
	return strings.TrimPrefix(s3Path, prefix)
}
