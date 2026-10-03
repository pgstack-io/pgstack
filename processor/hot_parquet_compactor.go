package main

import (
	"context"
	"fmt"
	"path"
	"strings"
)

const (
	HOT_PARQUET_COMPACTION_MIN_PERCENT = 80
	HOT_PARQUET_COMPACTION_MAX_PERCENT = 120
)

func (w *IcebergWriter) compactHotParquetFiles(ctx context.Context, parquetFiles []ParquetFileInfo) ([]ParquetFileInfo, []string, error) {
	if len(parquetFiles) < 3 {
		return parquetFiles, nil, nil
	}

	maxHotSize := w.config.MaxHotParquetFileSizeBytes()
	if maxHotSize <= 0 {
		return parquetFiles, nil, nil
	}

	pairIndex := hotParquetPairIndex(parquetFiles, maxHotSize)
	if pairIndex < 0 {
		return parquetFiles, nil, nil
	}

	newFile, err := w.mergeParquetFiles(ctx, parquetFiles[pairIndex:pairIndex+2], parquetFilenamePrefix(parquetFiles[pairIndex].Path))
	if err != nil {
		return nil, nil, err
	}

	filesToDelete := []string{
		w.s3PathToKey(parquetFiles[pairIndex].Path),
		w.s3PathToKey(parquetFiles[pairIndex+1].Path),
	}

	compactedFiles := make([]ParquetFileInfo, 0, len(parquetFiles)-1)
	compactedFiles = append(compactedFiles, parquetFiles[:pairIndex]...)
	compactedFiles = append(compactedFiles, newFile)
	compactedFiles = append(compactedFiles, parquetFiles[pairIndex+2:]...)

	LogInfo(w.config, fmt.Sprintf(
		"Compacted hot parquet pair: %s + %s -> %s",
		parquetFilename(parquetFiles[pairIndex].Path),
		parquetFilename(parquetFiles[pairIndex+1].Path),
		parquetFilename(newFile.Path),
	))

	return compactedFiles, filesToDelete, nil
}

func hotParquetPairIndex(parquetFiles []ParquetFileInfo, maxHotSize int64) int {
	latestFileIndex := len(parquetFiles) - 1
	for i := 0; i+1 < latestFileIndex; i++ {
		if shouldCompactHotParquetPair(parquetFiles[i], parquetFiles[i+1], maxHotSize) {
			return i
		}
	}

	return -1
}

func shouldCompactHotParquetPair(first, second ParquetFileInfo, maxHotSize int64) bool {
	return isAroundHotParquetSize(first.Size, maxHotSize) &&
		isAroundHotParquetSize(second.Size, maxHotSize)
}

func isAroundHotParquetSize(size, maxHotSize int64) bool {
	return size >= maxHotSize*HOT_PARQUET_COMPACTION_MIN_PERCENT/100 &&
		size <= maxHotSize*HOT_PARQUET_COMPACTION_MAX_PERCENT/100
}

func parquetFilename(parquetPath string) string {
	return path.Base(parquetPath)
}

func parquetFilenamePrefix(parquetPath string) string {
	filename := parquetFilename(parquetPath)
	prefix, _, found := strings.Cut(filename, "_")
	if !found {
		return strings.TrimSuffix(filename, ".parquet")
	}
	return prefix
}
