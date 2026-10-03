package main

import (
	"context"
	"fmt"
	"strings"
	"sync"
)

const DEDUPLICATION_MAX_SIZE = 50000

// DeduplicationKey uniquely identifies a change message
type DeduplicationKey struct {
	Position  uint64
	Database  string
	Schema    string
	Table     string
	Operation string
}

// DeduplicationTracker tracks processed messages to prevent duplicates
type DeduplicationTracker struct {
	mu        sync.RWMutex
	processed map[DeduplicationKey]bool
	order     []DeduplicationKey
	maxSize   int
}

func NewDeduplicationTracker(maxSize int) *DeduplicationTracker {
	return &DeduplicationTracker{
		processed: make(map[DeduplicationKey]bool),
		order:     make([]DeduplicationKey, 0, maxSize),
		maxSize:   maxSize,
	}
}

func (dt *DeduplicationTracker) MakeKey(msg *ChangeMessage, operation string) DeduplicationKey {
	return DeduplicationKey{
		Position:  msg.Source.Lsn,
		Database:  msg.Source.Db,
		Schema:    msg.Source.Schema,
		Table:     msg.Source.Table,
		Operation: operation,
	}
}

// LoadFromParquet reads recent records from Parquet files to populate the tracker
// Uses a sliding window approach: only loads records with LSN within the last N positions
func (dt *DeduplicationTracker) LoadFromParquet(
	ctx context.Context,
	newDuckDB func() (*DuckDBClient, error),
	config *Config,
	dataPath string,
	s3Client *S3Client,
) error {
	// Check if any parquet files exist first
	files, err := s3Client.ListFiles(ctx, dataPath)
	if err != nil {
		return fmt.Errorf("failed to list files: %w", err)
	}

	hasParquetFiles := false
	for _, file := range files {
		if strings.HasSuffix(file, ".parquet") {
			hasParquetFiles = true
			break
		}
	}

	if !hasParquetFiles {
		LogInfo(config, "No existing parquet files found, starting fresh")
		return nil
	}

	duckdb, err := newDuckDB()
	if err != nil {
		return err
	}

	s3Path := config.S3Path(dataPath + "/*.parquet")

	// First, get the max position (LSN) from existing data
	maxPosQuery := fmt.Sprintf(`
		SELECT MAX(position) as max_pos
		FROM read_parquet('%s')
	`, s3Path)

	var maxPosition *uint64
	err = duckdb.QueryRowContext(ctx, maxPosQuery).Scan(&maxPosition)
	if err != nil {
		return fmt.Errorf("failed to read maximum position from parquet files: %w", err)
	}

	if maxPosition == nil {
		LogInfo(config, "No records found in parquet files")
		return nil
	}

	// Define sliding window: only load records from last N positions
	// This prevents loading millions of records on restart
	windowSize := uint64(1000000) // 1 million LSN positions (~few hours of changes)
	minPosition := uint64(0)
	if *maxPosition > windowSize {
		minPosition = *maxPosition - windowSize
	}

	LogInfo(config, fmt.Sprintf(
		"Loading deduplication data from position %d to %d (window size: %d)",
		minPosition,
		*maxPosition,
		windowSize,
	))

	// Query to get unique keys from recent records only
	query := fmt.Sprintf(`
		SELECT DISTINCT
			position,
			database,
			schema,
			"table",
			operation
		FROM read_parquet('%s')
		WHERE position > %d
		ORDER BY position ASC
	`, s3Path, minPosition)

	rows, err := duckdb.QueryContext(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to query parquet files: %w", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var position uint64
		var database, schema, table, operation string

		if err := rows.Scan(&position, &database, &schema, &table, &operation); err != nil {
			return fmt.Errorf("failed to scan row: %w", err)
		}

		key := DeduplicationKey{
			Position:  position,
			Database:  database,
			Schema:    schema,
			Table:     table,
			Operation: operation,
		}

		dt.MarkProcessed(key)

		count++
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("error iterating rows: %w", err)
	}

	LogInfo(config, fmt.Sprintf("Loaded %d recent records into deduplication tracker (max LSN: %d)", count, *maxPosition))
	return nil
}

func (dt *DeduplicationTracker) IsDuplicate(key DeduplicationKey) bool {
	dt.mu.RLock()
	defer dt.mu.RUnlock()
	return dt.processed[key]
}

func (dt *DeduplicationTracker) MarkProcessed(key DeduplicationKey) {
	dt.mu.Lock()
	defer dt.mu.Unlock()

	if dt.processed[key] {
		return
	}

	dt.processed[key] = true
	dt.order = append(dt.order, key)

	for len(dt.processed) > dt.maxSize && len(dt.order) > 0 {
		oldest := dt.order[0]
		dt.order = dt.order[1:]
		delete(dt.processed, oldest)
	}
}

// FilterDuplicates removes duplicate records from a slice
func (dt *DeduplicationTracker) FilterDuplicates(config *Config, records []*FetchedRecord) []*FetchedRecord {
	filtered := make([]*FetchedRecord, 0, len(records))
	batchKeys := make(map[DeduplicationKey]bool, len(records))
	duplicateCount := 0

	for _, record := range records {
		operation := MapOperation(config, record.Message.Op)
		key := dt.MakeKey(record.Message, operation)

		if dt.IsDuplicate(key) || batchKeys[key] {
			duplicateCount++
			continue
		}

		batchKeys[key] = true
		filtered = append(filtered, record)
	}

	if duplicateCount > 0 {
		LogInfo(config, fmt.Sprintf("Filtered %d duplicate messages", duplicateCount))
	}

	return filtered
}

func (dt *DeduplicationTracker) MarkRecordsProcessed(config *Config, records []*FetchedRecord) {
	for _, record := range records {
		operation := MapOperation(config, record.Message.Op)
		dt.MarkProcessed(dt.MakeKey(record.Message, operation))
	}
}
