package main

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/duckdb/duckdb-go/v2"
	"github.com/google/uuid"
	"github.com/xitongsys/parquet-go-source/s3"
	"github.com/xitongsys/parquet-go/reader"
	"github.com/xitongsys/parquet-go/source"
)

const TABLE_NAME = "changes"

// MapOperation converts Debezium operation codes to readable operation names
func MapOperation(config *Config, op string) string {
	switch op {
	case OPERATION_CREATE:
		return "CREATE"
	case OPERATION_UPDATE:
		return "UPDATE"
	case OPERATION_DELETE:
		return "DELETE"
	case OPERATION_TRUNCATE:
		return "TRUNCATE"
	case OPERATION_MESSAGE:
		return "MESSAGE"
	default:
		// Should never happen with valid cdc data
		LogWarn(config, fmt.Sprintf("Unknown operation code: %s", op))
		return op
	}
}

type ParquetFileStats struct {
	ColumnSizes     map[int]int64
	ValueCounts     map[int]int64
	NullValueCounts map[int]int64
	LowerBounds     map[int][]byte
	UpperBounds     map[int][]byte
	SplitOffsets    []int64
}

const COMMITTED_AT_FIELD_ID = 10

type ParquetFileInfo struct {
	Path        string
	Size        int64
	RecordCount int64
	Stats       ParquetFileStats
}

type IcebergMetadata struct {
	FormatVersion     int               `json:"format-version"`
	TableUUID         string            `json:"table-uuid"`
	Location          string            `json:"location"`
	LastUpdatedMs     int64             `json:"last-updated-ms"`
	LastColumnId      int               `json:"last-column-id"`
	CurrentSnapshotId int64             `json:"current-snapshot-id"`
	Snapshots         []IcebergSnapshot `json:"snapshots"`
	Schemas           []interface{}     `json:"schemas"`
}

type IcebergSnapshot struct {
	SnapshotId     int64  `json:"snapshot-id"`
	SequenceNumber int    `json:"sequence-number"`
	TimestampMs    int64  `json:"timestamp-ms"`
	ManifestList   string `json:"manifest-list"`
}

type IcebergWriter struct {
	config             *Config
	duckdb             *DuckDBClient
	s3Client           *S3Client
	tableS3Path        string
	dataS3Path         string
	metadataPath       string
	storageUtils       *StorageUtils
	cleanedOrphans     bool
	parquetFiles       []ParquetFileInfo
	parquetFilesLoaded bool
	filesPendingDelete []string
}

func NewIcebergWriter(config *Config, s3Client *S3Client) *IcebergWriter {
	tableS3Path := path.Join(config.AuditBasePath, TABLE_NAME)
	dataS3Path := path.Join(tableS3Path, "data")
	metadataPath := path.Join(tableS3Path, "metadata")

	return &IcebergWriter{
		config:       config,
		s3Client:     s3Client,
		tableS3Path:  tableS3Path,
		dataS3Path:   dataS3Path,
		metadataPath: metadataPath,
		storageUtils: NewStorageUtils(config, s3Client),
	}
}

func (w *IcebergWriter) SetDuckDB(duckdb *DuckDBClient) {
	w.duckdb = duckdb
}

func (w *IcebergWriter) AppendMessages(ctx context.Context, records []*FetchedRecord) error {
	if len(records) == 0 {
		return nil
	}
	if w.duckdb == nil {
		return errors.New("duckdb client is not set")
	}

	// Use background context for all database/storage operations
	// to ensure they complete even if parent context is cancelled
	bgCtx := context.Background()

	// Get existing parquet files
	existingParquetFiles, err := w.getExistingParquetFiles(bgCtx)
	if err != nil {
		return fmt.Errorf("failed to get existing files: %w", err)
	}
	sortParquetFilesInfo(existingParquetFiles)

	var filesToDelete []string

	if !w.cleanedOrphans {
		orphanFiles, err := w.orphanParquetFiles(bgCtx, existingParquetFiles)
		if err != nil {
			LogWarn(w.config, "Failed to find orphan parquet files:", err)
		} else {
			filesToDelete = append(filesToDelete, orphanFiles...)
			w.cleanedOrphans = true
		}
	}

	// Create temp table with new data
	tempTable := w.createTempTableName()
	if err := w.createAndPopulateTempTable(bgCtx, tempTable, records); err != nil {
		return err
	}
	defer w.dropTempTable(bgCtx, tempTable)

	var parquetFiles []ParquetFileInfo
	var operationDesc string

	if len(existingParquetFiles) > 0 {
		lastFile := existingParquetFiles[len(existingParquetFiles)-1]

		if lastFile.Size < w.config.MaxHotParquetFileSizeBytes() { // && lastFile.RecordCount < 1 when testing multiple Parquet files
			parquetFiles = existingParquetFiles[:len(existingParquetFiles)-1]
			newFile, err := w.mergeAndWriteParquet(bgCtx, tempTable, lastFile)
			if err != nil {
				return err
			}
			parquetFiles = append(parquetFiles, newFile)
			filesToDelete = append(filesToDelete, w.s3PathToKey(lastFile.Path))
			operationDesc = fmt.Sprintf("merged with last parquet file, parquet_files=%d", len(parquetFiles))
		} else {
			parquetFiles = existingParquetFiles
			newFile, err := w.writeNewParquet(bgCtx, tempTable)
			if err != nil {
				return err
			}
			parquetFiles = append(parquetFiles, newFile)
			operationDesc = fmt.Sprintf("created new parquet file, parquet_files=%d", len(parquetFiles))
		}
	} else {
		// First file
		newFile, err := w.writeNewParquet(bgCtx, tempTable)
		if err != nil {
			return err
		}
		parquetFiles = []ParquetFileInfo{newFile}
		operationDesc = "created first parquet file"
	}

	compactedParquetFiles, compactedFilesToDelete, err := w.compactHotParquetFiles(bgCtx, parquetFiles)
	if err != nil {
		return err
	}
	if len(compactedFilesToDelete) > 0 {
		parquetFiles = compactedParquetFiles
		filesToDelete = append(filesToDelete, compactedFilesToDelete...)
		operationDesc = fmt.Sprintf("%s, compacted_old_parquet_files=%d, parquet_files=%d", operationDesc, len(compactedFilesToDelete), len(parquetFiles))
	}
	sortParquetFilesInfo(parquetFiles)

	retainedParquetFiles, retentionFilesToDelete, err := w.dropOldestParquetFileOutsideRetention(parquetFiles)
	if err != nil {
		LogWarn(w.config, "Failed to apply retention period:", err)
	} else if len(retentionFilesToDelete) > 0 {
		parquetFiles = retainedParquetFiles
		filesToDelete = append(filesToDelete, retentionFilesToDelete...)
		operationDesc = fmt.Sprintf("%s, dropped_oldest_retained_parquet_file=1, parquet_files=%d", operationDesc, len(parquetFiles))
	}

	// Get old metadata files to delete
	oldMetadataFiles, err := w.getOldMetadataFiles(bgCtx)
	if err != nil {
		LogWarn(w.config, "Failed to get old metadata files:", err)
	} else {
		filesToDelete = append(filesToDelete, oldMetadataFiles...)
	}

	// Write Iceberg metadata
	if err := w.writeIcebergMetadata(parquetFiles); err != nil {
		return fmt.Errorf("failed to write Iceberg metadata: %w", err)
	}

	w.setCachedParquetFiles(parquetFiles)
	// Keep files from the previous snapshot available for one more successful
	// append so readers that already resolved that snapshot can finish.
	filesReadyToDelete := w.stageFilesForDeletion(filesToDelete)
	w.deleteFiles(bgCtx, filesReadyToDelete)

	LogInfo(w.config, fmt.Sprintf("Appended %d messages (%s)", len(records), operationDesc))
	return nil
}

func (w *IcebergWriter) writeIcebergMetadata(parquetFiles []ParquetFileInfo) error {
	metadataWriter := NewIcebergMetadataWriter(w.config, w.s3Client, w.tableS3Path, w.metadataPath)

	// Calculate total size
	var totalSize int64
	for _, pf := range parquetFiles {
		totalSize += pf.Size
	}

	// Write manifest (lists all parquet files)
	manifestFile, err := metadataWriter.WriteManifest(parquetFiles)
	if err != nil {
		return fmt.Errorf("failed to write manifest: %w", err)
	}

	// Write manifest list (references the manifest)
	sequenceNumber := len(parquetFiles) + 1
	manifestListFile, err := metadataWriter.WriteManifestList(manifestFile, sequenceNumber, totalSize)
	if err != nil {
		return fmt.Errorf("failed to write manifest list: %w", err)
	}

	// Write metadata.json (root Iceberg metadata)
	if err := metadataWriter.WriteMetadata(manifestListFile); err != nil {
		return fmt.Errorf("failed to write metadata: %w", err)
	}

	LogInfo(w.config, "Wrote Iceberg metadata: manifest, manifest-list, metadata.json")
	return nil
}

func (w *IcebergWriter) createAndPopulateTempTable(ctx context.Context, tableName string, records []*FetchedRecord) error {
	createSQL := fmt.Sprintf(`
		CREATE TABLE %s (
			id VARCHAR,
			database VARCHAR(255),
			schema VARCHAR(255),
			"table" VARCHAR(255),
			primary_key VARCHAR(255),
			operation VARCHAR(10),
			before JSON,
			"after" JSON,
			context JSON,
			committed_at TIMESTAMPTZ,
			queued_at TIMESTAMPTZ,
			created_at TIMESTAMPTZ,
			transaction_id BIGINT,
			position BIGINT
		)
	`, tableName)

	if _, err := w.duckdb.ExecContext(ctx, createSQL); err != nil {
		return err
	}

	conn, err := w.duckdb.db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("failed to get duckdb connection: %w", err)
	}
	defer conn.Close()

	return conn.Raw(func(driverConn interface{}) error {
		duckdbConn, ok := driverConn.(driver.Conn)
		if !ok {
			return fmt.Errorf("unexpected duckdb driver connection type %T", driverConn)
		}

		appender, err := duckdb.NewAppenderFromConn(duckdbConn, "", tableName)
		if err != nil {
			return fmt.Errorf("failed to create duckdb appender: %w", err)
		}

		for _, record := range records {
			row := w.messageToRow(record.Message)
			if err := appender.AppendRow(
				row.ID, row.Database, row.Schema, row.Table, nullableStringValue(row.PrimaryKey),
				row.Operation, nullableJSONValue(row.Before), nullableJSONValue(row.After), nullableJSONValue(row.Context),
				row.CommittedAt, row.QueuedAt, row.CreatedAt,
				row.TransactionID, row.Position,
			); err != nil {
				_ = appender.Close()
				return fmt.Errorf("failed to append row: %w", err)
			}
		}

		if err := appender.Close(); err != nil {
			return fmt.Errorf("failed to close duckdb appender: %w", err)
		}
		return nil
	})
}

func (w *IcebergWriter) messageToRow(msg *ChangeMessage) struct {
	ID            string
	Database      string
	Schema        string
	Table         string
	PrimaryKey    sql.NullString
	Operation     string
	Before        sql.NullString
	After         sql.NullString
	Context       sql.NullString
	CommittedAt   time.Time
	QueuedAt      time.Time
	CreatedAt     time.Time
	TransactionID int64
	Position      int64
} {
	id := uuid.New().String()
	now := time.Now()
	committedAt := time.UnixMicro(msg.Source.TsUs)
	queuedAt := time.UnixMicro(msg.TsUs)

	operation := MapOperation(w.config, msg.Op)

	var beforeJSON, afterJSON, contextJSON sql.NullString
	if msg.Before != nil {
		if data, err := json.Marshal(msg.Before); err == nil {
			beforeJSON = sql.NullString{String: string(data), Valid: true}
		}
	}
	if msg.After != nil {
		if data, err := json.Marshal(msg.After); err == nil {
			afterJSON = sql.NullString{String: string(data), Valid: true}
		}
	}
	if len(msg.Context) > 0 {
		if data, err := json.Marshal(msg.Context); err == nil {
			contextJSON = sql.NullString{String: string(data), Valid: true}
		}
	}

	var primaryKeyValue sql.NullString
	if len(msg.Source.Pk) > 0 {
		// Extract primary key values from After (for INSERT/UPDATE) or Before (for DELETE)
		data := msg.After
		if data == nil {
			data = msg.Before
		}

		if data != nil {
			var pkStrings []string
			for _, pkCol := range msg.Source.Pk {
				if val, ok := data[pkCol]; ok {
					pkStrings = append(pkStrings, formatPrimaryKeyValue(val))
				}
			}

			if len(pkStrings) > 0 {
				// Single PK: store as string without quotes, compound PK: store as JSON array
				if len(pkStrings) == 1 {
					primaryKeyValue = sql.NullString{String: pkStrings[0], Valid: true}
				} else {
					if pkData, err := json.Marshal(pkStrings); err == nil {
						primaryKeyValue = sql.NullString{String: string(pkData), Valid: true}
					}
				}
			}
		}
	}

	return struct {
		ID            string
		Database      string
		Schema        string
		Table         string
		PrimaryKey    sql.NullString
		Operation     string
		Before        sql.NullString
		After         sql.NullString
		Context       sql.NullString
		CommittedAt   time.Time
		QueuedAt      time.Time
		CreatedAt     time.Time
		TransactionID int64
		Position      int64
	}{
		ID:            id,
		Database:      msg.Source.Db,
		Schema:        msg.Source.Schema,
		Table:         msg.Source.Table,
		PrimaryKey:    primaryKeyValue,
		Operation:     operation,
		Before:        beforeJSON,
		After:         afterJSON,
		Context:       contextJSON,
		CommittedAt:   committedAt,
		QueuedAt:      queuedAt,
		CreatedAt:     now,
		TransactionID: int64(msg.Source.TxId),
		Position:      int64(msg.Source.Lsn),
	}
}

func formatPrimaryKeyValue(value interface{}) string {
	switch v := value.(type) {
	case nil:
		return "null"
	case json.Number:
		return v.String()
	case string:
		return v
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64)
	case float32:
		return strconv.FormatFloat(float64(v), 'f', -1, 32)
	case fmt.Stringer:
		return v.String()
	default:
		return fmt.Sprintf("%v", v)
	}
}

func nullableStringValue(value sql.NullString) interface{} {
	if !value.Valid {
		return nil
	}
	return value.String
}

func nullableJSONValue(value sql.NullString) interface{} {
	if !value.Valid {
		return nil
	}
	return json.RawMessage(value.String)
}

func (w *IcebergWriter) mergeAndWriteParquet(ctx context.Context, tempTable string, existingFile ParquetFileInfo) (ParquetFileInfo, error) {
	existingS3Path := existingFile.Path
	if !strings.HasPrefix(existingS3Path, "s3://") {
		existingS3Path = w.config.S3Path(existingS3Path)
	}

	mergedTable := w.createTempTableName()
	createSQL := fmt.Sprintf(`
		CREATE TABLE %s AS
		SELECT * FROM read_parquet('%s')
		UNION ALL
		SELECT * FROM %s
	`, mergedTable, existingS3Path, tempTable)

	if _, err := w.duckdb.ExecContext(ctx, createSQL); err != nil {
		return ParquetFileInfo{}, fmt.Errorf("failed to merge: %w", err)
	}
	defer w.dropTempTable(ctx, mergedTable)

	return w.writeParquetFromTable(ctx, mergedTable)
}

func (w *IcebergWriter) mergeParquetFiles(ctx context.Context, existingFiles []ParquetFileInfo, filenamePrefix string) (ParquetFileInfo, error) {
	if len(existingFiles) == 0 {
		return ParquetFileInfo{}, fmt.Errorf("no parquet files to merge")
	}

	mergedTable := w.createTempTableName()
	selects := make([]string, 0, len(existingFiles))
	for _, existingFile := range existingFiles {
		existingS3Path := existingFile.Path
		if !strings.HasPrefix(existingS3Path, "s3://") {
			existingS3Path = w.config.S3Path(existingS3Path)
		}
		selects = append(selects, fmt.Sprintf("SELECT * FROM read_parquet('%s')", existingS3Path))
	}

	createSQL := fmt.Sprintf(`
		CREATE TABLE %s AS
		%s
	`, mergedTable, strings.Join(selects, "\nUNION ALL\n"))

	if _, err := w.duckdb.ExecContext(ctx, createSQL); err != nil {
		return ParquetFileInfo{}, fmt.Errorf("failed to merge parquet files: %w", err)
	}
	defer w.dropTempTable(ctx, mergedTable)

	return w.writeParquetFromTableWithFilenamePrefix(ctx, mergedTable, filenamePrefix)
}

func (w *IcebergWriter) writeNewParquet(ctx context.Context, tempTable string) (ParquetFileInfo, error) {
	return w.writeParquetFromTable(ctx, tempTable)
}

func (w *IcebergWriter) ReadParquetStats(fileReader source.ParquetFile) (ParquetFileStats, error) {
	defer fileReader.Close()

	pr, err := reader.NewParquetReader(fileReader, nil, 1)
	if err != nil {
		return ParquetFileStats{}, fmt.Errorf("failed to create parquet reader: %w", err)
	}
	defer pr.ReadStop()

	stats := ParquetFileStats{
		ColumnSizes:     make(map[int]int64),
		ValueCounts:     make(map[int]int64),
		NullValueCounts: make(map[int]int64),
		LowerBounds:     make(map[int][]byte),
		UpperBounds:     make(map[int][]byte),
		SplitOffsets:    []int64{},
	}

	fieldIdByColumnName := map[string]int{
		"id":             1,
		"database":       2,
		"schema":         3,
		"table":          4,
		"primary_key":    5,
		"operation":      6,
		"before":         7,
		"after":          8,
		"context":        9,
		"committed_at":   COMMITTED_AT_FIELD_ID,
		"queued_at":      11,
		"created_at":     12,
		"transaction_id": 13,
		"position":       14,
	}

	stringColumns := map[string]bool{
		"id":          true,
		"database":    true,
		"schema":      true,
		"table":       true,
		"primary_key": true,
		"operation":   true,
		"before":      true,
		"after":       true,
		"context":     true,
	}
	int64Columns := map[string]bool{
		"committed_at":   true,
		"queued_at":      true,
		"created_at":     true,
		"transaction_id": true,
		"position":       true,
	}

	for _, rowGroup := range pr.Footer.RowGroups {
		if rowGroup.FileOffset != nil {
			stats.SplitOffsets = append(stats.SplitOffsets, *rowGroup.FileOffset)
		}

		for _, columnChunk := range rowGroup.Columns {
			columnMetaData := columnChunk.MetaData
			columnName := strings.ToLower(columnMetaData.PathInSchema[0])
			fieldId, ok := fieldIdByColumnName[columnName]
			if !ok {
				continue
			}
			stats.ColumnSizes[fieldId] += columnMetaData.TotalCompressedSize
			stats.ValueCounts[fieldId] += int64(columnMetaData.NumValues)
			if columnMetaData.Statistics != nil && columnMetaData.Statistics.NullCount != nil {
				stats.NullValueCounts[fieldId] += *columnMetaData.Statistics.NullCount
			} else {
				stats.NullValueCounts[fieldId] += 0
			}

			if columnMetaData.Statistics != nil {
				minValue := columnMetaData.Statistics.Min
				maxValue := columnMetaData.Statistics.Max

				isString := stringColumns[columnName]
				isInt64 := int64Columns[columnName]
				// Ignore empty values for non-string columns
				if (isString || len(minValue) > 0) && shouldReplaceLowerBound(stats.LowerBounds[fieldId], minValue, isInt64) {
					stats.LowerBounds[fieldId] = minValue
				}
				if (isString || len(maxValue) > 0) && shouldReplaceUpperBound(stats.UpperBounds[fieldId], maxValue, isInt64) {
					stats.UpperBounds[fieldId] = maxValue
				}
			}
		}
	}

	return stats, nil
}

func shouldReplaceLowerBound(current, candidate []byte, isInt64 bool) bool {
	if current == nil {
		return true
	}
	return compareParquetBound(current, candidate, isInt64) > 0
}

func shouldReplaceUpperBound(current, candidate []byte, isInt64 bool) bool {
	if current == nil {
		return true
	}
	return compareParquetBound(current, candidate, isInt64) < 0
}

func compareParquetBound(left, right []byte, isInt64 bool) int {
	if isInt64 && len(left) == 8 && len(right) == 8 {
		leftValue := int64(binary.LittleEndian.Uint64(left))
		rightValue := int64(binary.LittleEndian.Uint64(right))
		switch {
		case leftValue < rightValue:
			return -1
		case leftValue > rightValue:
			return 1
		default:
			return 0
		}
	}

	return bytes.Compare(left, right)
}

func (w *IcebergWriter) writeParquetFromTable(ctx context.Context, tableName string) (ParquetFileInfo, error) {
	return w.writeParquetFromTableWithFilenamePrefix(ctx, tableName, time.Now().Format("20060102-150405"))
}

func (w *IcebergWriter) writeParquetFromTableWithFilenamePrefix(ctx context.Context, tableName string, filenamePrefix string) (ParquetFileInfo, error) {
	filename := fmt.Sprintf("%s_%s.parquet", filenamePrefix, uuid.New().String()[:8])
	filePath := path.Join(w.dataS3Path, filename)
	s3Path := w.config.S3Path(filePath)

	fieldIds := []string{
		`"id": 1`,
		`"database": 2`,
		`"schema": 3`,
		`"table": 4`,
		`"primary_key": 5`,
		`"operation": 6`,
		`"before": 7`,
		`"after": 8`,
		`"context": 9`,
		fmt.Sprintf(`"committed_at": %d`, COMMITTED_AT_FIELD_ID),
		`"queued_at": 11`,
		`"created_at": 12`,
		`"transaction_id": 13`,
		`"position": 14`,
	}
	copySQL := fmt.Sprintf(`COPY %s TO '%s' (FORMAT parquet, COMPRESSION zstd, COMPRESSION_LEVEL 1, ROW_GROUP_SIZE 100000, FIELD_IDS {%s})`, tableName, s3Path, strings.Join(fieldIds, ", "))

	if _, err := w.duckdb.ExecContext(ctx, copySQL); err != nil {
		return ParquetFileInfo{}, fmt.Errorf("failed to write parquet: %w", err)
	}

	// Get row count by reading back the file
	var recordCount int64
	countSQL := fmt.Sprintf(`SELECT COUNT(*) FROM '%s'`, s3Path)

	err := w.duckdb.QueryRowContext(ctx, countSQL).Scan(&recordCount)
	if err != nil {
		return ParquetFileInfo{}, fmt.Errorf("failed to get record count: %w", err)
	}

	// Get file size from S3 metadata
	fileSize, err := w.s3Client.GetFileSize(ctx, filePath)
	if err != nil {
		return ParquetFileInfo{}, fmt.Errorf("failed to get file size: %w", err)
	}

	// Read parquet stats
	fileReader, err := s3.NewS3FileReaderWithClient(ctx, w.s3Client.client, w.config.S3Bucket, filePath)
	if err != nil {
		return ParquetFileInfo{}, fmt.Errorf("failed to create s3 file reader: %w", err)
	}
	parquetStats, err := w.ReadParquetStats(fileReader)
	if err != nil {
		return ParquetFileInfo{}, fmt.Errorf("failed to read parquet stats: %w", err)
	}

	// Format file size for readability
	var sizeStr string
	if fileSize >= 1024*1024 {
		sizeStr = fmt.Sprintf("%.2f MB", float64(fileSize)/(1024*1024))
	} else if fileSize >= 1024 {
		sizeStr = fmt.Sprintf("%.2f KB", float64(fileSize)/1024)
	} else {
		sizeStr = fmt.Sprintf("%d bytes", fileSize)
	}
	LogInfo(w.config, fmt.Sprintf("Wrote parquet file: %s | Records: %d | Size: %s", filename, recordCount, sizeStr))

	return ParquetFileInfo{
		Path:        w.config.S3Path(filePath),
		Size:        fileSize,
		RecordCount: recordCount,
		Stats:       parquetStats,
	}, nil
}

func (w *IcebergWriter) getLatestMetadataFile(ctx context.Context) (string, error) {
	return path.Join(w.metadataPath, ICEBERG_METADATA_FILE_NAME), nil
}

func (w *IcebergWriter) getExistingParquetFiles(ctx context.Context) ([]ParquetFileInfo, error) {
	if w.parquetFilesLoaded {
		return cloneParquetFilesInfo(w.parquetFiles), nil
	}

	metadataFile, err := w.getLatestMetadataFile(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get metadata: %w", err)
	}

	parquetFiles, err := w.storageUtils.CurrentParquetFiles(ctx, metadataFile)
	if errors.Is(err, ErrIcebergMetadataNotFound) {
		dataFiles, listErr := w.s3Client.ListFiles(ctx, w.dataS3Path)
		if listErr != nil {
			return nil, fmt.Errorf("failed to list data files after missing metadata: %w", listErr)
		}

		for _, file := range dataFiles {
			if strings.HasSuffix(file, ".parquet") {
				return nil, fmt.Errorf("iceberg metadata is missing but parquet data files exist under %s", w.dataS3Path)
			}
		}

		w.setCachedParquetFiles(nil)
		w.cleanedOrphans = true
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	w.setCachedParquetFiles(parquetFiles)
	return cloneParquetFilesInfo(w.parquetFiles), nil
}

func (w *IcebergWriter) setCachedParquetFiles(parquetFiles []ParquetFileInfo) {
	w.parquetFiles = cloneParquetFilesInfo(parquetFiles)
	w.parquetFilesLoaded = true
}

func cloneParquetFilesInfo(parquetFiles []ParquetFileInfo) []ParquetFileInfo {
	if parquetFiles == nil {
		return nil
	}
	cloned := make([]ParquetFileInfo, len(parquetFiles))
	copy(cloned, parquetFiles)
	return cloned
}

func sortParquetFilesInfo(parquetFiles []ParquetFileInfo) {
	sort.Slice(parquetFiles, func(i, j int) bool {
		return parquetFilename(parquetFiles[i].Path) < parquetFilename(parquetFiles[j].Path)
	})
}

func (w *IcebergWriter) dropOldestParquetFileOutsideRetention(parquetFiles []ParquetFileInfo) ([]ParquetFileInfo, []string, error) {
	if len(parquetFiles) == 0 {
		return parquetFiles, nil, nil
	}

	if w.config.RetentionDays == nil {
		return parquetFiles, nil, nil
	}

	retentionStartsAt := time.Now().UTC().AddDate(0, 0, -*w.config.RetentionDays)
	oldestFile := parquetFiles[0]
	maxCommittedAt, err := maxCommittedAtFromMetadata(oldestFile)
	if err != nil {
		return parquetFiles, nil, err
	}
	if !maxCommittedAt.Valid || !maxCommittedAt.Time.Before(retentionStartsAt) {
		return parquetFiles, nil, nil
	}

	LogInfo(w.config, fmt.Sprintf(
		"Attempting to drop parquet file outside retention: file=%s, max_committed_at=%s, retention_starts_at=%s",
		parquetFilename(oldestFile.Path),
		maxCommittedAt.Time.UTC().Format(time.RFC3339Nano),
		retentionStartsAt.Format(time.RFC3339Nano),
	))

	return parquetFiles[1:], []string{w.s3PathToKey(oldestFile.Path)}, nil
}

func maxCommittedAtFromMetadata(parquetFile ParquetFileInfo) (sql.NullTime, error) {
	upperBound, ok := parquetFile.Stats.UpperBounds[COMMITTED_AT_FIELD_ID]
	if !ok {
		return sql.NullTime{}, nil
	}
	if len(upperBound) != 8 {
		return sql.NullTime{}, fmt.Errorf(
			"invalid committed_at upper bound for %s: got %d bytes, want 8",
			parquetFilename(parquetFile.Path),
			len(upperBound),
		)
	}

	return sql.NullTime{
		Time:  time.UnixMicro(int64(binary.LittleEndian.Uint64(upperBound))).UTC(),
		Valid: true,
	}, nil
}

func (w *IcebergWriter) orphanParquetFiles(ctx context.Context, parquetFiles []ParquetFileInfo) ([]string, error) {
	files, err := w.s3Client.ListFiles(ctx, w.dataS3Path)
	if err != nil {
		return nil, err
	}

	liveKeys := make(map[string]struct{}, len(parquetFiles))
	for _, parquetFile := range parquetFiles {
		liveKeys[w.s3PathToKey(parquetFile.Path)] = struct{}{}
	}

	var orphanFiles []string
	for _, file := range files {
		if !strings.HasSuffix(file, ".parquet") {
			continue
		}

		fileKey := path.Join(w.dataS3Path, file)
		if _, ok := liveKeys[fileKey]; !ok {
			orphanFiles = append(orphanFiles, fileKey)
		}
	}

	if len(orphanFiles) > 0 {
		LogInfo(w.config, fmt.Sprintf("Found %d orphan parquet file(s) not referenced by current Iceberg metadata", len(orphanFiles)))
	}

	return orphanFiles, nil
}

func (w *IcebergWriter) createTempTableName() string {
	return "temp_" + strings.ReplaceAll(uuid.New().String(), "-", "")
}

func (w *IcebergWriter) dropTempTable(ctx context.Context, tableName string) {
	_, _ = w.duckdb.ExecContext(ctx, "DROP TABLE IF EXISTS "+tableName)
}

func (w *IcebergWriter) s3PathToKey(s3Path string) string {
	// Convert s3://bucket/path to path
	prefix := fmt.Sprintf("s3://%s/", w.config.S3Bucket)
	return strings.TrimPrefix(s3Path, prefix)
}

func (w *IcebergWriter) getOldMetadataFiles(ctx context.Context) ([]string, error) {
	files, err := w.s3Client.ListFiles(ctx, w.metadataPath)
	if err != nil {
		return nil, err
	}

	var keysToDelete []string
	for _, file := range files {
		// Delete old manifest and manifest-list files (avro files)
		if strings.HasSuffix(file, ".avro") {
			keysToDelete = append(keysToDelete, path.Join(w.metadataPath, file))
		}
	}

	return keysToDelete, nil
}

func (w *IcebergWriter) deleteFiles(ctx context.Context, keys []string) {
	if len(keys) == 0 {
		return
	}

	for _, key := range keys {
		var fileName string
		if strings.Contains(key, "/data/") {
			fileName = strings.Split(key, "/data/")[1]
		} else if strings.Contains(key, "/metadata/") {
			fileName = strings.Split(key, "/metadata/")[1]
		} else {
			fileName = key
		}

		LogInfo(w.config, fmt.Sprintf("Deleting old file: %s", fileName))
		if err := w.s3Client.Delete(ctx, key); err != nil {
			LogWarn(w.config, fmt.Sprintf("Failed to delete %s: %v", fileName, err))
		}
	}
}

func (w *IcebergWriter) stageFilesForDeletion(keys []string) []string {
	ready := uniqueStrings(w.filesPendingDelete)
	readySet := make(map[string]struct{}, len(ready))
	for _, key := range ready {
		readySet[key] = struct{}{}
	}

	nextSet := make(map[string]struct{}, len(keys))
	next := make([]string, 0, len(keys))
	for _, key := range keys {
		if _, beingDeleted := readySet[key]; beingDeleted {
			continue
		}
		if _, exists := nextSet[key]; exists {
			continue
		}
		nextSet[key] = struct{}{}
		next = append(next, key)
	}

	w.filesPendingDelete = next
	return ready
}

func uniqueStrings(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	unique := make([]string, 0, len(values))
	for _, value := range values {
		if _, exists := seen[value]; exists {
			continue
		}
		seen[value] = struct{}{}
		unique = append(unique, value)
	}
	return unique
}
