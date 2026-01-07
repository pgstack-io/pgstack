package common

import (
	"context"
	"database/sql/driver"
	"encoding/csv"
	"io"
	"sort"
	"strings"

	"github.com/google/uuid"
)

const (
	MAX_LOAD_BATCH_SIZE   = 1024 * 1024 * 1024 // 1 GB
	MAX_PARQUET_FILE_SIZE = 100 * 1024 * 1024  // 100 MB
)

type IcebergTableWriter struct {
	Config               *CommonConfig
	StorageS3            *StorageS3
	DuckdbClient         *DuckdbClient
	IcebergTable         *IcebergTable
	IcebergSchemaColumns []*IcebergSchemaColumn
	CompressionFactor    int64
}

func NewIcebergTableWriter(
	config *CommonConfig,
	storageS3 *StorageS3,
	duckdbClient *DuckdbClient,
	icebergTable *IcebergTable,
	icebergSchemaColumns []*IcebergSchemaColumn,
	compressionFactor int64,
) *IcebergTableWriter {
	return &IcebergTableWriter{
		Config:               config,
		StorageS3:            storageS3,
		DuckdbClient:         duckdbClient,
		IcebergTable:         icebergTable,
		IcebergSchemaColumns: icebergSchemaColumns,
		CompressionFactor:    compressionFactor,
	}
}

// Public functions ----------------------------------------------------------------------------------------------------

func (writer *IcebergTableWriter) InsertFromCsvCappedBuffer(cappedBuffer *CappedBuffer) {
	csvReader := csv.NewReader(cappedBuffer)
	_, err := csvReader.Read() // Read the header row
	PanicIfError(writer.Config, err)

	writer.insertRows(func(duckdbTableName string, loadedSize int64) (int64, bool) {
		return writer.loadCsvRows(duckdbTableName, csvReader, loadedSize)
	})
}

func (writer *IcebergTableWriter) AppendFromCsvCappedBuffer(cursorValue CursorValue, cappedBuffer *CappedBuffer) {
	metadataFileS3Path := writer.IcebergTable.MetadataFileS3Path()
	if metadataFileS3Path == "" { // If the table does not exist, insert for the first time
		writer.InsertFromCsvCappedBuffer(cappedBuffer)
		return
	}

	csvReader := csv.NewReader(cappedBuffer)
	_, err := csvReader.Read() // Read the header row
	PanicIfError(writer.Config, err)

	writer.appendRows(metadataFileS3Path, cursorValue, func(duckdbTableName string, loadedSize int64) (int64, bool) {
		return writer.loadCsvRows(duckdbTableName, csvReader, loadedSize)
	})
}

func (writer *IcebergTableWriter) InsertFromJsonCappedBuffer(cappedBuffer *CappedBuffer) {
	jsonQueueReader := NewJsonQueueReader(cappedBuffer)

	writer.insertRows(func(duckdbTableName string, loadedSize int64) (int64, bool) {
		return writer.loadJsonRows(duckdbTableName, jsonQueueReader, loadedSize)
	})
}

func (writer *IcebergTableWriter) AppendFromJsonCappedBuffer(cursorValue CursorValue, cappedBuffer *CappedBuffer) {
	metadataFileS3Path := writer.IcebergTable.MetadataFileS3Path()
	if metadataFileS3Path == "" { // If the table does not exist, insert for the first time
		writer.InsertFromJsonCappedBuffer(cappedBuffer)
		return
	}

	jsonQueueReader := NewJsonQueueReader(cappedBuffer)

	writer.appendRows(metadataFileS3Path, cursorValue, func(duckdbTableName string, loadedSize int64) (int64, bool) {
		return writer.loadJsonRows(duckdbTableName, jsonQueueReader, loadedSize)
	})
}

func (writer *IcebergTableWriter) UpdateFromJsonCappedBuffer(cappedBuffer *CappedBuffer) {
	metadataFileS3Path := writer.IcebergTable.MetadataFileS3Path()
	if metadataFileS3Path == "" { // If the table does not exist, insert for the first time
		writer.InsertFromJsonCappedBuffer(cappedBuffer)
		return
	}

	jsonQueueReader := NewJsonQueueReader(cappedBuffer)

	uniqueIndexColumnNames := writer.UniqueIndexColumnNames()
	writer.updateRows(metadataFileS3Path, uniqueIndexColumnNames, func(duckdbTableName string, loadedSize int64) (int64, bool) {
		return writer.loadJsonRows(duckdbTableName, jsonQueueReader, loadedSize)
	})
}

func (writer *IcebergTableWriter) DeleteFromJsonCappedBuffer(cappedBuffer *CappedBuffer) {
	metadataFileS3Path := writer.IcebergTable.MetadataFileS3Path()
	if metadataFileS3Path == "" { // If the table does not exist, do nothing
		return
	}

	jsonQueueReader := NewJsonQueueReader(cappedBuffer)

	uniqueIndexColumnNames := writer.UniqueIndexColumnNames()
	writer.deleteRows(metadataFileS3Path, uniqueIndexColumnNames, func(duckdbTableName string, loadedSize int64) (int64, bool) {
		return writer.loadJsonRows(duckdbTableName, jsonQueueReader, loadedSize)
	})
}

func (writer *IcebergTableWriter) UniqueIndexColumnNames() []string {
	uniqueIndexColumnNames := []string{}
	for _, icebergSchemaColumn := range writer.IcebergSchemaColumns {
		if icebergSchemaColumn.IsPartOfUniqueIndex {
			uniqueIndexColumnNames = append(uniqueIndexColumnNames, icebergSchemaColumn.ColumnName)
		}
	}
	return uniqueIndexColumnNames
}

func (writer *IcebergTableWriter) InsertFromQuery(query string) error {
	tableS3Path := writer.IcebergTable.GenerateTableS3Path()
	dataS3Path := tableS3Path + "/data"
	metadataS3Path := tableS3Path + "/metadata"

	tempDuckdbTableName := "temp_" + strings.ReplaceAll(uuid.New().String(), "-", "")

	loadedRowCount, icebergSchemaColumns, err := writer.insertToDuckdbTableFromQuery(tempDuckdbTableName, query)
	defer writer.deleteTempDuckdbTable(tempDuckdbTableName)
	if err != nil {
		return err
	}

	// Create parquet
	parquetFile := writer.StorageS3.CreateParquet(dataS3Path, writer.DuckdbClient, tempDuckdbTableName, icebergSchemaColumns, loadedRowCount)
	parquetFilesSortedAsc := []ParquetFile{parquetFile}
	totalDataFileSize := parquetFile.Size

	// Create manifest
	manifestFile := writer.StorageS3.CreateManifest(metadataS3Path, parquetFilesSortedAsc)

	// Create manifest list
	manifestListItem := ManifestListItem{SequenceNumber: len(parquetFilesSortedAsc) + 1, ManifestFile: manifestFile}
	manifestListFile := writer.StorageS3.CreateManifestList(metadataS3Path, totalDataFileSize, []ManifestListItem{manifestListItem})

	// Create metadata
	writer.StorageS3.CreateMetadata(metadataS3Path, icebergSchemaColumns, []ManifestListFile{manifestListFile})
	LogInfo(writer.Config, "Written", parquetFile.RecordCount, "records in Parquet file #"+IntToString(len(parquetFilesSortedAsc)), "("+writer.formattedParquetFileSize(parquetFile.Size)+")")

	// Create as table
	writer.IcebergTable.Create(tableS3Path, icebergSchemaColumns)

	return nil
}

// Iceberg logic -------------------------------------------------------------------------------------------------------

func (writer *IcebergTableWriter) insertRows(loadRowsToDuckdbTableFunc func(duckdbTableName string, loadedSize int64) (loadedRowCount int64, reachedEnd bool)) {
	tableS3Path := writer.IcebergTable.GenerateTableS3Path()
	dataS3Path := tableS3Path + "/data"
	metadataS3Path := tableS3Path + "/metadata"

	var totalDataFileSize int64
	parquetFilesSortedAsc := []ParquetFile{}
	objectsToDeleteKeys := []string{}

	var manifestFile ManifestFile
	var manifestListFile ManifestListFile
	for {
		tempDuckdbTableName := writer.createTempDuckdbTable()
		defer writer.deleteTempDuckdbTable(tempDuckdbTableName)

		loadedRowCount, reachedEnd := loadRowsToDuckdbTableFunc(tempDuckdbTableName, 0)
		if loadedRowCount == 0 && len(parquetFilesSortedAsc) > 0 {
			break
		}

		// Create parquet
		parquetFile := writer.StorageS3.CreateParquet(dataS3Path, writer.DuckdbClient, tempDuckdbTableName, writer.IcebergSchemaColumns, loadedRowCount)
		parquetFilesSortedAsc = append(parquetFilesSortedAsc, parquetFile)
		totalDataFileSize += parquetFile.Size

		// Replace manifest
		if manifestFile.Key != "" {
			objectsToDeleteKeys = append(objectsToDeleteKeys, manifestFile.Key)
		}
		manifestFile = writer.StorageS3.CreateManifest(metadataS3Path, parquetFilesSortedAsc)

		// Replace manifest list
		if manifestListFile.Key != "" {
			objectsToDeleteKeys = append(objectsToDeleteKeys, manifestListFile.Key)
		}
		manifestListItem := ManifestListItem{SequenceNumber: len(parquetFilesSortedAsc) + 1, ManifestFile: manifestFile}
		manifestListFile = writer.StorageS3.CreateManifestList(metadataS3Path, totalDataFileSize, []ManifestListItem{manifestListItem})

		// Create metadata
		writer.StorageS3.CreateMetadata(metadataS3Path, writer.IcebergSchemaColumns, []ManifestListFile{manifestListFile})
		LogInfo(writer.Config, "Written", parquetFile.RecordCount, "records in Parquet file #"+IntToString(len(parquetFilesSortedAsc)), "("+writer.formattedParquetFileSize(parquetFile.Size)+")")

		// Create table
		if len(parquetFilesSortedAsc) == 1 {
			writer.IcebergTable.Create(tableS3Path, writer.IcebergSchemaColumns)
		}

		// Delete old files
		writer.deleteObjects(objectsToDeleteKeys)
		objectsToDeleteKeys = []string{}

		if reachedEnd {
			break
		}
	}
}

func (writer *IcebergTableWriter) appendRows(metadataFileS3Path string, cursorValue CursorValue, loadRowsToDuckdbTableFunc func(duckdbTableName string, loadedSize int64) (loadedRowCount int64, reachedEnd bool)) {
	tableS3Path := strings.Split(metadataFileS3Path, "/metadata/")[0]
	metadataS3Path := tableS3Path + "/metadata"
	dataS3Path := tableS3Path + "/data"

	existingManifestListFile := writer.StorageS3.LastManifestListFile(metadataFileS3Path)
	existingManifestListItem := writer.StorageS3.ManifestListItems(existingManifestListFile)[0]
	existingParquetFilesSortedAsc := writer.StorageS3.ParquetFiles(existingManifestListItem.ManifestFile, writer.IcebergSchemaColumns)
	lastParquetFile := existingParquetFilesSortedAsc[len(existingParquetFilesSortedAsc)-1]
	replaceLastExistingParquetFile := lastParquetFile.Size < MAX_PARQUET_FILE_SIZE

	objectsToDeleteKeys := []string{}
	parquetFilesSortedAsc := existingParquetFilesSortedAsc
	if replaceLastExistingParquetFile {
		objectsToDeleteKeys = append(objectsToDeleteKeys, lastParquetFile.Key)
		parquetFilesSortedAsc = existingParquetFilesSortedAsc[:len(existingParquetFilesSortedAsc)-1] // Remove last file from the list
	}
	var totalDataFileSize int64
	for _, parquetFile := range parquetFilesSortedAsc {
		totalDataFileSize += parquetFile.Size
	}
	var newParquetFileCount int

	for {
		tempDuckdbTableName := writer.createTempDuckdbTable()
		defer writer.deleteTempDuckdbTable(tempDuckdbTableName)

		var initialLoadedSize int64
		var initialLoadedRowCount int64
		if newParquetFileCount == 0 && replaceLastExistingParquetFile {
			initialLoadedRowCount = writer.insertToDuckdbTableFromParquet(tempDuckdbTableName, lastParquetFile.Path, cursorValue)
			initialLoadedSize = lastParquetFile.Size * (MAX_LOAD_BATCH_SIZE * writer.CompressionFactor / MAX_PARQUET_FILE_SIZE)
		}

		loadedRowCount, reachedEnd := loadRowsToDuckdbTableFunc(tempDuckdbTableName, initialLoadedSize)
		if loadedRowCount == 0 {
			if newParquetFileCount == 0 {
				return // no rows to append in the first batch
			} else {
				break // no more rows to append
			}
		} else if loadedRowCount == 1 && cursorValue.OverrideAppendedRows && cursorValue.StringValue != "" && newParquetFileCount == 0 {
			return // exit if there is a single row to append with OverrideAppendedRows (to avoid overriding the pulled cursor value)
		}

		// Create parquet
		newParquetFile := writer.StorageS3.CreateParquet(dataS3Path, writer.DuckdbClient, tempDuckdbTableName, writer.IcebergSchemaColumns, initialLoadedRowCount+loadedRowCount)
		parquetFilesSortedAsc = append(parquetFilesSortedAsc, newParquetFile)
		newParquetFileCount++
		totalDataFileSize += newParquetFile.Size
		LogInfo(writer.Config, "Written", newParquetFile.RecordCount, "records in Parquet file #"+IntToString(newParquetFileCount), "("+writer.formattedParquetFileSize(newParquetFile.Size)+")")

		if reachedEnd {
			break
		}
	}

	// Replace manifest
	objectsToDeleteKeys = append(objectsToDeleteKeys, existingManifestListItem.ManifestFile.Key)
	manifestFile := writer.StorageS3.CreateManifest(metadataS3Path, parquetFilesSortedAsc)

	// Replace manifest list
	objectsToDeleteKeys = append(objectsToDeleteKeys, existingManifestListFile.Key)
	manifestListItem := ManifestListItem{SequenceNumber: len(parquetFilesSortedAsc) + 1, ManifestFile: manifestFile}
	manifestListFile := writer.StorageS3.CreateManifestList(metadataS3Path, totalDataFileSize, []ManifestListItem{manifestListItem})

	// Create metadata
	writer.StorageS3.CreateMetadata(metadataS3Path, writer.IcebergSchemaColumns, []ManifestListFile{manifestListFile})

	// Delete old files
	writer.deleteObjects(objectsToDeleteKeys)
}

func (writer *IcebergTableWriter) updateRows(metadataFileS3Path string, uniqueIndexColumnNames []string, loadRowsToDuckdbTableFunc func(duckdbTableName string, loadedSize int64) (loadedRowCount int64, reachedEnd bool)) {
	tableS3Path := strings.Split(metadataFileS3Path, "/metadata/")[0]
	metadataS3Path := tableS3Path + "/metadata"
	dataS3Path := tableS3Path + "/data"

	existingManifestListFile := writer.StorageS3.LastManifestListFile(metadataFileS3Path)
	existingManifestListItem := writer.StorageS3.ManifestListItems(existingManifestListFile)[0]
	existingParquetFilesSortedAsc := writer.StorageS3.ParquetFiles(existingManifestListItem.ManifestFile, writer.IcebergSchemaColumns)

	objectsToDeleteKeys := []string{}
	parquetFilesSortedAsc := existingParquetFilesSortedAsc
	for {
		tempUpdatedRowsDuckdbTableName := writer.createTempDuckdbTable()
		defer writer.deleteTempDuckdbTable(tempUpdatedRowsDuckdbTableName)

		loadedRowCount, reachedEnd := loadRowsToDuckdbTableFunc(tempUpdatedRowsDuckdbTableName, 0)
		if loadedRowCount == 0 {
			if len(objectsToDeleteKeys) == 0 {
				return // no rows to update in the first batch
			} else {
				break // no more rows to update
			}
		}

		for i, parquetFile := range parquetFilesSortedAsc {
			if !writer.hasOverlappingRowsInParquet(tempUpdatedRowsDuckdbTableName, parquetFile.Path, uniqueIndexColumnNames) {
				continue
			}

			tempDuckdbTableName := writer.createTempDuckdbTable()
			defer writer.deleteTempDuckdbTable(tempDuckdbTableName)

			var loadedRowCount int64
			loadedRowCount += writer.insertToDuckdbTableFromParquetWithoutOverlappingDuckdbTableRows(tempDuckdbTableName, tempUpdatedRowsDuckdbTableName, parquetFile.Path, uniqueIndexColumnNames)
			loadedRowCount += writer.insertToDuckdbTableFromDuckdbTableThatOverlapWithParquetRows(tempDuckdbTableName, tempUpdatedRowsDuckdbTableName, parquetFile.Path, uniqueIndexColumnNames)

			// Create parquet
			newParquetFile := writer.StorageS3.CreateParquet(dataS3Path, writer.DuckdbClient, tempDuckdbTableName, writer.IcebergSchemaColumns, loadedRowCount)
			LogInfo(writer.Config, "Written", newParquetFile.RecordCount, "records in 'updated' Parquet file ("+writer.formattedParquetFileSize(newParquetFile.Size)+")")

			// Replace the old Parquet file with the new one
			parquetFilesSortedAsc[i] = newParquetFile
			objectsToDeleteKeys = append(objectsToDeleteKeys, parquetFile.Key)
		}

		if reachedEnd {
			break
		}
	}

	if len(objectsToDeleteKeys) == 0 {
		return // no overlapping rows found
	}

	// Replace manifest
	objectsToDeleteKeys = append(objectsToDeleteKeys, existingManifestListItem.ManifestFile.Key)
	manifestFile := writer.StorageS3.CreateManifest(metadataS3Path, parquetFilesSortedAsc)

	// Replace manifest list
	objectsToDeleteKeys = append(objectsToDeleteKeys, existingManifestListFile.Key)
	var totalDataFileSize int64
	for _, parquetFile := range parquetFilesSortedAsc {
		totalDataFileSize += parquetFile.Size
	}
	manifestListItem := ManifestListItem{SequenceNumber: len(parquetFilesSortedAsc) + 1, ManifestFile: manifestFile}
	manifestListFile := writer.StorageS3.CreateManifestList(metadataS3Path, totalDataFileSize, []ManifestListItem{manifestListItem})

	// Create metadata
	writer.StorageS3.CreateMetadata(metadataS3Path, writer.IcebergSchemaColumns, []ManifestListFile{manifestListFile})

	// Delete old files
	writer.deleteObjects(objectsToDeleteKeys)
}

func (writer *IcebergTableWriter) deleteRows(metadataFileS3Path string, uniqueIndexColumnNames []string, loadRowsToDuckdbTableFunc func(duckdbTableName string, loadedSize int64) (loadedRowCount int64, reachedEnd bool)) {
	tableS3Path := strings.Split(metadataFileS3Path, "/metadata/")[0]
	metadataS3Path := tableS3Path + "/metadata"
	dataS3Path := tableS3Path + "/data"

	existingManifestListFile := writer.StorageS3.LastManifestListFile(metadataFileS3Path)
	existingManifestListItem := writer.StorageS3.ManifestListItems(existingManifestListFile)[0]
	existingParquetFilesSortedAsc := writer.StorageS3.ParquetFiles(existingManifestListItem.ManifestFile, writer.IcebergSchemaColumns)

	objectsToDeleteKeys := []string{}
	parquetFilesSortedAsc := existingParquetFilesSortedAsc
	for {
		tempDeletedRowsDuckdbTableName := writer.createTempDuckdbTable()
		defer writer.deleteTempDuckdbTable(tempDeletedRowsDuckdbTableName)

		loadedRowCount, reachedEnd := loadRowsToDuckdbTableFunc(tempDeletedRowsDuckdbTableName, 0)
		if loadedRowCount == 0 {
			if len(objectsToDeleteKeys) == 0 {
				return // no rows to delete in the first batch
			} else {
				break // no more rows to delete
			}
		}

		for i, parquetFile := range parquetFilesSortedAsc {
			if !writer.hasOverlappingRowsInParquet(tempDeletedRowsDuckdbTableName, parquetFile.Path, uniqueIndexColumnNames) {
				continue
			}

			tempDuckdbTableName := writer.createTempDuckdbTable()
			defer writer.deleteTempDuckdbTable(tempDuckdbTableName)

			rowCount := writer.insertToDuckdbTableFromParquetWithoutOverlappingDuckdbTableRows(tempDuckdbTableName, tempDeletedRowsDuckdbTableName, parquetFile.Path, uniqueIndexColumnNames)

			// Create parquet
			newParquetFile := writer.StorageS3.CreateParquet(dataS3Path, writer.DuckdbClient, tempDuckdbTableName, writer.IcebergSchemaColumns, rowCount)
			LogInfo(writer.Config, "Written", newParquetFile.RecordCount, "records in 'deleted' Parquet file ("+writer.formattedParquetFileSize(newParquetFile.Size)+")")

			// Replace the old Parquet file with the new one
			parquetFilesSortedAsc[i] = newParquetFile
			objectsToDeleteKeys = append(objectsToDeleteKeys, parquetFile.Key)
		}

		if reachedEnd {
			break
		}
	}

	if len(objectsToDeleteKeys) == 0 {
		return // no overlapping rows found
	}

	// Replace manifest
	objectsToDeleteKeys = append(objectsToDeleteKeys, existingManifestListItem.ManifestFile.Key)
	manifestFile := writer.StorageS3.CreateManifest(metadataS3Path, parquetFilesSortedAsc)

	// Replace manifest list
	objectsToDeleteKeys = append(objectsToDeleteKeys, existingManifestListFile.Key)
	var totalDataFileSize int64
	for _, parquetFile := range parquetFilesSortedAsc {
		totalDataFileSize += parquetFile.Size
	}
	manifestListItem := ManifestListItem{SequenceNumber: len(parquetFilesSortedAsc) + 1, ManifestFile: manifestFile}
	manifestListFile := writer.StorageS3.CreateManifestList(metadataS3Path, totalDataFileSize, []ManifestListItem{manifestListItem})

	// Create metadata
	writer.StorageS3.CreateMetadata(metadataS3Path, writer.IcebergSchemaColumns, []ManifestListFile{manifestListFile})

	// Delete old files
	writer.deleteObjects(objectsToDeleteKeys)
}

// DuckDB --------------------------------------------------------------------------------------------------------------

func (writer *IcebergTableWriter) createTempDuckdbTable() string {
	tableName := "temp_" + strings.ReplaceAll(uuid.New().String(), "-", "")
	columnSchemas := make([]string, len(writer.IcebergSchemaColumns))
	for i, col := range writer.IcebergSchemaColumns {
		columnSchemas[i] = col.QuotedColumnName() + " " + col.DuckdbType()
	}

	_, err := writer.DuckdbClient.ExecContext(context.Background(), "CREATE TABLE "+tableName+"("+strings.Join(columnSchemas, ",")+")")
	PanicIfError(writer.Config, err)

	return tableName
}

func (writer *IcebergTableWriter) hasOverlappingRowsInParquet(duckdbTableName string, parquetFileS3Path string, uniqueIndexColumnNames []string) bool {
	uniqueIndexConditions := make([]string, len(uniqueIndexColumnNames))
	for i, uniqueIndexColumnName := range uniqueIndexColumnNames {
		uniqueIndexConditions[i] = `"` + uniqueIndexColumnName + `" IN (SELECT "` + uniqueIndexColumnName + `" FROM ` + duckdbTableName + ")"
	}
	countSql := "SELECT COUNT(*) FROM read_parquet('" + parquetFileS3Path + "') WHERE " + strings.Join(uniqueIndexConditions, " AND ")

	var count int
	err := writer.DuckdbClient.QueryRowContext(context.Background(), countSql).Scan(&count)
	PanicIfError(writer.Config, err)
	return count > 0
}

func (writer *IcebergTableWriter) insertToDuckdbTableFromParquet(duckdbTableName string, parquetFileS3Path string, cursorValue CursorValue) int64 {
	sql := "INSERT INTO " + duckdbTableName + " SELECT * FROM read_parquet('" + parquetFileS3Path + "')"
	if cursorValue.OverrideAppendedRows {
		// Exclude rows with the cursor value
		sql += ` WHERE "` + cursorValue.ColumnName + `" != '` + cursorValue.StringValue + `'`
		LogDebug(writer.Config, "Replacing last existing Parquet file excluding cursor value:", strings.Split(parquetFileS3Path, "/data/")[1])
	} else {
		LogDebug(writer.Config, "Replacing last existing Parquet file:", strings.Split(parquetFileS3Path, "/data/")[1])
	}

	result, err := writer.DuckdbClient.ExecContext(context.Background(), sql)
	PanicIfError(writer.Config, err)

	rowsAffected, err := result.RowsAffected()
	PanicIfError(writer.Config, err)

	return rowsAffected
}

func (writer *IcebergTableWriter) insertToDuckdbTableFromDuckdbTableThatOverlapWithParquetRows(insertDuckdbTableName string, sourceDuckdbTableName string, parquetFileS3Path string, uniqueIndexColumnNames []string) int64 {
	uniqueIndexConditions := make([]string, len(uniqueIndexColumnNames))
	for i, uniqueIndexColumnName := range uniqueIndexColumnNames {
		uniqueIndexConditions[i] = `"` + uniqueIndexColumnName + `" IN (SELECT "` + uniqueIndexColumnName + `" FROM read_parquet('` + parquetFileS3Path + "'))"
	}
	sql := "INSERT INTO " + insertDuckdbTableName + " SELECT * FROM " + sourceDuckdbTableName + " WHERE " + strings.Join(uniqueIndexConditions, " AND ")
	result, err := writer.DuckdbClient.ExecContext(context.Background(), sql)
	PanicIfError(writer.Config, err)

	rowsAffected, err := result.RowsAffected()
	PanicIfError(writer.Config, err)

	return rowsAffected
}

func (writer *IcebergTableWriter) insertToDuckdbTableFromParquetWithoutOverlappingDuckdbTableRows(insertDuckdbTableName string, overlappingDuckdbTableName string, parquetFileS3Path string, uniqueIndexColumnNames []string) int64 {
	uniqueIndexConditions := make([]string, len(uniqueIndexColumnNames))
	for i, uniqueIndexColumnName := range uniqueIndexColumnNames {
		uniqueIndexConditions[i] = `"` + uniqueIndexColumnName + `" NOT IN (SELECT "` + uniqueIndexColumnName + `" FROM ` + overlappingDuckdbTableName + ")"
	}
	sql := "INSERT INTO " + insertDuckdbTableName + " SELECT * FROM read_parquet('" + parquetFileS3Path + "') WHERE " + strings.Join(uniqueIndexConditions, " AND ")
	result, err := writer.DuckdbClient.ExecContext(context.Background(), sql)
	PanicIfError(writer.Config, err)

	rowsAffected, err := result.RowsAffected()
	PanicIfError(writer.Config, err)

	return rowsAffected
}

func (writer *IcebergTableWriter) insertToDuckdbTableFromQuery(duckdbTableName string, query string) (int64, []*IcebergSchemaColumn, error) {
	ctx := context.Background()

	result, err := writer.DuckdbClient.ExecContext(ctx, "CREATE TABLE "+duckdbTableName+" AS "+query)
	if err != nil {
		return 0, nil, err
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return 0, nil, err
	}

	var icebergSchemaColumns []*IcebergSchemaColumn
	rows, err := writer.DuckdbClient.QueryContext(
		ctx,
		`SELECT
			columns.column_name,
			columns.data_type,
			columns.is_nullable,
			columns.ordinal_position,
			COALESCE(columns.numeric_precision, 0),
			COALESCE(columns.numeric_scale, 0),
			COALESCE(columns.datetime_precision, 0),
		FROM information_schema.columns
		WHERE columns.table_name = '`+duckdbTableName+`'`,
	)
	if err != nil {
		return 0, nil, err
	}
	defer rows.Close()

	for rows.Next() {
		duckdbSchemaColumn := &DuckdbSchemaColumn{Config: writer.Config}
		err := rows.Scan(
			&duckdbSchemaColumn.ColumnName,
			&duckdbSchemaColumn.DataType,
			&duckdbSchemaColumn.IsNullable,
			&duckdbSchemaColumn.OrdinalPosition,
			&duckdbSchemaColumn.NumericPrecision,
			&duckdbSchemaColumn.NumericScale,
			&duckdbSchemaColumn.DatetimePrecision,
		)
		if err != nil {
			return 0, nil, err
		}

		icebergSchemaColumns = append(icebergSchemaColumns, duckdbSchemaColumn.ToIcebergSchemaColumn())
	}

	return rowsAffected, icebergSchemaColumns, nil
}

func (writer *IcebergTableWriter) deleteTempDuckdbTable(duckdbTableName string) {
	_, err := writer.DuckdbClient.ExecContext(context.Background(), "DROP TABLE IF EXISTS "+duckdbTableName)
	PanicIfError(writer.Config, err)
}

func (writer *IcebergTableWriter) deleteObjects(keys []string) {
	for _, key := range keys {
		writer.deleteObject(key)
	}
}

func (writer *IcebergTableWriter) deleteObject(key string) {
	var fileName string
	if strings.Contains(key, "/data/") {
		fileName = strings.Split(key, "/data/")[1]
	} else if strings.Contains(key, "/metadata/") {
		fileName = strings.Split(key, "/metadata/")[1]
	}

	LogDebug(writer.Config, "Deleting object:", fileName)
	writer.StorageS3.S3Client.DeleteObject(key)
}

func (writer *IcebergTableWriter) loadCsvRows(duckdbTableName string, csvReader *csv.Reader, loadedSize int64) (loadedRowCount int64, reachedEnd bool) {
	appender, err := writer.DuckdbClient.Appender("", duckdbTableName)
	PanicIfError(writer.Config, err)
	defer appender.Close()

	for {
		row, err := csvReader.Read()
		if err == io.EOF {
			reachedEnd = true
			break
		}
		PanicIfError(writer.Config, err)

		duckdbRowValues := make([]driver.Value, len(writer.IcebergSchemaColumns))
		for i, icebergSchemaColumn := range writer.IcebergSchemaColumns {
			duckdbRowValues[i] = icebergSchemaColumn.DuckdbValueFromCsv(row[i])
			loadedSize += int64(len(row[i]))
		}

		LogTrace(writer.Config, "DuckDB appending row values:", duckdbRowValues)
		err = appender.AppendRow(duckdbRowValues...)
		PanicIfError(writer.Config, err)

		loadedRowCount++
		if loadedSize >= (MAX_LOAD_BATCH_SIZE * writer.CompressionFactor) {
			LogDebug(writer.Config, "Reached batch size limit")
			break
		}
	}

	LogInfo(writer.Config, "Loaded", loadedRowCount, "rows")
	return loadedRowCount, reachedEnd
}

func (writer *IcebergTableWriter) loadJsonRows(duckdbTableName string, jsonQueueReader *JsonQueueReader, loadedSize int64) (loadedRowCount int64, reachedEnd bool) {
	appender, err := writer.DuckdbClient.Appender("", duckdbTableName)
	PanicIfError(writer.Config, err)
	defer appender.Close()

	for {
		var rowValues map[string]interface{}
		valueSize, err := jsonQueueReader.Read(&rowValues)
		if err == io.EOF {
			reachedEnd = true
			break
		}
		PanicIfError(writer.Config, err)
		loadedSize += int64(valueSize)

		duckdbRowValues := writer.jsonToDuckdbRowValues(rowValues)

		LogTrace(writer.Config, "DuckDB appending row values:", rowValues)
		err = appender.AppendRow(duckdbRowValues...)
		PanicIfError(writer.Config, err)

		loadedRowCount++
		if loadedSize >= (MAX_LOAD_BATCH_SIZE * writer.CompressionFactor) {
			LogDebug(writer.Config, "Reached batch size limit")
			break
		}
	}

	LogInfo(writer.Config, "Loaded", loadedRowCount, "rows")
	return loadedRowCount, reachedEnd
}

func (writer *IcebergTableWriter) jsonToDuckdbRowValues(rowValues map[string]interface{}) []driver.Value {
	// Detect row column drift
	rowColumnNames := []string{}
	for columnName := range rowValues {
		rowColumnNames = append(rowColumnNames, columnName)
	}
	tableColumnNames := make([]string, len(writer.IcebergSchemaColumns))
	for i, icebergSchemaColumn := range writer.IcebergSchemaColumns {
		tableColumnNames[i] = icebergSchemaColumn.ColumnName
	}
	if len(rowColumnNames) != len(tableColumnNames) {
		Panic(writer.Config, "Row column names count doesn't match table column names count: "+strings.Join(rowColumnNames, ", ")+" (row) vs "+strings.Join(tableColumnNames, ", ")+" (table)")
	}
	sort.Strings(rowColumnNames)
	sort.Strings(tableColumnNames)
	for i := range rowColumnNames {
		if rowColumnNames[i] != tableColumnNames[i] {
			Panic(writer.Config, "Row column names don't match table column names: "+strings.Join(rowColumnNames, ", ")+" (row) vs "+strings.Join(tableColumnNames, ", ")+" (table)")
		}
	}

	// Convert row values to DuckDB values
	duckdbRowValues := make([]driver.Value, len(writer.IcebergSchemaColumns))
	for i, icebergSchemaColumn := range writer.IcebergSchemaColumns {
		columnName := icebergSchemaColumn.ColumnName
		value := rowValues[columnName]
		duckdbRowValues[i] = icebergSchemaColumn.DuckdbValueFromJson(value)
	}
	return duckdbRowValues
}

func (writer *IcebergTableWriter) formattedParquetFileSize(parquetFileSize int64) string {
	if parquetFileSize < 1024 {
		return Int64ToString(parquetFileSize) + "B"
	} else if parquetFileSize < 1024*1024 {
		return Int64ToString(parquetFileSize/1024) + "KB"
	} else if parquetFileSize < 1024*1024*1024 {
		return Int64ToString(parquetFileSize/(1024*1024)) + "MB"
	}
	return Int64ToString(parquetFileSize/(1024*1024*1024)) + "GB"
}
