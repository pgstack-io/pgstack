package common

import (
	"context"

	"github.com/google/uuid"
)

type IcebergTable struct {
	Config             *CommonConfig
	IcebergSchemaTable IcebergSchemaTable
	IcebergCatalog     *IcebergCatalog
	StorageS3          *StorageS3
	DuckdbClient       *DuckdbClient
}

type CursorValue struct {
	ColumnName           string
	StringValue          string
	OverrideAppendedRows bool // Override rows that have the same value as this cursor value (if new rows are added with the same value, they will be included in the next sync)
}

func NewIcebergTable(config *CommonConfig, storageS3 *StorageS3, duckdbClient *DuckdbClient, icebergSchemaTable IcebergSchemaTable) *IcebergTable {
	return &IcebergTable{
		Config:             config,
		IcebergSchemaTable: icebergSchemaTable,
		IcebergCatalog:     NewIcebergCatalog(config),
		StorageS3:          storageS3,
		DuckdbClient:       duckdbClient,
	}
}

func (table *IcebergTable) String() string {
	return table.IcebergSchemaTable.String()
}

func (table *IcebergTable) MetadataFileS3Path() string {
	return table.IcebergCatalog.MetadataFileS3Path(table.IcebergSchemaTable)
}

func (table *IcebergTable) Create(tableS3Path string, icebergSchemaColumns []*IcebergSchemaColumn) {
	LogInfo(table.Config, "Creating Iceberg table:", table.IcebergSchemaTable.Table)
	table.IcebergCatalog.CreateTable(table.IcebergSchemaTable, tableS3Path+"/metadata/"+ICEBERG_METADATA_INITIAL_FILE_NAME, icebergSchemaColumns)
}

func (table *IcebergTable) ReplaceWith(callbackFunc func(syncingIcebergTable *IcebergTable)) {
	originalTableName := table.IcebergSchemaTable.Table

	// Delete -syncing table
	syncingIcebergSchemaTable := IcebergSchemaTable{Schema: table.IcebergSchemaTable.Schema, Table: originalTableName + TEMP_TABLE_SUFFIX_SYNCING}
	syncingIcebergTable := NewIcebergTable(table.Config, table.StorageS3, table.DuckdbClient, syncingIcebergSchemaTable)
	syncingIcebergTable.DropIfExists()

	// Insert into -syncing table
	callbackFunc(syncingIcebergTable)

	// Delete -deleting table
	deletingIcebergSchemaTable := IcebergSchemaTable{Schema: table.IcebergSchemaTable.Schema, Table: originalTableName + TEMP_TABLE_SUFFIX_DELETING}
	deletingIcebergTable := NewIcebergTable(table.Config, table.StorageS3, table.DuckdbClient, deletingIcebergSchemaTable)
	deletingIcebergTable.DropIfExists()

	// Rename table to -deleting
	table.Rename(deletingIcebergSchemaTable.Table)

	// Rename -syncing to table
	syncingIcebergTable.Rename(originalTableName)

	// Delete -deleting table
	deletingIcebergTable.DropIfExists()
}

func (table *IcebergTable) DropIfExists() {
	tableS3Path := table.IcebergCatalog.TableS3Path(table.IcebergSchemaTable)
	if tableS3Path == "" {
		return
	}

	LogInfo(table.Config, "Deleting Iceberg table:", table.IcebergSchemaTable.Table)
	table.StorageS3.DeleteTableFiles(tableS3Path)
	table.IcebergCatalog.DropTable(table.IcebergSchemaTable)
}

func (table *IcebergTable) Rename(newName string) {
	LogInfo(table.Config, "Renaming Iceberg table from", table.IcebergSchemaTable.Table, "to", newName)
	table.IcebergCatalog.RenameTable(table.IcebergSchemaTable, newName)
	table.IcebergSchemaTable.Table = newName
}

func (table *IcebergTable) GenerateTableS3Path() string {
	return "s3://" + table.Config.Aws.S3Bucket + "/iceberg/" + table.IcebergSchemaTable.Schema + "/" + table.IcebergSchemaTable.Table + "-" + uuid.New().String()
}

func (table *IcebergTable) LastCursorValue(columnName string) CursorValue {
	if columnName == "" {
		Panic(table.Config, "Couldn't find cursor column for table "+table.IcebergSchemaTable.Table)
	}

	metadataFileS3Path := table.MetadataFileS3Path()
	if metadataFileS3Path == "" {
		LogDebug(table.Config, "No S3 table path found for Iceberg table:", table.IcebergSchemaTable.Table)
		return CursorValue{ColumnName: columnName}
	}

	row := table.DuckdbClient.QueryRowContext(
		context.Background(),
		`SELECT COALESCE(CAST(max("`+columnName+`") AS VARCHAR), '') FROM iceberg_scan('`+metadataFileS3Path+`')`,
	)
	if row == nil {
		return CursorValue{ColumnName: columnName}
	}

	var lastCursorValue string
	err := row.Scan(&lastCursorValue)
	PanicIfError(table.Config, err)

	return CursorValue{ColumnName: columnName, StringValue: lastCursorValue}
}
