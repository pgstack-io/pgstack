package main

import (
	"encoding/json"
	"fmt"
	"io"
	"path"
	"strings"
)

// IcebergReader reads Iceberg table metadata directly from S3
type IcebergReader struct {
	Config   *Config
	S3Client *S3Client
}

func NewIcebergReader(config *Config, s3Client *S3Client) *IcebergReader {
	return &IcebergReader{
		Config:   config,
		S3Client: s3Client,
	}
}

// SchemaTables discovers all tables by scanning S3 for metadata files
func (reader *IcebergReader) SchemaTables() (Set[IcebergSchemaTable], error) {
	schemaTables := NewSet[IcebergSchemaTable]()

	// List all folders in the iceberg base path (e.g., "audit/")
	// Each folder like "audit/changes" becomes "audit.changes"
	basePath := reader.Config.AuditBasePath
	if basePath == "" {
		basePath = "."
	}

	// List objects with the base path prefix
	objects := reader.S3Client.ListObjects(basePath + "/")

	// Track discovered table paths
	discoveredPaths := make(map[string]bool)

	for _, obj := range objects.Contents {
		key := *obj.Key

		// Look for metadata files: schema/table/metadata/v*.metadata.json
		if strings.Contains(key, "/metadata/") && strings.HasSuffix(key, ".metadata.json") {
			// Extract schema and table from path
			// Example: "audit/changes/metadata/v1.metadata.json" -> schema="audit", table="changes"
			relativePath := strings.TrimPrefix(key, basePath+"/")
			parts := strings.Split(relativePath, "/")

			var schema, table string
			if len(parts) >= 4 {
				schema = parts[0]
				table = parts[1]
			} else if len(parts) == 3 {
				table = parts[0]
				baseParts := strings.Split(strings.TrimSuffix(basePath, "/"), "/")
				if len(baseParts) > 0 && baseParts[len(baseParts)-1] != "" && baseParts[len(baseParts)-1] != "." {
					schema = baseParts[len(baseParts)-1]
				} else {
					schema = "public"
				}
			}

			if schema != "" && table != "" {
				tablePath := schema + "/" + table

				if !discoveredPaths[tablePath] {
					discoveredPaths[tablePath] = true
					schemaTables.Add(IcebergSchemaTable{
						Schema: schema,
						Table:  table,
					})
				}
			}
		}
	}

	return schemaTables, nil
}

// MetadataFileS3Path returns the path to the latest metadata file for a table
func (reader *IcebergReader) MetadataFileS3Path(icebergSchemaTable IcebergSchemaTable) string {
	basePath := reader.Config.AuditBasePath
	var tablePath string
	if basePath != "" {
		baseParts := strings.Split(strings.TrimSuffix(basePath, "/"), "/")
		lastBasePart := ""
		if len(baseParts) > 0 {
			lastBasePart = baseParts[len(baseParts)-1]
		}
		if lastBasePart == icebergSchemaTable.Schema {
			tablePath = path.Join(basePath, icebergSchemaTable.Table)
		} else {
			tablePath = path.Join(basePath, icebergSchemaTable.Schema, icebergSchemaTable.Table)
		}
	} else {
		tablePath = path.Join(icebergSchemaTable.Schema, icebergSchemaTable.Table)
	}
	metadataPath := tablePath + "/metadata"

	// List metadata files
	objects := reader.S3Client.ListObjects(metadataPath + "/")

	// Find the latest v*.metadata.json file
	var latestVersion int
	var latestFile string

	for _, obj := range objects.Contents {
		key := *obj.Key
		if strings.HasSuffix(key, ".metadata.json") {
			// Extract version number from filename like "v1.metadata.json"
			filename := path.Base(key)
			var version int
			if _, err := fmt.Sscanf(filename, "v%d.metadata.json", &version); err == nil {
				if version > latestVersion {
					latestVersion = version
					latestFile = key
				}
			}
		}
	}

	if latestFile == "" {
		// Fallback to v1.metadata.json
		return tablePath + "/metadata/v1.metadata.json"
	}

	return latestFile
}

// TableColumns reads column information from the Iceberg metadata file
func (reader *IcebergReader) TableColumns(icebergSchemaTable IcebergSchemaTable) ([]IcebergSchemaColumn, error) {
	metadataPath := reader.MetadataFileS3Path(icebergSchemaTable)
	if metadataPath == "" {
		return nil, fmt.Errorf("metadata file not found for table %s.%s", icebergSchemaTable.Schema, icebergSchemaTable.Table)
	}

	// Read metadata file from S3
	s3Key := reader.S3Client.ObjectKey(metadataPath)
	getObjectOutput := reader.S3Client.GetObject(s3Key)
	defer getObjectOutput.Body.Close()

	contentBytes, err := io.ReadAll(getObjectOutput.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read metadata file: %w", err)
	}
	content := string(contentBytes)

	// Parse Iceberg metadata JSON
	var metadata struct {
		Schemas []struct {
			Fields []struct {
				ID       int         `json:"id"`
				Name     string      `json:"name"`
				Required bool        `json:"required"`
				Type     interface{} `json:"type"`
			} `json:"fields"`
		} `json:"schemas"`
	}

	if err := json.Unmarshal([]byte(content), &metadata); err != nil {
		return nil, fmt.Errorf("failed to parse metadata JSON: %w", err)
	}

	// Use the last (current) schema
	if len(metadata.Schemas) == 0 {
		return nil, fmt.Errorf("no schemas found in metadata")
	}

	currentSchema := metadata.Schemas[len(metadata.Schemas)-1]
	columns := make([]IcebergSchemaColumn, 0, len(currentSchema.Fields))

	for _, field := range currentSchema.Fields {
		columns = append(columns, IcebergSchemaColumn{
			Name:     field.Name,
			Type:     reader.icebergTypeToString(field.Type),
			Required: field.Required,
		})
	}

	return columns, nil
}

// icebergTypeToString converts Iceberg type to string representation
func (reader *IcebergReader) icebergTypeToString(icebergType interface{}) string {
	switch t := icebergType.(type) {
	case string:
		return t
	case map[string]interface{}:
		if typeStr, ok := t["type"].(string); ok {
			return typeStr
		}
	}
	return fmt.Sprintf("%v", icebergType)
}
