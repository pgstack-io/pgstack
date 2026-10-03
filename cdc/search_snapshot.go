package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
)

const SEARCH_SNAPSHOT_FLUSH_SIZE = 1000

func (r *ReplicationHandler) RunSearchSnapshot(ctx context.Context) error {
	if len(r.config.SearchTables) == 0 {
		return nil
	}

	conn, err := pgx.Connect(ctx, r.config.DatabaseURL)
	if err != nil {
		return fmt.Errorf("failed to connect for search snapshot: %w", err)
	}
	defer conn.Close(ctx)
	var snapshotLSNText string
	if err := conn.QueryRow(ctx, "SELECT pg_current_wal_lsn()::text").Scan(&snapshotLSNText); err != nil {
		return fmt.Errorf("failed to read search snapshot position: %w", err)
	}
	snapshotLSN, err := pglogrepl.ParseLSN(snapshotLSNText)
	if err != nil {
		return fmt.Errorf("failed to parse search snapshot position: %w", err)
	}
	tx, err := conn.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return fmt.Errorf("failed to begin search snapshot: %w", err)
	}
	defer tx.Rollback(ctx)
	snapshotTime := time.Now()

	for _, configuredTable := range r.config.SearchTables {
		schema, table, err := splitSchemaTable(configuredTable)
		if err != nil {
			return err
		}
		primaryKeys, err := queryPrimaryKey(ctx, tx, schema, table)
		if err != nil {
			return err
		}
		if len(primaryKeys) == 0 {
			return fmt.Errorf("search table %s must have a primary key", configuredTable)
		}

		rows, err := tx.Query(ctx, "SELECT to_jsonb(snapshot_row) FROM "+pgx.Identifier{schema, table}.Sanitize()+" AS snapshot_row")
		if err != nil {
			return fmt.Errorf("failed to snapshot %s: %w", configuredTable, err)
		}
		count := 0
		for rows.Next() {
			var raw []byte
			if err := rows.Scan(&raw); err != nil {
				rows.Close()
				return err
			}
			var after map[string]interface{}
			decoder := json.NewDecoder(bytes.NewReader(raw))
			decoder.UseNumber()
			if err := decoder.Decode(&after); err != nil {
				rows.Close()
				return err
			}
			message := &ChangeMessage{
				After:    after,
				Op:       OPERATION_CREATE,
				TsUs:     snapshotTime.UnixMicro(),
				Products: []string{PRODUCT_SEARCH},
				Source: SourceMetadata{
					TsUs:   snapshotTime.UnixMicro(),
					Db:     r.dbName,
					Schema: schema,
					Table:  table,
					Lsn:    uint64(snapshotLSN),
					Pk:     primaryKeys,
				},
			}
			if err := r.natsPublisher.PublishChange(message); err != nil {
				rows.Close()
				return err
			}
			count++
			if count%SEARCH_SNAPSHOT_FLUSH_SIZE == 0 {
				if err := r.natsPublisher.Flush(); err != nil {
					rows.Close()
					return err
				}
			}
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return err
		}
		rows.Close()
		// The processor normally holds an unpaired transaction tail briefly for a
		// possible context message. This marker closes the synthetic snapshot batch.
		if err := r.natsPublisher.PublishChange(&ChangeMessage{
			Op:       OPERATION_MESSAGE,
			TsUs:     snapshotTime.UnixMicro(),
			Products: []string{PRODUCT_SEARCH},
			Source: SourceMetadata{
				TsUs:   snapshotTime.UnixMicro(),
				Db:     r.dbName,
				Schema: schema,
				Table:  table,
				Lsn:    uint64(snapshotLSN),
			},
		}); err != nil {
			return err
		}
		if err := r.natsPublisher.Flush(); err != nil {
			return err
		}
		LogInfo(r.config, fmt.Sprintf("Published %d search snapshot rows for %s", count, configuredTable))
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit search snapshot: %w", err)
	}
	return nil
}

func splitSchemaTable(value string) (string, string, error) {
	for i := 0; i < len(value); i++ {
		if value[i] == '.' {
			if i == 0 || i == len(value)-1 {
				break
			}
			return value[:i], value[i+1:], nil
		}
	}
	return "", "", fmt.Errorf("invalid schema-qualified search table %q", value)
}
