package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

const (
	MAX_CHANGE_SIZE_BYTES          = 2097152 // 2MB
	MAX_LOGICAL_MESSAGE_SIZE_BYTES = 10240   // 10KB
	APPLICATION_NAME               = "PgStack"
	ACTIVE_SLOT_TERMINATION_WAIT   = 5 * time.Second
	TRANSACTION_STANDBY_TIMEOUT    = 30 * time.Second
	PGOUTPUT_PLUGIN                = "pgoutput"
	PG_DUPLICATE_OBJECT_CODE       = "42710"
	REPLICATION_SLOT_WAL_LOST      = "lost"
	UNCHANGED_TOAST                = "UNCHANGED_TOAST"
	MESSAGE_PREFIX_CONTEXT         = "_bemi"
)

type ReplicationHandler struct {
	conn           *pgconn.PgConn
	config         *Config
	relations      map[uint32]*pglogrepl.RelationMessage
	primaryKeys    map[uint32][]string
	typeMap        *TypeMap
	txnBuffer      *TransactionBuffer
	lastWrittenLSN pglogrepl.LSN
	standbyTimeout time.Duration
	natsPublisher  *NatsPublisher
	dbName         string
}

func NewReplicationHandler(config *Config) (*ReplicationHandler, error) {
	connConfig, err := pgconn.ParseConfig(config.DatabaseURL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse database URL: %w", err)
	}

	typeMap, err := LoadTypeMap(context.Background(), config.DatabaseURL)
	if err != nil {
		return nil, fmt.Errorf("failed to load type map: %w", err)
	}

	natsPublisher, err := NewNatsPublisher(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create NATS publisher: %w", err)
	}

	return &ReplicationHandler{
		config:         config,
		relations:      make(map[uint32]*pglogrepl.RelationMessage),
		primaryKeys:    make(map[uint32][]string),
		typeMap:        typeMap,
		txnBuffer:      NewTransactionBuffer(),
		standbyTimeout: time.Second * 10,
		natsPublisher:  natsPublisher,
		dbName:         connConfig.Database,
	}, nil
}

func (r *ReplicationHandler) Start(ctx context.Context) error {
	if _, err := r.ensureReplicationSlot(ctx); err != nil {
		return fmt.Errorf("failed to ensure replication slot: %w", err)
	}
	if r.config.SearchSnapshot {
		if err := r.RunSearchSnapshot(ctx); err != nil {
			return fmt.Errorf("failed to run search snapshot: %w", err)
		}
	}
	if err := r.terminateActiveReplicationSlotConnection(ctx); err != nil {
		return fmt.Errorf("failed to terminate active replication slot connection: %w", err)
	}
	if err := r.recreateLostReplicationSlot(ctx); err != nil {
		return fmt.Errorf("failed to recover replication slot: %w", err)
	}

	pluginArguments := []string{
		"proto_version '1'",
		fmt.Sprintf("publication_names '%s'", r.config.PublicationName),
		"messages 'true'",
	}

	if err := r.connectReplication(ctx); err != nil {
		return fmt.Errorf("failed to connect for replication: %w", err)
	}

	err := pglogrepl.StartReplication(
		ctx,
		r.conn,
		r.config.SlotName,
		0,
		pglogrepl.StartReplicationOptions{
			PluginArgs: pluginArguments,
		},
	)
	if err != nil {
		return fmt.Errorf("failed to start replication: %w", err)
	}

	LogInfo(r.config, "Logical replication started from slot:", r.config.SlotName)

	standbyMessageTicker := time.NewTicker(r.standbyTimeout)
	defer standbyMessageTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-standbyMessageTicker.C:
			if err := r.sendStandbyStatusUpdate(ctx); err != nil {
				return fmt.Errorf("failed to send standby status: %w", err)
			}
		default:
			if err := r.receiveMessage(ctx); err != nil {
				return err
			}
		}
	}
}

func connectReplication(ctx context.Context, databaseURL string) (*pgconn.PgConn, error) {
	connConfig, err := pgconn.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse database URL: %w", err)
	}
	connConfig.RuntimeParams["replication"] = "database"
	connConfig.RuntimeParams["application_name"] = APPLICATION_NAME

	conn, err := pgconn.ConnectConfig(ctx, connConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}

	return conn, nil
}

func (r *ReplicationHandler) connectReplication(ctx context.Context) error {
	if r.conn != nil {
		if err := r.conn.Close(ctx); err != nil {
			LogWarn(r.config, "Failed to close old replication connection:", err)
		}
		r.conn = nil
	}

	conn, err := connectReplication(ctx, r.config.DatabaseURL)
	if err != nil {
		return err
	}

	r.conn = conn
	return nil
}

func (r *ReplicationHandler) ensureReplicationSlot(ctx context.Context) (pglogrepl.LSN, error) {
	conn, err := connectReplication(ctx, r.config.DatabaseURL)
	if err != nil {
		return 0, err
	}
	defer conn.Close(ctx)

	result, err := pglogrepl.CreateReplicationSlot(ctx, conn, r.config.SlotName, PGOUTPUT_PLUGIN, pglogrepl.CreateReplicationSlotOptions{})
	if err != nil {
		if isDuplicateReplicationSlotError(err) {
			LogInfo(r.config, "Replication slot already exists:", r.config.SlotName)
			return 0, nil
		}
		return 0, err
	}

	lsn, err := pglogrepl.ParseLSN(result.ConsistentPoint)
	if err != nil {
		return 0, fmt.Errorf("failed to parse replication slot consistent point %q: %w", result.ConsistentPoint, err)
	}

	LogInfo(r.config, fmt.Sprintf("Created replication slot %s at %s", result.SlotName, result.ConsistentPoint))
	return lsn, nil
}

func (r *ReplicationHandler) recreateLostReplicationSlot(ctx context.Context) error {
	conn, err := pgx.Connect(ctx, r.config.DatabaseURL)
	if err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}
	defer conn.Close(ctx)

	var walStatus *string
	if err := conn.QueryRow(ctx, `
		SELECT wal_status
		FROM pg_replication_slots
		WHERE slot_name = $1
	`, r.config.SlotName).Scan(&walStatus); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		return fmt.Errorf("failed to query replication slot status: %w", err)
	}

	if walStatus == nil || *walStatus != REPLICATION_SLOT_WAL_LOST {
		return nil
	}

	LogWarn(r.config, fmt.Sprintf(
		"Replication slot %s is lost; recreating it at the current WAL position",
		r.config.SlotName,
	))

	if _, err := conn.Exec(ctx, `SELECT pg_drop_replication_slot($1)`, r.config.SlotName); err != nil {
		return fmt.Errorf("failed to drop lost replication slot: %w", err)
	}
	if _, err := r.ensureReplicationSlot(ctx); err != nil {
		return fmt.Errorf("failed to recreate lost replication slot: %w", err)
	}

	return nil
}

func (r *ReplicationHandler) terminateActiveReplicationSlotConnection(ctx context.Context) error {
	conn, err := pgx.Connect(ctx, r.config.DatabaseURL)
	if err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}
	defer conn.Close(ctx)

	activePID, err := replicationSlotActivePID(ctx, conn, r.config.SlotName)
	if err != nil {
		return err
	}
	if activePID == nil {
		return nil
	}

	var terminated bool
	if err := conn.QueryRow(ctx, `SELECT pg_terminate_backend($1)`, *activePID).Scan(&terminated); err != nil {
		return fmt.Errorf("failed to terminate backend %d: %w", *activePID, err)
	}
	if !terminated {
		return fmt.Errorf("postgres did not terminate backend %d", *activePID)
	}

	LogInfo(r.config, fmt.Sprintf("Terminated backend %d using replication slot %s", *activePID, r.config.SlotName))

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(ACTIVE_SLOT_TERMINATION_WAIT):
		return nil
	}
}

func replicationSlotActivePID(ctx context.Context, conn *pgx.Conn, slotName string) (*int32, error) {
	var activePID *int32
	if err := conn.QueryRow(ctx, `
		SELECT active_pid
		FROM pg_replication_slots
		WHERE slot_name = $1
	`, slotName).Scan(&activePID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to query active PID for replication slot: %w", err)
	}

	return activePID, nil
}

func isDuplicateReplicationSlotError(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == PG_DUPLICATE_OBJECT_CODE
}

func (r *ReplicationHandler) receiveMessage(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, time.Second*1)
	defer cancel()

	msg, err := r.conn.ReceiveMessage(ctx)
	if err != nil {
		if pgconn.Timeout(err) {
			return nil
		}
		return fmt.Errorf("failed to receive message: %w", err)
	}

	switch msg := msg.(type) {
	case *pgproto3.CopyData:
		return r.handleCopyData(msg.Data)
	default:
		LogError(r.config, fmt.Sprintf("Received unexpected message: %T", msg))
	}

	return nil
}

func (r *ReplicationHandler) handleCopyData(data []byte) error {
	if data[0] == pglogrepl.PrimaryKeepaliveMessageByteID {
		return r.handlePrimaryKeepalive(data[1:])
	}

	if data[0] != pglogrepl.XLogDataByteID {
		return nil
	}

	xld, err := pglogrepl.ParseXLogData(data[1:])
	if err != nil {
		return fmt.Errorf("failed to parse XLogData: %w", err)
	}

	if err := r.handleWALData(xld.WALData, uint64(xld.WALStart)); err != nil {
		return err
	}

	// Only acknowledge WAL after handling it successfully. In particular, a
	// transaction commit is not acknowledged until every JetStream PubAck has
	// been received.
	r.lastWrittenLSN = xld.WALStart + pglogrepl.LSN(len(xld.WALData))
	return nil
}

func (r *ReplicationHandler) handleWALData(walData []byte, walStart uint64) error {
	logicalMsg, err := pglogrepl.Parse(walData)
	if err != nil {
		return fmt.Errorf("failed to parse logical replication message: %w", err)
	}

	switch msg := logicalMsg.(type) {
	case *pglogrepl.RelationMessage:
		r.relations[msg.RelationID] = msg
		r.extractPrimaryKey(msg)

	case *pglogrepl.BeginMessage:
		r.txnBuffer.Begin(msg)

	case *pglogrepl.InsertMessage:
		r.handleInsert(msg, walStart)

	case *pglogrepl.UpdateMessage:
		r.handleUpdate(msg, walStart)

	case *pglogrepl.DeleteMessage:
		r.handleDelete(msg, walStart)

	case *pglogrepl.CommitMessage:
		r.txnBuffer.Commit(msg)
		if err := r.flushTransaction(); err != nil {
			return err
		}

	case *pglogrepl.TruncateMessage:
		r.handleTruncate(msg, walStart)

	case *pglogrepl.TypeMessage:
		LogInfo(r.config, fmt.Sprintf("Type message: %+v", msg))

	case *pglogrepl.OriginMessage:
		LogInfo(r.config, fmt.Sprintf("Origin message: %+v", msg))

	case *pglogrepl.LogicalDecodingMessage:
		r.handleLogicalMessage(msg, walStart)
	}

	return nil
}

func (r *ReplicationHandler) handleInsert(msg *pglogrepl.InsertMessage, walStart uint64) {
	rel, ok := r.relations[msg.RelationID]
	if !ok {
		LogError(r.config, "Unknown relation ID:", msg.RelationID)
		return
	}

	if !r.config.ShouldProcessTable(rel.Namespace, rel.RelationName) {
		return
	}

	values := r.decodeRow(rel, msg.Tuple)
	change := Change{
		Operation: "INSERT",
		Schema:    rel.Namespace,
		Table:     rel.RelationName,
		Position:  walStart,
		After:     values,
	}

	if r.isChangeTooLarge(change) {
		return
	}

	r.txnBuffer.AddChange(change)
}

func (r *ReplicationHandler) handleUpdate(msg *pglogrepl.UpdateMessage, walStart uint64) {
	rel, ok := r.relations[msg.RelationID]
	if !ok {
		LogError(r.config, "Unknown relation ID:", msg.RelationID)
		return
	}

	if !r.config.ShouldProcessTable(rel.Namespace, rel.RelationName) {
		return
	}

	var before map[string]interface{}
	if msg.OldTuple != nil {
		before = r.decodeRow(rel, msg.OldTuple)
	}

	after := r.decodeRow(rel, msg.NewTuple)
	resolveUnchangedToastFromBefore(msg.OldTupleType, before, after)

	// Check if only ignored columns have changed
	if r.shouldSkipUpdate(rel.Namespace, rel.RelationName, before, after) && !r.config.ShouldSearchTable(rel.Namespace, rel.RelationName) {
		LogInfo(r.config, "Skipping no-diff changes in", rel.Namespace+"."+rel.RelationName)
		return
	}

	change := Change{
		Operation: "UPDATE",
		Schema:    rel.Namespace,
		Table:     rel.RelationName,
		Position:  walStart,
		Before:    before,
		After:     after,
	}

	if r.isChangeTooLarge(change) {
		return
	}

	r.txnBuffer.AddChange(change)
}

func (r *ReplicationHandler) handleDelete(msg *pglogrepl.DeleteMessage, walStart uint64) {
	rel, ok := r.relations[msg.RelationID]
	if !ok {
		LogError(r.config, "Unknown relation ID:", msg.RelationID)
		return
	}

	if !r.config.ShouldProcessTable(rel.Namespace, rel.RelationName) {
		return
	}

	var before map[string]interface{}
	if msg.OldTuple != nil {
		before = r.decodeRow(rel, msg.OldTuple)
	}

	change := Change{
		Operation: "DELETE",
		Schema:    rel.Namespace,
		Table:     rel.RelationName,
		Position:  walStart,
		Before:    before,
	}

	if r.isChangeTooLarge(change) {
		return
	}

	r.txnBuffer.AddChange(change)
}

func (r *ReplicationHandler) handleTruncate(msg *pglogrepl.TruncateMessage, walStart uint64) {
	for _, relID := range msg.RelationIDs {
		rel, ok := r.relations[relID]
		if !ok {
			continue
		}

		if !r.config.ShouldProcessTable(rel.Namespace, rel.RelationName) {
			continue
		}

		change := Change{
			Operation: "TRUNCATE",
			Schema:    rel.Namespace,
			Table:     rel.RelationName,
			Position:  walStart,
		}

		if r.isChangeTooLarge(change) {
			continue
		}

		r.txnBuffer.AddChange(change)
	}
}

func (r *ReplicationHandler) handleLogicalMessage(msg *pglogrepl.LogicalDecodingMessage, walStart uint64) {
	lm := LogicalMessage{
		Prefix:        msg.Prefix,
		Content:       append([]byte(nil), msg.Content...),
		Transactional: msg.Transactional,
		Position:      walStart,
	}

	if r.shouldIgnoreLogicalMessage(lm) {
		return
	}

	r.txnBuffer.AddLogicalMessage(lm)
}

func (r *ReplicationHandler) flushTransaction() error {
	txn := r.txnBuffer.GetTransaction()
	if txn == nil {
		return nil
	}

	nextStandbyStatus := time.Now().Add(TRANSACTION_STANDBY_TIMEOUT)
	sendStandbyStatusIfDue := func() error {
		if time.Now().Before(nextStandbyStatus) {
			return nil
		}
		if err := r.sendStandbyStatusUpdate(context.Background()); err != nil {
			return err
		}
		nextStandbyStatus = time.Now().Add(TRANSACTION_STANDBY_TIMEOUT)
		return nil
	}

	for _, change := range txn.Changes {
		if err := sendStandbyStatusIfDue(); err != nil {
			return fmt.Errorf("failed to send standby status while flushing transaction %d: %w", txn.XID, err)
		}

		LogDebug(r.config, fmt.Sprintf("[%s] %s.%s", change.Operation, change.Schema, change.Table))

		// Publish to NATS
		primaryKey := r.getPrimaryKeyForChange(change)
		msg := CreateChangeMessage(change, txn, change.Position, r.dbName, primaryKey)
		if r.config.ShouldAuditTable(change.Schema, change.Table) &&
			(change.Operation != "UPDATE" || !r.shouldSkipUpdate(change.Schema, change.Table, change.Before, change.After)) {
			msg.Products = append(msg.Products, PRODUCT_AUDIT)
		}
		if r.config.ShouldSearchTable(change.Schema, change.Table) {
			msg.Products = append(msg.Products, PRODUCT_SEARCH)
		}
		if len(msg.Products) == 0 {
			continue
		}
		if err := r.natsPublisher.PublishChange(msg); err != nil {
			return fmt.Errorf("failed to publish transaction %d change at LSN %d: %w", txn.XID, change.Position, err)
		}
	}

	for _, lm := range txn.LogicalMessages {
		if err := sendStandbyStatusIfDue(); err != nil {
			return fmt.Errorf("failed to send standby status while flushing transaction %d: %w", txn.XID, err)
		}

		LogDebug(r.config, "[MESSAGE] Prefix:", lm.Prefix)

		// Publish to NATS
		msg := CreateLogicalMessage(lm, txn, lm.Position, r.dbName)
		if r.config.AuditConfigured {
			msg.Products = []string{PRODUCT_AUDIT}
			if err := r.natsPublisher.PublishChange(msg); err != nil {
				return fmt.Errorf("failed to publish transaction %d logical message at LSN %d: %w", txn.XID, lm.Position, err)
			}
		}
	}

	return nil
}

func (r *ReplicationHandler) decodeRow(rel *pglogrepl.RelationMessage, tuple *pglogrepl.TupleData) map[string]interface{} {
	values := make(map[string]interface{})

	for idx, col := range tuple.Columns {
		if idx >= len(rel.Columns) {
			continue
		}

		colName := rel.Columns[idx].Name

		switch col.DataType {
		case 'n':
			values[colName] = nil
		case 'u':
			values[colName] = UNCHANGED_TOAST
		case 't':
			values[colName] = r.typeMap.DecodeValue(rel.Columns[idx].DataType, col.Data)
		}
	}

	return values
}

func resolveUnchangedToastFromBefore(oldTupleType uint8, before, after map[string]interface{}) {
	if oldTupleType != pglogrepl.UpdateMessageTupleTypeOld || before == nil || after == nil {
		return
	}

	for key, afterVal := range after {
		if afterVal != UNCHANGED_TOAST {
			continue
		}

		beforeVal, ok := before[key]
		if !ok || beforeVal == UNCHANGED_TOAST {
			continue
		}

		after[key] = beforeVal
	}
}

func (r *ReplicationHandler) handlePrimaryKeepalive(data []byte) error {
	pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(data)
	if err != nil {
		return fmt.Errorf("failed to parse keepalive: %w", err)
	}

	if pkm.ReplyRequested {
		return r.sendStandbyStatusUpdate(context.Background())
	}

	return nil
}

func (r *ReplicationHandler) sendStandbyStatusUpdate(ctx context.Context) error {
	return pglogrepl.SendStandbyStatusUpdate(
		ctx,
		r.conn,
		pglogrepl.StandbyStatusUpdate{
			WALWritePosition: r.lastWrittenLSN,
		},
	)
}

func (r *ReplicationHandler) extractPrimaryKey(rel *pglogrepl.RelationMessage) {
	pkCols, err := loadPrimaryKey(context.Background(), r.config.DatabaseURL, rel.Namespace, rel.RelationName)
	if err != nil {
		LogError(r.config, "Failed to load primary key for", rel.Namespace+"."+rel.RelationName+":", err)
		return
	}
	r.primaryKeys[rel.RelationID] = pkCols
}

func loadPrimaryKey(ctx context.Context, databaseURL, schema, table string) ([]string, error) {
	conn, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		return nil, fmt.Errorf("failed to connect: %w", err)
	}
	defer conn.Close(ctx)
	return queryPrimaryKey(ctx, conn, schema, table)
}

type primaryKeyQuerier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

func queryPrimaryKey(ctx context.Context, querier primaryKeyQuerier, schema, table string) ([]string, error) {
	rows, err := querier.Query(ctx, primaryKeyQuery(), schema, table)
	if err != nil {
		return nil, fmt.Errorf("failed to query primary key: %w", err)
	}
	defer rows.Close()

	var pkCols []string
	for rows.Next() {
		var columnName string
		if err := rows.Scan(&columnName); err != nil {
			return nil, fmt.Errorf("failed to scan primary key column: %w", err)
		}
		pkCols = append(pkCols, columnName)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to read primary key rows: %w", err)
	}

	return pkCols, nil
}

func primaryKeyQuery() string {
	return `
		WITH primary_key_columns AS (
			SELECT a.attname AS column_name, key.ordinality::int AS ordinal_position
			FROM pg_index i
			JOIN pg_class c ON c.oid = i.indrelid
			JOIN pg_namespace n ON n.oid = c.relnamespace
			CROSS JOIN LATERAL unnest(i.indkey) WITH ORDINALITY AS key(attnum, ordinality)
			JOIN pg_attribute a
			  ON a.attrelid = c.oid
			 AND a.attnum = key.attnum
			WHERE i.indisprimary
			  AND n.nspname = $1
			  AND c.relname = $2
		),
		fallback_id_columns AS (
			SELECT a.attname AS column_name, a.attnum::int AS ordinal_position
			FROM pg_class c
			JOIN pg_namespace n ON n.oid = c.relnamespace
			JOIN pg_attribute a ON a.attrelid = c.oid
			LEFT JOIN pg_attrdef d ON d.adrelid = c.oid AND d.adnum = a.attnum
			WHERE n.nspname = $1
			  AND c.relname = $2
			  AND c.relkind IN ('r', 'p')
			  AND a.attname = 'id'
			  AND a.attnotnull
			  AND a.attnum > 0
			  AND NOT a.attisdropped
			  AND (
				  a.attidentity <> ''
				  OR pg_get_expr(d.adbin, d.adrelid) LIKE 'nextval(%'
			  )
			  AND NOT EXISTS (SELECT 1 FROM primary_key_columns)
		),
		key_columns AS (
			SELECT column_name, ordinal_position
			FROM primary_key_columns
			UNION ALL
			SELECT column_name, ordinal_position
			FROM fallback_id_columns
		)
		SELECT column_name
		FROM key_columns
		ORDER BY key_columns.ordinal_position
	`
}

func (r *ReplicationHandler) getPrimaryKeyForChange(change Change) []string {
	for relID, rel := range r.relations {
		if rel.Namespace == change.Schema && rel.RelationName == change.Table {
			return r.primaryKeys[relID]
		}
	}
	return nil
}

func (r *ReplicationHandler) Close() error {
	r.natsPublisher.Close()
	if r.conn == nil {
		return nil
	}
	return r.conn.Close(context.Background())
}

func (r *ReplicationHandler) shouldSkipUpdate(schema, table string, before, after map[string]interface{}) bool {
	if before == nil || after == nil {
		return false
	}

	// Find columns that have actually changed
	var changedColumns []string
	for key, beforeVal := range before {
		afterVal, exists := after[key]
		if !exists {
			continue
		}

		// Skip UNCHANGED_TOAST values
		if beforeVal == UNCHANGED_TOAST || afterVal == UNCHANGED_TOAST {
			continue
		}

		// Check if values are different
		if !valuesEqual(beforeVal, afterVal) {
			changedColumns = append(changedColumns, key)
		}
	}

	// If no columns changed, skip
	if len(changedColumns) == 0 {
		return true
	}

	// Check if all changed columns should be ignored
	for _, col := range changedColumns {
		if !r.config.ShouldIgnoreColumn(schema, table, col) {
			// Found a column that shouldn't be ignored, so don't skip
			return false
		}
	}

	// All changed columns should be ignored
	return true
}

func valuesEqual(a, b interface{}) bool {
	// Handle nil cases
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}

	// Simple comparison (works for basic types)
	return fmt.Sprintf("%v", a) == fmt.Sprintf("%v", b)
}

func (r *ReplicationHandler) isChangeTooLarge(change Change) bool {
	// Create a temporary message to estimate size
	tempMsg := &ChangeMessage{
		Before: change.Before,
		After:  change.After,
		Op:     getOpCode(change.Operation),
		Source: SourceMetadata{
			Schema: change.Schema,
			Table:  change.Table,
		},
	}

	data, err := json.Marshal(tempMsg)
	if err != nil {
		LogError(r.config, "Failed to marshal change for size check:", err)
		return false
	}

	dataSize := len(data)

	// Ignore large changes (larger than 2MB)
	if dataSize > MAX_CHANGE_SIZE_BYTES {
		LogWarn(r.config, fmt.Sprintf("Ignoring large change %s.%s = %.2fMB (larger than 2MB)", change.Schema, change.Table, float64(dataSize)/1000000))
		return true
	}

	return false
}

func (r *ReplicationHandler) shouldIgnoreLogicalMessage(lm LogicalMessage) bool {
	if lm.Prefix != MESSAGE_PREFIX_CONTEXT {
		LogDebug(r.config, "Ignoring logical message with prefix:", lm.Prefix)
		return true
	}

	// Create a temporary message to estimate size
	tempMsg := &ChangeMessage{
		Op: OPERATION_MESSAGE,
		Message: &MessagePayload{
			Prefix:  lm.Prefix,
			Content: string(lm.Content),
		},
	}

	data, err := json.Marshal(tempMsg)
	if err != nil {
		LogError(r.config, "Failed to marshal logical message for size check:", err)
		return false
	}

	dataSize := len(data)

	// Ignore oversized logical-message operations.
	if dataSize > MAX_LOGICAL_MESSAGE_SIZE_BYTES {
		LogWarn(r.config, fmt.Sprintf("Ignoring large context message = %.2fKB (larger than 10KB)", float64(dataSize)/1000))
		return true
	}

	return false
}

func getOpCode(operation string) string {
	switch operation {
	case "INSERT":
		return OPERATION_CREATE
	case "UPDATE":
		return OPERATION_UPDATE
	case "DELETE":
		return OPERATION_DELETE
	case "TRUNCATE":
		return OPERATION_TRUNCATE
	default:
		return ""
	}
}
