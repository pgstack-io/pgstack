package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
)

func main() {
	config := LoadConfig()

	LogInfo(config, "Starting processor...")
	LogInfo(config, fmt.Sprintf("NATS: %s, Subject: %s", config.NatsURL, config.NatsSubject))
	LogInfo(config, fmt.Sprintf("S3: %s, Bucket: %s", config.S3Endpoint, config.S3Bucket))
	LogInfo(config, fmt.Sprintf("Batch interval: %v, Max hot Parquet size: %d MB", config.NatsBatchInterval, config.MaxHotParquetSizeMB))
	LogInfo(config, fmt.Sprintf("DuckDB memory limit: %s", config.DuckDBMemoryLimit))
	LogInfo(config, fmt.Sprintf("Retention days: %s", formatRetentionDays(config.RetentionDays)))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s3Client, err := NewS3Client(config)
	if err != nil {
		log.Fatalf("Failed to create S3 client: %v", err)
	}

	if err := s3Client.EnsureBucket(ctx); err != nil {
		LogWarn(config, "Failed to ensure bucket exists:", err)
	}

	if err := prepareStorage(ctx, config, s3Client); err != nil {
		log.Fatalf("Failed to prepare storage: %v", err)
	}

	writer := NewIcebergWriter(config, s3Client)
	searchWriter, err := NewSearchWriter(config)
	if err != nil {
		log.Fatalf("Failed to create Search writer: %v", err)
	}
	if err := reconcileSearchTables(ctx, config, searchWriter); err != nil {
		log.Fatalf("Failed to reconcile Search tables: %v", err)
	}

	natsConsumer, err := NewNatsConsumer(config)
	if err != nil {
		log.Fatalf("Failed to create NATS consumer: %v", err)
	}
	defer natsConsumer.Close()

	// Initialize message buffer and deduplication tracker
	messageBuffer := NewMessageBuffer()
	deduplicationTracker := NewDeduplicationTracker(DEDUPLICATION_MAX_SIZE)

	deduplicationInitialized := false

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		LogInfo(config, "Shutting down...")
		cancel()
	}()

	LogInfo(config, "Starting to consume messages...")

	for {
		select {
		case <-ctx.Done():
			LogInfo(config, "Context cancelled, exiting...")
			return
		default:
			batch, err := natsConsumer.ConsumeBatch(ctx, config.NatsBatchInterval)
			if err != nil {
				LogError(config, "Error consuming batch:", err)
				continue
			}

			if len(batch.Messages) == 0 {
				LogDebug(config, "No messages received in batch window, continuing...")
				continue
			}

			LogInfo(config, fmt.Sprintf("Received %d messages", len(batch.Messages)))

			stitchResult, err := processBatch(
				ctx,
				config,
				batch,
				&messageBuffer,
				writer,
				deduplicationTracker,
				&deduplicationInitialized,
				s3Client,
				searchWriter,
			)
			if err != nil {
				if isDuckDBOutOfMemory(err) {
					log.Fatalf("DuckDB ran out of memory: %v", err)
				}
				LogError(config, "Error processing messages:", err)
				// Don't ack on failure - messages will be redelivered
				continue
			}

			// Ack every resolved message. With AckExplicitPolicy, JetStream acks are
			// per-message; acking a later stream sequence does not ack earlier ones.
			ackedCount := 0
			for _, natsMsg := range stitchResult.AckMessages {
				if natsMsg == nil {
					continue
				}
				if err := natsMsg.Ack(); err != nil {
					LogError(config, "Failed to ack message:", err)
					continue
				}
				ackedCount++
			}
			if ackedCount > 0 {
				LogInfo(config, fmt.Sprintf("Acked %d messages through sequence #%d", ackedCount, stitchResult.AckStreamSequence))
			}

		}
	}
}

func processBatch(
	ctx context.Context,
	config *Config,
	batch *MessageBatch,
	messageBuffer **MessageBuffer,
	writer *IcebergWriter,
	deduplicationTracker *DeduplicationTracker,
	deduplicationInitialized *bool,
	s3Client *S3Client,
	searchWriter *SearchWriter,
) (*StitchResult, error) {
	var duckdb *DuckDBClient
	defer func() {
		closeBatchDuckDB(config, duckdb, writer, searchWriter)
	}()

	ensureDuckDB := func() (*DuckDBClient, error) {
		if duckdb != nil {
			return duckdb, nil
		}
		client, err := NewDuckDBClient(config)
		if err != nil {
			return nil, fmt.Errorf("failed to create DuckDB client: %w", err)
		}
		duckdb = client
		return duckdb, nil
	}

	if !*deduplicationInitialized {
		LogInfo(config, "Loading existing records for deduplication...")
		if err := deduplicationTracker.LoadFromParquet(ctx, ensureDuckDB, config, writer.dataS3Path, s3Client); err != nil {
			return nil, fmt.Errorf("failed to initialize deduplication: %w", err)
		}
		*deduplicationInitialized = true
	}

	stitchResult := StitchMessages(config, batch, *messageBuffer)
	*messageBuffer = stitchResult.Buffer

	auditRecords := recordsForProduct(stitchResult.StitchedRecords, PRODUCT_AUDIT)
	uniqueRecords := deduplicationTracker.FilterDuplicates(config, auditRecords)
	searchRecords := recordsForProduct(stitchResult.StitchedRecords, PRODUCT_SEARCH)

	LogInfo(config, fmt.Sprintf(
		"Processing: stitched=%d, audit=%d, search=%d, buffered=%d",
		len(stitchResult.StitchedRecords),
		len(uniqueRecords),
		len(searchRecords),
		(*messageBuffer).Size(),
	))

	if err := processAuditRecords(ctx, writer, deduplicationTracker, uniqueRecords, ensureDuckDB); err != nil {
		return nil, err
	}
	if err := processSearchRecords(ctx, searchWriter, searchRecords, ensureDuckDB); err != nil {
		return nil, err
	}

	return stitchResult, nil
}

func reconcileSearchTables(ctx context.Context, config *Config, writer *SearchWriter) error {
	if !writer.Enabled() {
		return nil
	}

	duckdb, err := NewDuckDBClient(config)
	if err != nil {
		return fmt.Errorf("failed to create DuckDB client: %w", err)
	}
	writer.SetDuckDB(duckdb)
	defer func() {
		writer.SetDuckDB(nil)
		if err := duckdb.Close(); err != nil {
			LogWarn(config, "Failed to close Search reconciliation DuckDB:", err)
		}
	}()

	return writer.ReconcileTables(ctx)
}

func recordsForProduct(records []*FetchedRecord, product string) []*FetchedRecord {
	filtered := make([]*FetchedRecord, 0, len(records))
	for _, record := range records {
		if record.Message.IsForProduct(product) {
			filtered = append(filtered, record)
		}
	}
	return filtered
}

func processAuditRecords(
	ctx context.Context,
	writer *IcebergWriter,
	tracker *DeduplicationTracker,
	records []*FetchedRecord,
	ensureDuckDB func() (*DuckDBClient, error),
) error {
	if len(records) == 0 {
		return nil
	}

	duckdb, err := ensureDuckDB()
	if err != nil {
		return err
	}
	writer.SetDuckDB(duckdb)
	if err := writer.AppendMessages(ctx, records); err != nil {
		return err
	}
	tracker.MarkRecordsProcessed(writer.config, records)
	return nil
}

func processSearchRecords(
	ctx context.Context,
	writer *SearchWriter,
	records []*FetchedRecord,
	ensureDuckDB func() (*DuckDBClient, error),
) error {
	if len(records) == 0 {
		return nil
	}

	duckdb, err := ensureDuckDB()
	if err != nil {
		return err
	}
	writer.SetDuckDB(duckdb)
	return writer.Apply(ctx, records)
}

func closeBatchDuckDB(config *Config, duckdb *DuckDBClient, icebergWriter *IcebergWriter, searchWriter *SearchWriter) {
	icebergWriter.SetDuckDB(nil)
	searchWriter.SetDuckDB(nil)
	if duckdb == nil {
		return
	}
	if err := duckdb.Close(); err != nil {
		LogWarn(config, "Failed to close DuckDB client:", err)
	}
	memoryUsage := getPodMemoryUsage()
	if memoryUsage == "" {
		memoryUsage = "unavailable"
	}
	LogDebug(config, fmt.Sprintf("Closed batch DuckDB. Pod memory: %s", memoryUsage))
}

func formatRetentionDays(retentionDays *int) string {
	if retentionDays == nil {
		return "unlimited"
	}

	return strconv.Itoa(*retentionDays)
}

func getPodMemoryUsage() string {
	data, err := os.ReadFile("/sys/fs/cgroup/memory.current")
	if err != nil {
		return ""
	}

	bytes, err := strconv.ParseInt(strings.TrimSpace(string(data)), 10, 64)
	if err != nil {
		return ""
	}

	return formatBytes(bytes)
}

func formatBytes(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}

	value := float64(bytes)
	for _, suffix := range []string{"KB", "MB", "GB", "TB"} {
		value /= unit
		if value < unit {
			return fmt.Sprintf("%.2f %s", value, suffix)
		}
	}

	return fmt.Sprintf("%.2f PB", value/unit)
}
