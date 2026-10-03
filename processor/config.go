package main

import (
	"fmt"
	"os"
	"path"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	IntegrationConfig
	LogLevel            string
	ConsumerName        string
	NatsURL             string
	NatsSubject         string
	NatsStreamName      string
	NatsBatchInterval   time.Duration
	S3Endpoint          string
	S3Bucket            string
	S3AccessKey         string
	S3SecretKey         string
	S3Region            string
	AuditBasePath       string
	MaxHotParquetSizeMB int64
	DuckDBMemoryLimit   string
	RetentionDays       *int
	SearchBasePath      string
	SearchTablesJSON    string
	OpenAIAPIKey        string
}

func LoadConfig() *Config {
	batchIntervalSec := getEnvInt("NATS_BATCH_INTERVAL_SEC", 30)
	maxHotParquetSizeMB := getRequiredEnvInt64("MAX_HOT_PARQUET_SIZE_MB")

	s3Endpoint := getEnv("AWS_S3_ENDPOINT", "http://localhost:9000")
	targetS3Bucket := getRequiredEnv("AWS_S3_BUCKET")
	targetAuditBasePath := getRequiredEnv("AUDIT_BASE_PATH")
	defaultSearchBasePath := path.Join(path.Dir(targetAuditBasePath), "search")
	s3AccessKey := getEnv("AWS_ACCESS_KEY_ID", "minioadmin")
	s3SecretKey := getEnv("AWS_SECRET_ACCESS_KEY", "minioadmin")

	config := &Config{
		LogLevel:            getEnv("LOG_LEVEL", LOG_LEVEL_INFO),
		ConsumerName:        consumerName(),
		NatsURL:             getEnv("NATS_URL", "nats://localhost:4222"),
		NatsSubject:         getEnv("NATS_SUBJECT", ""),
		NatsStreamName:      getEnv("NATS_STREAM_NAME", ""),
		NatsBatchInterval:   time.Duration(batchIntervalSec) * time.Second,
		S3Endpoint:          s3Endpoint,
		S3Bucket:            targetS3Bucket,
		S3AccessKey:         s3AccessKey,
		S3SecretKey:         s3SecretKey,
		S3Region:            getEnv("AWS_REGION", "us-east-1"),
		AuditBasePath:       targetAuditBasePath,
		MaxHotParquetSizeMB: maxHotParquetSizeMB,
		DuckDBMemoryLimit:   getRequiredEnv("DUCKDB_MEMORY_LIMIT"),
		RetentionDays:       getOptionalEnvInt("RETENTION_DAYS"),
		SearchBasePath:      getEnv("SEARCH_BASE_PATH", defaultSearchBasePath),
		SearchTablesJSON:    getEnv("SEARCH_TABLES_JSON", "[]"),
		OpenAIAPIKey:        getEnv("OPENAI_API_KEY", ""),
	}

	if !isValidLogLevel(config.LogLevel) {
		panic("Invalid LOG_LEVEL " + config.LogLevel + ". Must be one of " + strings.Join(LOG_LEVELS, ", "))
	}

	loadIntegrationConfig(config)
	return config
}

func (c *Config) MaxHotParquetFileSizeBytes() int64 {
	return c.MaxHotParquetSizeMB * 1024 * 1024
}

func (c *Config) S3Path(key string) string {
	return "s3://" + c.S3Bucket + "/" + key
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return strings.TrimSpace(value)
	}
	return defaultValue
}

func getRequiredEnv(key string) string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		panic(fmt.Sprintf("%s is required", key))
	}
	return value
}

func getEnvInt(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		if i, err := strconv.Atoi(value); err == nil {
			return i
		}
	}
	return defaultValue
}

func getOptionalEnvInt(key string) *int {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return nil
	}

	i, err := strconv.Atoi(value)
	if err != nil {
		panic(fmt.Sprintf("invalid %s: %v", key, err))
	}

	return &i
}

func getRequiredEnvInt64(key string) int64 {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		panic(fmt.Sprintf("%s is required", key))
	}

	i, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		panic(fmt.Sprintf("invalid %s: %v", key, err))
	}

	return i
}
