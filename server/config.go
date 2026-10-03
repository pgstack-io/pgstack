package main

import (
	"flag"
	"os"
	"slices"
	"strconv"
	"strings"
)

const (
	VERSION = "2.0.0"

	ENV_LOG_LEVEL     = "LOG_LEVEL"
	ENV_AUDIT_ENABLED = "AUDIT_ENABLED"

	ENV_AWS_REGION            = "AWS_REGION"
	ENV_AWS_S3_ENDPOINT       = "AWS_S3_ENDPOINT"
	ENV_AWS_S3_BUCKET         = "AWS_S3_BUCKET"
	ENV_AWS_ACCESS_KEY_ID     = "AWS_ACCESS_KEY_ID"
	ENV_AWS_SECRET_ACCESS_KEY = "AWS_SECRET_ACCESS_KEY"

	ENV_PORT                = "PGSTACK_PORT"
	ENV_DATABASE            = "PGSTACK_DATABASE"
	ENV_USER                = "PGSTACK_USER"
	ENV_PASSWORD            = "PGSTACK_PASSWORD"
	ENV_HOST                = "PGSTACK_HOST"
	ENV_DUCKDB_MEMORY_LIMIT = "DUCKDB_MEMORY_LIMIT"
	ENV_OPENAI_API_KEY      = "OPENAI_API_KEY"

	DEFAULT_LOG_LEVEL           = "INFO"
	DEFAULT_AWS_S3_ENDPOINT     = "s3.amazonaws.com"
	DEFAULT_HOST                = "0.0.0.0"
	DEFAULT_PORT                = "54321"
	DEFAULT_DATABASE            = "pgstack"
	DEFAULT_DUCKDB_MEMORY_LIMIT = "512MB"
)

type Config struct {
	// AWS config
	AwsRegion          string
	AwsS3Endpoint      string
	AwsS3Bucket        string
	AwsAccessKeyId     string
	AwsSecretAccessKey string

	// Server config
	LogLevel          string
	AuditEnabled      bool
	AuditBasePath     string
	Host              string
	Port              string
	Database          string
	User              string
	EncryptedPassword string
	DuckDBMemoryLimit string
	SearchBasePath    string
	SearchTablesJSON  string
	OpenAIAPIKey      string
}

type configParseValues struct {
	password string
}

var _config Config
var _configParseValues configParseValues

func init() {
	registerFlags()
}

func registerFlags() {
	flag.StringVar(&_config.LogLevel, "log-level", os.Getenv(ENV_LOG_LEVEL), `Log level: "ERROR", "WARN", "INFO", "DEBUG", "TRACE". Default: "`+DEFAULT_LOG_LEVEL+`"`)
	flag.BoolVar(&_config.AuditEnabled, "audit-enabled", getEnvBool(ENV_AUDIT_ENABLED, true), "Enable Audit and Iceberg support")
	flag.StringVar(&_config.AwsRegion, "aws-region", os.Getenv(ENV_AWS_REGION), "AWS region")
	flag.StringVar(&_config.AwsS3Endpoint, "aws-s3-endpoint", os.Getenv(ENV_AWS_S3_ENDPOINT), "AWS S3 endpoint. Default: \""+DEFAULT_AWS_S3_ENDPOINT+`"`)
	flag.StringVar(&_config.AwsS3Bucket, "aws-s3-bucket", os.Getenv(ENV_AWS_S3_BUCKET), "AWS S3 bucket name")
	flag.StringVar(&_config.AwsAccessKeyId, "aws-access-key-id", os.Getenv(ENV_AWS_ACCESS_KEY_ID), "AWS access key ID")
	flag.StringVar(&_config.AwsSecretAccessKey, "aws-secret-access-key", os.Getenv(ENV_AWS_SECRET_ACCESS_KEY), "AWS secret access key")

	flag.StringVar(&_config.AuditBasePath, "audit-base-path", os.Getenv("AUDIT_BASE_PATH"), "Base path in S3 for Iceberg tables")
	flag.StringVar(&_config.Host, "host", os.Getenv(ENV_HOST), "Host for pgstack to listen on")
	flag.StringVar(&_config.Port, "port", os.Getenv(ENV_PORT), "Port for pgstack to listen on")
	flag.StringVar(&_config.Database, "database", os.Getenv(ENV_DATABASE), "Database name")
	flag.StringVar(&_config.User, "user", os.Getenv(ENV_USER), "Database user")
	flag.StringVar(&_configParseValues.password, "password", os.Getenv(ENV_PASSWORD), "Database password")
	flag.StringVar(&_config.DuckDBMemoryLimit, "duckdb-memory-limit", os.Getenv(ENV_DUCKDB_MEMORY_LIMIT), "DuckDB memory limit. Default: \""+DEFAULT_DUCKDB_MEMORY_LIMIT+`"`)
	flag.StringVar(&_config.SearchBasePath, "search-base-path", os.Getenv("SEARCH_BASE_PATH"), "Base path in S3 for Lance tables")
	flag.StringVar(&_config.SearchTablesJSON, "search-tables-json", os.Getenv("SEARCH_TABLES_JSON"), "Search table configuration as JSON")
	flag.StringVar(&_config.OpenAIAPIKey, "openai-api-key", os.Getenv(ENV_OPENAI_API_KEY), "OpenAI API key used by search rank")
}

func parseFlags() {
	flag.Parse()

	if _config.LogLevel == "" {
		_config.LogLevel = DEFAULT_LOG_LEVEL
	} else if !slices.Contains(LOG_LEVELS, _config.LogLevel) {
		panic("Invalid log level " + _config.LogLevel + ". Must be one of " + strings.Join(LOG_LEVELS, ", "))
	}
	if _config.AwsRegion == "" {
		panic("AWS region is required")
	}
	if _config.AwsS3Endpoint == "" {
		_config.AwsS3Endpoint = DEFAULT_AWS_S3_ENDPOINT
	}
	if _config.AwsS3Bucket == "" {
		panic("AWS S3 bucket name is required")
	}
	if _config.AwsAccessKeyId != "" && _config.AwsSecretAccessKey == "" {
		panic("AWS secret access key is required")
	}
	if _config.AwsAccessKeyId == "" && _config.AwsSecretAccessKey != "" {
		panic("AWS access key ID is required")
	}

	if _config.Host == "" {
		panic("Host is required")
	}
	if _config.Port == "" {
		_config.Port = DEFAULT_PORT
	}
	if _config.Database == "" {
		_config.Database = DEFAULT_DATABASE
	}
	if _configParseValues.password != "" {
		_config.EncryptedPassword = StringToScramSha256(_configParseValues.password)
	}
	if _config.DuckDBMemoryLimit == "" {
		_config.DuckDBMemoryLimit = DEFAULT_DUCKDB_MEMORY_LIMIT
	}
	if _config.AuditEnabled && _config.AuditBasePath == "" {
		panic("Audit base path is required when Audit is enabled")
	}

	validateRuntimeConfig(&_config)
	_configParseValues = configParseValues{}
}

func LoadConfig() *Config {
	parseFlags()
	return &_config
}

func getEnvBool(key string, defaultValue bool) bool {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return defaultValue
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		panic("Invalid boolean " + key + ": " + value)
	}
	return parsed
}
