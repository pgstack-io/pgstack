package common

const (
	VERSION = "1.8.0"

	ENV_LOG_LEVEL                   = "BEMIDB_LOG_LEVEL"
	ENV_DISABLE_ANONYMOUS_ANALYTICS = "BEMIDB_DISABLE_ANONYMOUS_ANALYTICS"

	ENV_CATALOG_DATABASE_URL = "CATALOG_DATABASE_URL"

	ENV_AWS_REGION            = "AWS_REGION"
	ENV_AWS_S3_ENDPOINT       = "AWS_S3_ENDPOINT"
	ENV_AWS_S3_BUCKET         = "AWS_S3_BUCKET"
	ENV_AWS_ACCESS_KEY_ID     = "AWS_ACCESS_KEY_ID"
	ENV_AWS_SECRET_ACCESS_KEY = "AWS_SECRET_ACCESS_KEY"

	DEFAULT_LOG_LEVEL       = "INFO"
	DEFAULT_AWS_S3_ENDPOINT = "s3.amazonaws.com"
)

type AwsConfig struct {
	Region          string
	S3Endpoint      string // optional
	S3Bucket        string
	AccessKeyId     string
	SecretAccessKey string
}

type CommonConfig struct {
	Aws                       AwsConfig
	LogLevel                  string
	CatalogDatabaseUrl        string
	DisableAnonymousAnalytics bool
}
