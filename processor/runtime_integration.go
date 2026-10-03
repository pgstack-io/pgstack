package main

import "context"

type IntegrationConfig struct{}

func loadIntegrationConfig(_ *Config) {}

func consumerName() string {
	return getEnv("NATS_CONSUMER_NAME", "pgstack-processor")
}

func prepareStorage(_ context.Context, _ *Config, _ *S3Client) error { return nil }
