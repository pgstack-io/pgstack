package main

import (
	"context"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsConfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type S3Client struct {
	Config *Config
	S3     *s3.Client
}

func NewS3Client(Config *Config) (*S3Client, error) {
	var awsConfigOptions = []func(*awsConfig.LoadOptions) error{
		awsConfig.WithRegion(Config.AwsRegion),
	}

	if Config.LogLevel == LOG_LEVEL_TRACE {
		awsConfigOptions = append(awsConfigOptions, awsConfig.WithClientLogMode(aws.LogRequest))
	}

	awsCredentials := credentials.NewStaticCredentialsProvider(
		Config.AwsAccessKeyId,
		Config.AwsSecretAccessKey,
		"",
	)
	awsConfigOptions = append(awsConfigOptions, awsConfig.WithCredentialsProvider(awsCredentials))

	loadedAwsConfig, err := awsConfig.LoadDefaultConfig(context.Background(), awsConfigOptions...)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS configuration: %w", err)
	}

	s3Client := s3.NewFromConfig(loadedAwsConfig, func(o *s3.Options) {
		if Config.AwsS3Endpoint != DEFAULT_AWS_S3_ENDPOINT {
			endpoint := Config.AwsS3Endpoint
			if !strings.HasPrefix(endpoint, "http://") && !strings.HasPrefix(endpoint, "https://") {
				if IsLocalHost(endpoint) {
					endpoint = "http://" + endpoint
				} else {
					endpoint = "https://" + endpoint
				}
			}
			o.BaseEndpoint = aws.String(endpoint)
			o.UsePathStyle = true
		}
	})

	return &S3Client{
		Config: Config,
		S3:     s3Client,
	}, nil
}

func (s3Client *S3Client) BucketS3Prefix() string {
	return "s3://" + s3Client.Config.AwsS3Bucket + "/"
}

// s3://bucket/some/path -> some/path
func (s3Client *S3Client) ObjectKey(objectPath string) string {
	return strings.TrimPrefix(objectPath, s3Client.BucketS3Prefix())
}

func (s3Client *S3Client) GetObject(fileKey string) *s3.GetObjectOutput {
	getObjectOutput, err := s3Client.S3.GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: aws.String(s3Client.Config.AwsS3Bucket),
		Key:    aws.String(fileKey),
	})
	PanicIfError(s3Client.Config, err)
	return getObjectOutput
}

func (s3Client *S3Client) ListObjects(prefix string) *s3.ListObjectsV2Output {
	listObjectsOutput, err := s3Client.S3.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{
		Bucket: aws.String(s3Client.Config.AwsS3Bucket),
		Prefix: aws.String(prefix),
	})
	PanicIfError(s3Client.Config, err)

	return listObjectsOutput
}
