package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

type S3Client struct {
	client *s3.S3
	bucket string
}

func NewS3Client(config *Config) (*S3Client, error) {
	sess, err := session.NewSession(&aws.Config{
		Region:           aws.String(config.S3Region),
		Endpoint:         aws.String(config.S3Endpoint),
		Credentials:      credentials.NewStaticCredentials(config.S3AccessKey, config.S3SecretKey, ""),
		S3ForcePathStyle: aws.Bool(true),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create AWS session: %w", err)
	}

	client := s3.New(sess)

	return &S3Client{
		client: client,
		bucket: config.S3Bucket,
	}, nil
}

func (s3c *S3Client) Upload(ctx context.Context, key string, data []byte) error {
	// Use a timeout-based context instead of the parent context
	// This ensures uploads complete even if parent context is cancelled
	uploadCtx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	_, err := s3c.client.PutObjectWithContext(uploadCtx, &s3.PutObjectInput{
		Bucket: aws.String(s3c.bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(data),
	})
	return err
}

func (s3c *S3Client) CopyFrom(ctx context.Context, sourceBucket string, sourceKey string, targetKey string) error {
	copyCtx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	copySource := url.PathEscape(sourceBucket + "/" + sourceKey)
	copySource = strings.ReplaceAll(copySource, "%2F", "/")

	_, err := s3c.client.CopyObjectWithContext(copyCtx, &s3.CopyObjectInput{
		Bucket:     aws.String(s3c.bucket),
		Key:        aws.String(targetKey),
		CopySource: aws.String(copySource),
	})
	return err
}

func (s3c *S3Client) GetFileSize(ctx context.Context, key string) (int64, error) {
	headCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	result, err := s3c.client.HeadObjectWithContext(headCtx, &s3.HeadObjectInput{
		Bucket: aws.String(s3c.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return 0, err
	}

	return *result.ContentLength, nil
}

func (s3c *S3Client) Download(ctx context.Context, key string) ([]byte, error) {
	result, err := s3c.client.GetObjectWithContext(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s3c.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, err
	}
	defer result.Body.Close()

	return io.ReadAll(result.Body)
}

func (s3c *S3Client) ListFiles(ctx context.Context, prefix string) ([]string, error) {
	// Use a timeout-based context instead of the parent context
	listCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	result, err := s3c.client.ListObjectsV2WithContext(listCtx, &s3.ListObjectsV2Input{
		Bucket: aws.String(s3c.bucket),
		Prefix: aws.String(prefix),
	})
	if err != nil {
		return nil, err
	}

	var files []string
	for _, obj := range result.Contents {
		filename := strings.TrimPrefix(*obj.Key, prefix+"/")
		if filename != "" && !strings.Contains(filename, "/") {
			files = append(files, filename)
		}
	}

	sort.Strings(files)
	return files, nil
}

func (s3c *S3Client) EnsureBucket(ctx context.Context) error {
	_, err := s3c.client.HeadBucketWithContext(ctx, &s3.HeadBucketInput{
		Bucket: aws.String(s3c.bucket),
	})
	if err == nil {
		return nil
	}

	_, err = s3c.client.CreateBucketWithContext(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(s3c.bucket),
	})
	return err
}

func (s3c *S3Client) Delete(ctx context.Context, key string) error {
	deleteCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := s3c.client.DeleteObjectWithContext(deleteCtx, &s3.DeleteObjectInput{
		Bucket: aws.String(s3c.bucket),
		Key:    aws.String(key),
	})
	return err
}
