package main

import (
	"encoding/binary"
	"testing"
	"time"
)

func TestParquetInt64BoundsUseNumericOrdering(t *testing.T) {
	actualMin := int64Bound(1781621455606000)
	actualMax := int64Bound(1781813349941875)

	lexicographicallySmallerButLater := int64Bound(1781810317423793)
	lexicographicallyLargerButEarlier := int64Bound(1781719942625000)

	lower := lexicographicallySmallerButLater
	if shouldReplaceLowerBound(lower, actualMin, true) {
		lower = actualMin
	}
	if got := int64(binary.LittleEndian.Uint64(lower)); got != 1781621455606000 {
		t.Fatalf("lower bound = %d, want %d", got, int64(1781621455606000))
	}

	upper := lexicographicallyLargerButEarlier
	if shouldReplaceUpperBound(upper, actualMax, true) {
		upper = actualMax
	}
	if got := int64(binary.LittleEndian.Uint64(upper)); got != 1781813349941875 {
		t.Fatalf("upper bound = %d, want %d", got, int64(1781813349941875))
	}
}

func TestParquetStringBoundsUseByteOrdering(t *testing.T) {
	if !shouldReplaceLowerBound([]byte("b"), []byte("a"), false) {
		t.Fatal("expected string lower bound to use byte ordering")
	}
	if !shouldReplaceUpperBound([]byte("b"), []byte("c"), false) {
		t.Fatal("expected string upper bound to use byte ordering")
	}
}

func TestMaxCommittedAtFromMetadataUsesUpperBound(t *testing.T) {
	want := time.Date(2026, 6, 19, 12, 34, 56, 789000000, time.UTC)
	got, err := maxCommittedAtFromMetadata(ParquetFileInfo{
		Path: "s3://bucket/path/file.parquet",
		Stats: ParquetFileStats{
			UpperBounds: map[int][]byte{
				COMMITTED_AT_FIELD_ID: int64Bound(want.UnixMicro()),
			},
		},
	})
	if err != nil {
		t.Fatalf("maxCommittedAtFromMetadata returned error: %v", err)
	}
	if !got.Valid {
		t.Fatal("maxCommittedAtFromMetadata returned invalid time")
	}
	if !got.Time.Equal(want) {
		t.Fatalf("maxCommittedAtFromMetadata = %s, want %s", got.Time, want)
	}
}

func TestMaxCommittedAtFromMetadataMissingBound(t *testing.T) {
	got, err := maxCommittedAtFromMetadata(ParquetFileInfo{})
	if err != nil {
		t.Fatalf("maxCommittedAtFromMetadata returned error: %v", err)
	}
	if got.Valid {
		t.Fatalf("maxCommittedAtFromMetadata valid = true, want false")
	}
}

func TestMaxCommittedAtFromMetadataInvalidBound(t *testing.T) {
	_, err := maxCommittedAtFromMetadata(ParquetFileInfo{
		Path: "s3://bucket/path/file.parquet",
		Stats: ParquetFileStats{
			UpperBounds: map[int][]byte{
				COMMITTED_AT_FIELD_ID: []byte{1, 2, 3},
			},
		},
	})
	if err == nil {
		t.Fatal("maxCommittedAtFromMetadata returned nil error for invalid bound")
	}
}

func TestDropOldestParquetFileOutsideRetentionDropsExpiredOldestFile(t *testing.T) {
	retentionDays := 1
	writer := &IcebergWriter{
		config: &Config{
			S3Bucket:      "bucket",
			RetentionDays: &retentionDays,
		},
	}
	oldFile := parquetFileWithMaxCommittedAt("s3://bucket/path/old.parquet", time.Now().UTC().AddDate(0, 0, -2))
	newFile := parquetFileWithMaxCommittedAt("s3://bucket/path/new.parquet", time.Now().UTC())

	gotFiles, gotKeys, err := writer.dropOldestParquetFileOutsideRetention([]ParquetFileInfo{oldFile, newFile})
	if err != nil {
		t.Fatalf("dropOldestParquetFileOutsideRetention returned error: %v", err)
	}
	if len(gotFiles) != 1 || gotFiles[0].Path != newFile.Path {
		t.Fatalf("retained files = %+v, want only %s", gotFiles, newFile.Path)
	}
	if len(gotKeys) != 1 || gotKeys[0] != "path/old.parquet" {
		t.Fatalf("delete keys = %+v, want [path/old.parquet]", gotKeys)
	}
}

func TestDropOldestParquetFileOutsideRetentionKeepsFileWithinRetention(t *testing.T) {
	retentionDays := 1
	writer := &IcebergWriter{
		config: &Config{
			S3Bucket:      "bucket",
			RetentionDays: &retentionDays,
		},
	}
	parquetFiles := []ParquetFileInfo{
		parquetFileWithMaxCommittedAt("s3://bucket/path/file.parquet", time.Now().UTC()),
	}

	gotFiles, gotKeys, err := writer.dropOldestParquetFileOutsideRetention(parquetFiles)
	if err != nil {
		t.Fatalf("dropOldestParquetFileOutsideRetention returned error: %v", err)
	}
	if len(gotFiles) != len(parquetFiles) || gotFiles[0].Path != parquetFiles[0].Path {
		t.Fatalf("retained files = %+v, want original files", gotFiles)
	}
	if len(gotKeys) != 0 {
		t.Fatalf("delete keys = %+v, want none", gotKeys)
	}
}

func TestDropOldestParquetFileOutsideRetentionKeepsFilesWithoutRetention(t *testing.T) {
	writer := &IcebergWriter{
		config: &Config{
			S3Bucket: "bucket",
		},
	}
	parquetFiles := []ParquetFileInfo{
		parquetFileWithMaxCommittedAt("s3://bucket/path/file.parquet", time.Now().UTC().AddDate(0, 0, -2)),
	}

	gotFiles, gotKeys, err := writer.dropOldestParquetFileOutsideRetention(parquetFiles)
	if err != nil {
		t.Fatalf("dropOldestParquetFileOutsideRetention returned error: %v", err)
	}
	if len(gotFiles) != len(parquetFiles) || gotFiles[0].Path != parquetFiles[0].Path {
		t.Fatalf("retained files = %+v, want original files", gotFiles)
	}
	if len(gotKeys) != 0 {
		t.Fatalf("delete keys = %+v, want none", gotKeys)
	}
}

func TestStageFilesForDeletionWaitsOneSuccessfulCycle(t *testing.T) {
	writer := &IcebergWriter{}

	if ready := writer.stageFilesForDeletion([]string{"old.parquet", "old.parquet"}); len(ready) != 0 {
		t.Fatalf("first cycle ready files = %v, want none", ready)
	}

	ready := writer.stageFilesForDeletion([]string{"old.parquet", "old.avro", "old.avro"})
	if len(ready) != 1 || ready[0] != "old.parquet" {
		t.Fatalf("second cycle ready files = %v, want [old.parquet]", ready)
	}
	if len(writer.filesPendingDelete) != 1 || writer.filesPendingDelete[0] != "old.avro" {
		t.Fatalf("second cycle pending files = %v, want [old.avro]", writer.filesPendingDelete)
	}

	ready = writer.stageFilesForDeletion(nil)
	if len(ready) != 1 || ready[0] != "old.avro" {
		t.Fatalf("third cycle ready files = %v, want [old.avro]", ready)
	}
}

func parquetFileWithMaxCommittedAt(filePath string, committedAt time.Time) ParquetFileInfo {
	return ParquetFileInfo{
		Path: filePath,
		Stats: ParquetFileStats{
			UpperBounds: map[int][]byte{
				COMMITTED_AT_FIELD_ID: int64Bound(committedAt.UnixMicro()),
			},
		},
	}
}

func int64Bound(value int64) []byte {
	bound := make([]byte, 8)
	binary.LittleEndian.PutUint64(bound, uint64(value))
	return bound
}
