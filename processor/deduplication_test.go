package main

import "testing"

func TestDeduplicationTrackerEvictsOldestKeys(t *testing.T) {
	tracker := NewDeduplicationTracker(2)
	first := DeduplicationKey{Position: 1, Database: "db", Schema: "public", Table: "users", Operation: "CREATE"}
	second := DeduplicationKey{Position: 2, Database: "db", Schema: "public", Table: "users", Operation: "CREATE"}
	third := DeduplicationKey{Position: 3, Database: "db", Schema: "public", Table: "users", Operation: "CREATE"}

	tracker.MarkProcessed(first)
	tracker.MarkProcessed(second)
	tracker.MarkProcessed(third)

	if tracker.IsDuplicate(first) {
		t.Fatal("expected oldest key to be evicted")
	}
	if !tracker.IsDuplicate(second) || !tracker.IsDuplicate(third) {
		t.Fatal("expected newest keys to remain tracked")
	}
}

func TestDeduplicationTrackerDoesNotReorderDuplicateMarks(t *testing.T) {
	tracker := NewDeduplicationTracker(2)
	first := DeduplicationKey{Position: 1, Database: "db", Schema: "public", Table: "users", Operation: "CREATE"}
	second := DeduplicationKey{Position: 2, Database: "db", Schema: "public", Table: "users", Operation: "CREATE"}
	third := DeduplicationKey{Position: 3, Database: "db", Schema: "public", Table: "users", Operation: "CREATE"}

	tracker.MarkProcessed(first)
	tracker.MarkProcessed(second)
	tracker.MarkProcessed(first)
	tracker.MarkProcessed(third)

	if tracker.IsDuplicate(first) {
		t.Fatal("expected duplicate mark not to refresh insertion order")
	}
	if !tracker.IsDuplicate(second) || !tracker.IsDuplicate(third) {
		t.Fatal("expected second and third keys to remain tracked")
	}
}

func TestFilterDuplicatesDoesNotMarkRecordsProcessed(t *testing.T) {
	config := quietTestConfig()
	tracker := NewDeduplicationTracker(10)
	record := fetchedRecordForDeduplicationTest(1)

	filtered := tracker.FilterDuplicates(config, []*FetchedRecord{record})
	if len(filtered) != 1 {
		t.Fatalf("filtered records = %d, want 1", len(filtered))
	}

	key := tracker.MakeKey(record.Message, MapOperation(config, record.Message.Op))
	if tracker.IsDuplicate(key) {
		t.Fatal("filtering must not mark a record before storage succeeds")
	}

	tracker.MarkRecordsProcessed(config, filtered)
	if !tracker.IsDuplicate(key) {
		t.Fatal("expected successfully stored record to be marked")
	}
}

func TestFilterDuplicatesRemovesDuplicatesWithinBatch(t *testing.T) {
	config := quietTestConfig()
	tracker := NewDeduplicationTracker(10)
	record := fetchedRecordForDeduplicationTest(1)
	duplicate := fetchedRecordForDeduplicationTest(1)

	filtered := tracker.FilterDuplicates(config, []*FetchedRecord{record, duplicate})
	if len(filtered) != 1 {
		t.Fatalf("filtered records = %d, want 1", len(filtered))
	}
}

func quietTestConfig() *Config {
	return &Config{LogLevel: LOG_LEVEL_ERROR}
}

func fetchedRecordForDeduplicationTest(position uint64) *FetchedRecord {
	return &FetchedRecord{
		Message: &ChangeMessage{
			Op: OPERATION_CREATE,
			Source: SourceMetadata{
				Lsn:    position,
				Db:     "db",
				Schema: "public",
				Table:  "users",
			},
		},
	}
}
