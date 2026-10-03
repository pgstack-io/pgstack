package main

import "testing"

func TestHotParquetPairIndexReturnsFirstEligiblePair(t *testing.T) {
	maxHotSize := int64(100)
	parquetFiles := []ParquetFileInfo{
		{Size: 200},
		{Size: 100},
		{Size: 110},
		{Size: 105},
		{Size: 100},
	}

	if got := hotParquetPairIndex(parquetFiles, maxHotSize); got != 1 {
		t.Fatalf("hotParquetPairIndex = %d, want 1", got)
	}
}

func TestHotParquetPairIndexSkipsLatestFile(t *testing.T) {
	maxHotSize := int64(100)
	parquetFiles := []ParquetFileInfo{
		{Size: 200},
		{Size: 200},
		{Size: 100},
		{Size: 100},
	}

	if got := hotParquetPairIndex(parquetFiles, maxHotSize); got != -1 {
		t.Fatalf("hotParquetPairIndex = %d, want -1", got)
	}
}

func TestShouldCompactHotParquetPairAllowsBothFilesAtMaxPercent(t *testing.T) {
	maxHotSize := int64(100)

	if !shouldCompactHotParquetPair(
		ParquetFileInfo{Size: 120},
		ParquetFileInfo{Size: 120},
		maxHotSize,
	) {
		t.Fatal("expected files at max hot percent to compact")
	}
}

func TestShouldCompactHotParquetPairRequiresEachFileAroundHotSize(t *testing.T) {
	maxHotSize := int64(100)

	if shouldCompactHotParquetPair(
		ParquetFileInfo{Size: 79},
		ParquetFileInfo{Size: 100},
		maxHotSize,
	) {
		t.Fatal("expected file below min hot percent to skip compaction")
	}

	if shouldCompactHotParquetPair(
		ParquetFileInfo{Size: 100},
		ParquetFileInfo{Size: 121},
		maxHotSize,
	) {
		t.Fatal("expected file above max hot percent to skip compaction")
	}
}
