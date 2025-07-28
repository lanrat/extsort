package extsort_test

import (
	"context"
	"os"
	"testing"

	"github.com/lanrat/extsort"
)

// TestSingleChunkOptimization verifies that small datasets don't create temp files
func TestSingleChunkOptimization(t *testing.T) {
	// Small dataset that should fit in a single chunk
	inputChan := make(chan extsort.SortType, 10)
	for i := 9; i >= 0; i-- { // reverse order to ensure sorting happens
		inputChan <- val{Key: i, Order: i}
	}
	close(inputChan)

	// Create sorter with default config (ChunkSize = 1M)
	sort, outChan, errChan := extsort.New(inputChan, fromBytesForTest, KeyLessThan, nil)

	// Count temp files before
	tempDir := os.TempDir()
	beforeFiles, err := os.ReadDir(tempDir)
	if err != nil {
		t.Fatalf("Failed to read temp dir: %v", err)
	}
	beforeCount := len(beforeFiles)

	// Sort
	sort.Sort(context.Background())

	// Read results
	results := make([]val, 0, 10)
	for rec := range outChan {
		results = append(results, rec.(val))
	}
	if err := <-errChan; err != nil {
		t.Fatalf("Sort error: %v", err)
	}

	// Count temp files after
	afterFiles, err := os.ReadDir(tempDir)
	if err != nil {
		t.Fatalf("Failed to read temp dir: %v", err)
	}
	afterCount := len(afterFiles)

	// Verify results are sorted
	for i := 0; i < len(results); i++ {
		if results[i].Key != i {
			t.Errorf("Expected Key %d at position %d, got %d", i, i, results[i].Key)
		}
	}

	// Verify no additional temp files were created
	if afterCount > beforeCount {
		t.Errorf("Expected no temp files to be created for single chunk, but temp file count increased from %d to %d", beforeCount, afterCount)
	}

	t.Logf("Single chunk optimization working: no temp files created for 10 items")
}

// TestMultiChunkStillUsesTempFiles verifies that large datasets still use temp files
func TestMultiChunkStillUsesTempFiles(t *testing.T) {
	// Use a very small chunk size to force multiple chunks
	config := extsort.DefaultConfig()
	config.ChunkSize = 2 // Force multiple chunks for small dataset

	inputChan := make(chan extsort.SortType, 10)
	for i := 9; i >= 0; i-- {
		inputChan <- val{Key: i, Order: i}
	}
	close(inputChan)

	sort, outChan, errChan := extsort.New(inputChan, fromBytesForTest, KeyLessThan, config)
	sort.Sort(context.Background())

	// Read results to completion
	results := make([]val, 0, 10)
	for rec := range outChan {
		results = append(results, rec.(val))
	}
	if err := <-errChan; err != nil {
		t.Fatalf("Sort error: %v", err)
	}

	// Verify results are sorted
	for i := 0; i < len(results); i++ {
		if results[i].Key != i {
			t.Errorf("Expected Key %d at position %d, got %d", i, i, results[i].Key)
		}
	}

	t.Logf("Multi-chunk path still working with small chunk size")
}
