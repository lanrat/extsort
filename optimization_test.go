package extsort_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/lanrat/extsort"
)

// unusableTempDir returns a TempFilesDir that cannot hold files: a path below
// a regular file. Any attempt to create a temp file there fails, so a sort
// that succeeds with it never touched the disk. Counting files in a temp dir
// can't show this, because temp files are unlinked as soon as they are created.
func unusableTempDir(t *testing.T) string {
	t.Helper()
	file := filepath.Join(t.TempDir(), "not-a-dir")
	if err := os.WriteFile(file, nil, 0o600); err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}
	return filepath.Join(file, "sub")
}

// sortTen sorts 10 records in reverse order and returns the output and the sort error.
func sortTen(config *extsort.Config) ([]val, error) {
	inputChan := make(chan extsort.SortType, 10)
	for i := 9; i >= 0; i-- { // reverse order to ensure sorting happens
		inputChan <- val{Key: i, Order: i}
	}
	close(inputChan)

	sort, outChan, errChan := extsort.New(inputChan, fromBytesForTest, KeyLessThan, config)
	sort.Sort(context.Background())

	results := make([]val, 0, 10)
	for rec := range outChan {
		results = append(results, rec.(val))
	}
	return results, <-errChan
}

// TestSingleChunkOptimization verifies that small datasets don't create temp files
func TestSingleChunkOptimization(t *testing.T) {
	// Default ChunkSize (1M) holds all 10 records in one chunk
	config := extsort.DefaultConfig()
	config.TempFilesDir = unusableTempDir(t)

	results, err := sortTen(config)
	if err != nil {
		t.Fatalf("Single-chunk sort tried to create a temp file: %v", err)
	}
	if len(results) != 10 {
		t.Fatalf("Expected 10 results, got %d", len(results))
	}
	for i := range results {
		if results[i].Key != i {
			t.Errorf("Expected Key %d at position %d, got %d", i, i, results[i].Key)
		}
	}
}

// TestMultiChunkStillUsesTempFiles verifies that large datasets still use temp files
func TestMultiChunkStillUsesTempFiles(t *testing.T) {
	// Use a very small chunk size to force multiple chunks
	config := extsort.DefaultConfig()
	config.ChunkSize = 2

	results, err := sortTen(config)
	if err != nil {
		t.Fatalf("Sort error: %v", err)
	}
	if len(results) != 10 {
		t.Fatalf("Expected 10 results, got %d", len(results))
	}
	for i := range results {
		if results[i].Key != i {
			t.Errorf("Expected Key %d at position %d, got %d", i, i, results[i].Key)
		}
	}

	// The same sort must fail when no temp file can be created, which shows it needs one
	config.TempFilesDir = unusableTempDir(t)
	if _, err := sortTen(config); err == nil {
		t.Fatal("Expected an error from a multi-chunk sort with an unusable TempFilesDir, got nil")
	}
}
