package extsort_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/lanrat/extsort"
)

// TestTempFileCreationFailure tests that the library handles tempfile creation
// failures gracefully without causing segmentation faults. This test addresses
// the bug reported in issue #10 where a full filesystem causes a segfault.
func TestTempFileCreationFailure(t *testing.T) {
	// Create a test directory that will be automatically cleaned up
	testDir := t.TempDir()

	// Create a read-only subdirectory to simulate filesystem permission issues
	readOnlyDir := filepath.Join(testDir, "readonly")
	err := os.Mkdir(readOnlyDir, 0555) // r-xr-xr-x (no write permission)
	if err != nil {
		t.Fatalf("Failed to create read-only directory: %v", err)
	}

	// Ensure cleanup even on panic - make directory writable so it can be removed
	defer func() {
		if r := recover(); r != nil {
			t.Logf("Test panicked: %v", r)
			// Still try to cleanup before re-panicking
			if err := os.Chmod(readOnlyDir, 0755); err != nil {
				t.Logf("Warning: Could not make directory writable for cleanup after panic: %v", err)
			}
			panic(r) // Re-panic after cleanup
		} else {
			// Normal cleanup
			if err := os.Chmod(readOnlyDir, 0755); err != nil {
				t.Logf("Warning: Could not make directory writable for cleanup: %v", err)
			}
		}
	}()

	t.Logf("Created read-only temp directory: %s", readOnlyDir)

	// Create input data that would normally require multiple chunks (and temp files)
	inputChan := make(chan extsort.SortType, 20)
	for i := 0; i < 15; i++ {
		inputChan <- &testData{Key: i, Value: "data"}
	}
	close(inputChan)

	// Configure extsort to use the read-only directory for temp files
	// This should cause tempfile.New() to fail
	config := &extsort.Config{
		TempFilesDir: readOnlyDir,
		ChunkSize:    5, // Force multiple chunks to trigger temp file usage
		NumWorkers:   2,
	}

	// Create the sorter - this should fail when trying to create temp files
	sort, outChan, errChan := extsort.New(inputChan, testFromBytes, testLess, config)

	// The temp file is created lazily, so the sorter is usable and Sort reports the failure
	if sort == nil {
		t.Fatal("Sorter is nil; Sort() on it would panic instead of reporting the error")
	}
	sort.Sort(context.Background())

	// Drain any output that might come through
	outputCount := 0
	for range outChan {
		outputCount++
	}
	t.Logf("Received %d output items before error", outputCount)

	// We should get an error, not a panic/segfault
	select {
	case err := <-errChan:
		if err == nil {
			t.Fatal("Expected an error due to temp file creation failure, got nil")
		}
		t.Logf("Got expected error: %v", err)
		// The error should be related to file/directory permissions or creation
		if err.Error() == "" {
			t.Fatal("Error message is empty")
		}
	default:
		t.Fatal("Expected an error to be sent to error channel")
	}
}

// TestTempFileCreationFailureStrings tests the same scenario with string sorting
func TestTempFileCreationFailureStrings(t *testing.T) {
	testDir := t.TempDir()
	readOnlyDir := filepath.Join(testDir, "readonly")
	err := os.Mkdir(readOnlyDir, 0555)
	if err != nil {
		t.Fatalf("Failed to create read-only directory: %v", err)
	}

	// Ensure cleanup even on panic - make directory writable so it can be removed
	defer func() {
		if r := recover(); r != nil {
			t.Logf("Test panicked: %v", r)
			// Still try to cleanup before re-panicking
			if err := os.Chmod(readOnlyDir, 0755); err != nil {
				t.Logf("Warning: Could not make directory writable for cleanup after panic: %v", err)
			}
			panic(r) // Re-panic after cleanup
		} else {
			// Normal cleanup
			if err := os.Chmod(readOnlyDir, 0755); err != nil {
				t.Logf("Warning: Could not make directory writable for cleanup: %v", err)
			}
		}
	}()

	// Create string input data
	inputChan := make(chan string, 20)
	testStrings := []string{"zebra", "apple", "banana", "cherry", "date", "elderberry", "fig", "grape"}
	for _, str := range testStrings {
		inputChan <- str
	}
	close(inputChan)

	config := &extsort.Config{
		TempFilesDir: readOnlyDir,
		ChunkSize:    3, // Force multiple chunks
		NumWorkers:   2,
	}

	sort, outChan, errChan := extsort.Strings(inputChan, config)

	// The temp file is created lazily, so the sorter is usable and Sort reports the failure
	if sort == nil {
		t.Fatal("String sorter is nil; Sort() on it would panic instead of reporting the error")
	}
	sort.Sort(context.Background())

	// Drain output
	outputCount := 0
	for range outChan {
		outputCount++
	}
	t.Logf("String sort received %d output items before error", outputCount)

	// Should get error, not segfault
	select {
	case err := <-errChan:
		if err == nil {
			t.Fatal("Expected an error due to temp file creation failure, got nil")
		}
		t.Logf("Got expected error from string sort: %v", err)
	default:
		t.Fatal("Expected an error to be sent to error channel")
	}
}

// Helper types and functions for testing
type testData struct {
	Key   int
	Value string
}

func testFromBytes(data []byte) extsort.SortType {
	// Simple deserialization for testing
	if len(data) < 4 {
		return &testData{Key: 0, Value: "empty"}
	}
	key := int(data[0])
	value := string(data[1:])
	return &testData{Key: key, Value: value}
}

func testLess(a, b extsort.SortType) bool {
	ta, ok1 := a.(*testData)
	tb, ok2 := b.(*testData)
	if !ok1 || !ok2 {
		return false
	}
	return ta.Key < tb.Key
}

// ToBytes method for testData
func (td *testData) ToBytes() []byte {
	result := make([]byte, 1+len(td.Value))
	result[0] = byte(td.Key)
	copy(result[1:], td.Value)
	return result
}
