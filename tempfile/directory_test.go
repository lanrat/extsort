package tempfile_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/lanrat/extsort/tempfile"
)

func TestDirectorySharing(t *testing.T) {
	// Test that multiple writers share the same directory when using intelligent directory selection

	// Create first writer - this will use the intelligent directory selection
	writer1, err := tempfile.New("", true)
	if err != nil {
		t.Fatalf("Failed to create first tempfile writer: %v", err)
	}

	// Get the directory that was actually used
	dir1 := filepath.Dir(writer1.Name())
	t.Logf("First writer using directory: %s", dir1)

	// Create second writer with same parameters - should use same directory if it's cached
	writer2, err := tempfile.New("", true)
	if err != nil {
		t.Fatalf("Failed to create second tempfile writer: %v", err)
	}

	dir2 := filepath.Dir(writer2.Name())
	t.Logf("Second writer using directory: %s", dir2)

	// Both should use the same directory due to caching
	if dir1 != dir2 {
		t.Errorf("Expected both writers to use same directory, got %s and %s", dir1, dir2)
	}

	// The directory should exist
	if _, err := os.Stat(dir1); os.IsNotExist(err) {
		t.Fatalf("Expected directory %s to exist", dir1)
	}

	// Close first writer
	err = writer1.Close()
	if err != nil {
		t.Fatalf("Failed to close first writer: %v", err)
	}

	// Directory should still exist
	if _, err := os.Stat(dir1); os.IsNotExist(err) {
		t.Errorf("Expected directory %s to still exist after closing first writer", dir1)
	}

	// Close second writer
	err = writer2.Close()
	if err != nil {
		t.Fatalf("Failed to close second writer: %v", err)
	}

	// Check final state - the behavior depends on whether we created the directory
	_, err = os.Stat(dir1)
	dirStillExists := !os.IsNotExist(err)

	t.Logf("Directory %s still exists after all writers closed: %t", dir1, dirStillExists)

	// This test documents the behavior rather than asserting specific cleanup
	// The important guarantee is that temp files are cleaned up, not necessarily the directories
}

func TestDirectoryReferenceCountingCleanup(t *testing.T) {
	// Test directory reference counting behavior with user-specified directories

	// Create a base test directory
	baseDir, err := os.MkdirTemp("", "extsort-cleanup-test-")
	if err != nil {
		t.Fatalf("Failed to create test base dir: %v", err)
	}
	defer func() {
		err := os.RemoveAll(baseDir)
		if err != nil {
			t.Fatal(err)
		}
	}()

	// Create a subdirectory that matches our process-specific pattern
	// This simulates what would happen in the fallback case
	testDir := filepath.Join(baseDir, "test-extsort-dir")

	// Don't create the directory - let tempfile.New create it

	// Create first writer in the test directory
	writer1, err := tempfile.New(testDir, true)
	if err != nil {
		t.Fatalf("Failed to create first tempfile writer: %v", err)
	}

	// Verify directory was created
	if _, err := os.Stat(testDir); os.IsNotExist(err) {
		t.Fatalf("Expected directory %s to be created", testDir)
	}

	// Create second writer in the same directory
	writer2, err := tempfile.New(testDir, true)
	if err != nil {
		t.Fatalf("Failed to create second tempfile writer: %v", err)
	}

	// Both writers should be in the same directory
	dir1 := filepath.Dir(writer1.Name())
	dir2 := filepath.Dir(writer2.Name())
	if dir1 != testDir || dir2 != testDir {
		t.Errorf("Expected both writers in %s, got %s and %s", testDir, dir1, dir2)
	}

	// Close first writer - directory should still exist
	err = writer1.Close()
	if err != nil {
		t.Fatalf("Failed to close first writer: %v", err)
	}

	// Directory should still exist because second writer is using it
	if _, err := os.Stat(testDir); os.IsNotExist(err) {
		t.Errorf("Expected directory %s to still exist after closing first writer", testDir)
	}

	// Close second writer
	err = writer2.Close()
	if err != nil {
		t.Fatalf("Failed to close second writer: %v", err)
	}

	// Since this isn't a process-specific extsort directory, it should still exist
	// (We only clean up directories that match our specific naming pattern)
	if _, err := os.Stat(testDir); os.IsNotExist(err) {
		t.Errorf("Expected directory %s to still exist after closing all writers (not an extsort-specific dir)", testDir)
	}
}
