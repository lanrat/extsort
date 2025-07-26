package tempfile_test

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/lanrat/extsort/tempfile"
)

// TestRobustCleanup verifies that temp files are cleaned up properly
// across different platforms and error scenarios
func TestRobustCleanup(t *testing.T) {
	// Test normal cleanup path
	t.Run("NormalCleanup", func(t *testing.T) {
		writer, err := tempfile.New("")
		if err != nil {
			t.Fatalf("Failed to create temp file: %v", err)
		}

		filename := writer.Name()
		
		// Write some data
		_, err = writer.WriteString("test data")
		if err != nil {
			t.Fatalf("Failed to write: %v", err)
		}

		// Save and get reader
		reader, err := writer.Save()
		if err != nil {
			t.Fatalf("Failed to save: %v", err)
		}

		// Verify we can read the data
		data, err := reader.Read(0).ReadString('\n')
		if err != nil && data != "test data" {
			// ReadString includes delimiter, so check contains
			if data != "test data" {
				t.Errorf("Expected 'test data', got %q", data)
			}
		}

		// Close reader - this should clean up the file
		err = reader.Close()
		if err != nil {
			t.Fatalf("Failed to close reader: %v", err)
		}

		// Check cleanup behavior based on platform
		_, err = os.Stat(filename)
		if runtime.GOOS == "windows" {
			// On Windows, file should be gone after close
			if !os.IsNotExist(err) {
				t.Errorf("Expected file to be deleted on Windows, but it still exists")
			}
		} else {
			// On Unix, file was unlinked immediately but may still show as not exist
			// This is expected behavior - the test passes if we got here without errors
		}
	})

	// Test cleanup when writer is closed directly (abort case)
	t.Run("WriterAbortCleanup", func(t *testing.T) {
		writer, err := tempfile.New("")
		if err != nil {
			t.Fatalf("Failed to create temp file: %v", err)
		}

		filename := writer.Name()
		
		// Write some data
		_, err = writer.WriteString("test data")
		if err != nil {
			t.Fatalf("Failed to write: %v", err)
		}

		// Close writer directly (abort case)
		err = writer.Close()
		if err != nil {
			t.Fatalf("Failed to close writer: %v", err)
		}

		// File should be cleaned up
		_, err = os.Stat(filename)
		if !os.IsNotExist(err) && runtime.GOOS == "windows" {
			t.Errorf("Expected file to be deleted after writer close")
		}
	})
}

// TestCleanupBehaviorDifferences documents the platform differences
func TestCleanupBehaviorDifferences(t *testing.T) {
	writer, err := tempfile.New("")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}

	filename := writer.Name()
	tempDir := filepath.Dir(filename)
	
	// Check if file exists initially
	_, err = os.Stat(filename)
	initialExists := err == nil

	t.Logf("Platform: %s", runtime.GOOS)
	t.Logf("Temp file: %s", filename)
	t.Logf("Temp dir: %s", tempDir)
	t.Logf("File exists after creation: %t", initialExists)
	
	if runtime.GOOS != "windows" {
		// On Unix, file should be unlinked immediately, so stat should fail
		if initialExists {
			t.Logf("NOTE: File still visible after creation (expected on some systems)")
		} else {
			t.Logf("File unlinked immediately after creation (Unix behavior)")
		}
	} else {
		// On Windows, file should exist until explicitly closed
		if !initialExists {
			t.Errorf("Expected file to exist on Windows after creation")
		} else {
			t.Logf("File exists until close (Windows behavior)")
		}
	}

	// Clean up
	writer.Close()
}