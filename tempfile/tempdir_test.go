package tempfile

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func TestGetTempDirWithPreferences(t *testing.T) {
	// No state reset needed - new implementation is stateless after initialization

	// Test with preferDiskBacked = true
	result1 := GetTempDir("", true)
	if result1 == "" {
		t.Error("Expected non-empty directory with preferDiskBacked=true")
	}

	// Test with preferDiskBacked = false
	result2 := GetTempDir("", false)
	if result2 == "" {
		t.Error("Expected non-empty directory with preferDiskBacked=false")
	}

	t.Logf("preferDiskBacked=true: %s", result1)
	t.Logf("preferDiskBacked=false: %s", result2)
}

func TestGetTempDirWithSpecificDir(t *testing.T) {
	// No state reset needed - new implementation is stateless after initialization

	// Create a temporary directory for testing
	testDir, err := os.MkdirTemp("", "extsort-test-")
	if err != nil {
		t.Fatalf("Failed to create test dir: %v", err)
	}
	defer func() {
		err := os.RemoveAll(testDir)
		if err != nil {
			t.Fatalf("%s", err)
		}
	}()

	// When a specific directory is provided and valid, it should be used
	result := GetTempDir(testDir, true)
	if result != testDir {
		t.Errorf("Expected GetTempDir to return %s, got %s", testDir, result)
	}
}

func TestGetTempDirWithEmptyDir(t *testing.T) {
	// No state reset needed - new implementation is stateless after initialization

	// When empty directory is provided, should use selected temp dir
	result := GetTempDir("", true)
	if result == "" {
		t.Error("Expected GetTempDir to return non-empty directory")
	}

	// Should be a valid directory path
	if !filepath.IsAbs(result) {
		t.Errorf("Expected absolute path, got %s", result)
	}
}

func TestGetTempDirConsistency(t *testing.T) {
	// No state reset needed - new implementation is stateless after initialization

	// Multiple calls with empty string should return the same directory
	result1 := GetTempDir("", true)
	result2 := GetTempDir("", true)

	if result1 != result2 {
		t.Errorf("Expected consistent results, got %s and %s", result1, result2)
	}
}

func TestIsDirectoryUsable(t *testing.T) {
	// Test with existing directory
	testDir, err := os.MkdirTemp("", "extsort-usable-test-")
	if err != nil {
		t.Fatalf("Failed to create test dir: %v", err)
	}
	defer func() {
		err := os.RemoveAll(testDir)
		if err != nil {
			t.Fatalf("%s", err)
		}
	}()

	if !isDirectoryUsable(testDir) {
		t.Errorf("Expected existing directory %s to be usable", testDir)
	}

	// Test with non-existent directory that can be created
	nonExistentDir := filepath.Join(testDir, "subdir")
	if !isDirectoryUsable(nonExistentDir) {
		t.Errorf("Expected creatable directory %s to be usable", nonExistentDir)
	}

	// Test with invalid directory (file)
	testFile := filepath.Join(testDir, "testfile")
	if err := os.WriteFile(testFile, []byte("test"), 0644); err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	if isDirectoryUsable(testFile) {
		t.Errorf("Expected file %s to not be usable as directory", testFile)
	}
}

// Removed TestGetTempDirCandidates - covered by other tests

func TestGetDiskPreferredCandidates(t *testing.T) {
	candidates := buildDiskPreferredCandidates()

	// Should return some candidates on Unix-like systems
	switch runtime.GOOS {
	case "linux", "darwin", "freebsd", "openbsd", "netbsd", "dragonfly", "solaris":
		if len(candidates) == 0 {
			t.Error("Expected some disk-preferred candidates on Unix-like system")
		}

		// Should include /var/tmp on Unix-like systems
		found := false
		for _, candidate := range candidates {
			if candidate == "/var/tmp" {
				found = true
				break
			}
		}
		if !found {
			t.Error("Expected /var/tmp to be included in disk-preferred candidates")
		}

	case "windows":
		// Windows typically doesn't have the same issue, candidates may be empty
		t.Logf("Windows disk-preferred candidates: %v", candidates)

	default:
		// Unknown OS should have minimal or no candidates
		t.Logf("Unknown OS (%s) disk-preferred candidates: %v", runtime.GOOS, candidates)
	}
}

func TestGetAdditionalFallbacks(t *testing.T) {
	fallbacks := buildAdditionalFallbacks()

	// Should include some fallbacks
	if len(fallbacks) == 0 {
		t.Error("Expected some fallback directories")
	}

	// Each fallback should be an absolute path
	for _, fallback := range fallbacks {
		if !filepath.IsAbs(fallback) {
			t.Errorf("Expected fallback %s to be absolute path", fallback)
		}
	}

	// Should include home directory fallback if home directory is available
	if homeDir, err := os.UserHomeDir(); err == nil {
		hasHomeFallback := false
		for _, fallback := range fallbacks {
			if filepath.Dir(fallback) == homeDir {
				hasHomeFallback = true
				break
			}
		}
		if !hasHomeFallback {
			t.Error("Expected fallbacks to include a subdirectory of home directory")
		}
	}

	// Should include working directory fallback if working directory is available
	if workDir, err := os.Getwd(); err == nil {
		hasWorkFallback := false
		for _, fallback := range fallbacks {
			if filepath.Dir(fallback) == workDir {
				hasWorkFallback = true
				break
			}
		}
		if !hasWorkFallback {
			t.Error("Expected fallbacks to include a subdirectory of working directory")
		}
	}
}

// Removed TestNewWithDiskPreference - covered by integration tests

// Removed TestNewWithSpecificDirectory - covered by TestGetTempDirWithSpecificDir
