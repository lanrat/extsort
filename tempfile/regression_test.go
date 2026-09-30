package tempfile

// Regression tests for temp directory selection and cleanup.

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

// canTestPermissions reports whether chmod restrictions apply to this process.
func canTestPermissions() bool {
	return runtime.GOOS != "windows" && os.Geteuid() != 0
}

// An explicit directory that cannot hold files used to be silently replaced by the
// default directory. It must be reported as an error instead.
func TestExplicitUnusableDirIsAnError(t *testing.T) {
	base := t.TempDir()
	file := filepath.Join(base, "file")
	if err := os.WriteFile(file, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	cases := map[string]string{
		"regular file":        file,
		"path through a file": filepath.Join(file, "sub"),
	}
	if canTestPermissions() {
		locked := filepath.Join(base, "locked")
		if err := os.Mkdir(locked, 0o000); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = os.Chmod(locked, 0o700) })
		cases["permission denied"] = filepath.Join(locked, "sub")
	}
	for name, dir := range cases {
		t.Run(name, func(t *testing.T) {
			if got := GetTempDir(dir, true); got != dir {
				t.Errorf("GetTempDir(%q) = %q, want it unchanged", dir, got)
			}
			w, err := New(dir, true)
			if err == nil {
				t.Errorf("New(%q) created %s, want an error", dir, w.Name())
				_ = w.Close()
			}
		})
	}
}

// Default selection used to pick a candidate that did not exist (and then fail to
// create it, as with no /var/tmp in a non-root container) or was read-only (as with a
// read-only root filesystem), instead of falling back to the next candidate.
func TestDefaultSelectionSkipsUnusableCandidates(t *testing.T) {
	base := t.TempDir()
	missing := filepath.Join(base, "missing")
	file := filepath.Join(base, "file")
	if err := os.WriteFile(file, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	writable := filepath.Join(base, "writable")
	if err := os.Mkdir(writable, 0o700); err != nil {
		t.Fatal(err)
	}
	candidates := []string{missing, file}
	if canTestPermissions() {
		readOnly := filepath.Join(base, "readonly")
		if err := os.Mkdir(readOnly, 0o500); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = os.Chmod(readOnly, 0o700) })
		candidates = append(candidates, readOnly)
	}
	candidates = append(candidates, writable)

	if got := firstUsableDir(candidates); got != writable {
		t.Fatalf("firstUsableDir(%q) = %q, want %q", candidates, got, writable)
	}
	if _, err := os.Stat(missing); !os.IsNotExist(err) {
		t.Errorf("missing candidate %s was created", missing)
	}
	if entries, err := os.ReadDir(writable); err != nil || len(entries) != 0 {
		t.Errorf("probe left files behind in %s: %v %v", writable, entries, err)
	}
}

// Our own .extsort_<pid> fallback directories are still selected before they exist,
// because New creates them on demand.
func TestDefaultSelectionAcceptsMissingExtsortDir(t *testing.T) {
	ours := filepath.Join(t.TempDir(), extsortTempDirName)
	if got := firstUsableDir([]string{ours}); got != ours {
		t.Fatalf("firstUsableDir() = %q, want %q", got, ours)
	}
	if _, err := os.Stat(ours); !os.IsNotExist(err) {
		t.Errorf("selection created %s; New should create it", ours)
	}
}

// Save hands the writer's directory reference to the reader, so closing the reader
// must release it. The .extsort_<pid> directory used to be left behind after every
// successful sort.
func TestReaderCloseRemovesExtsortDir(t *testing.T) {
	dir := filepath.Join(t.TempDir(), extsortTempDirName)
	w, err := New(dir, true)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.WriteString("data"); err != nil {
		t.Fatal(err)
	}
	r, err := w.Save()
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Errorf("%s still exists after the reader was closed", dir)
	}
}

// BenchmarkWriteAndSave writes 64 MiB in 1 MiB sections, then saves and closes the file.
// Save used to fsync the file, which is already unlinked on Unix and only read back by
// this process.
func BenchmarkWriteAndSave(b *testing.B) {
	data := make([]byte, 1<<20)
	dir := b.TempDir()
	for b.Loop() {
		w, err := New(dir, true)
		if err != nil {
			b.Fatal(err)
		}
		for range 64 {
			if _, err := w.Write(data); err != nil {
				b.Fatal(err)
			}
			if _, err := w.Next(); err != nil {
				b.Fatal(err)
			}
		}
		r, err := w.Save()
		if err != nil {
			b.Fatal(err)
		}
		if err := r.Close(); err != nil {
			b.Fatal(err)
		}
	}
}
