package tempfile

// Regression tests for temp directory selection and cleanup.

import (
	"io"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

// testWriters creates each TempWriter implementation.
var testWriters = map[string]func(t *testing.T) TempWriter{
	"FileWriter": func(t *testing.T) TempWriter {
		w, err := New(t.TempDir(), true)
		if err != nil {
			t.Fatal(err)
		}
		return w
	},
	"MockFileWriter": func(*testing.T) TempWriter { return Mock(0) },
}

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

// Save used to end the current section even when nothing was written since the last
// Next, adding an empty section after the last one.
func TestSaveAddsNoEmptySection(t *testing.T) {
	for name, newWriter := range testWriters {
		t.Run(name, func(t *testing.T) {
			for _, tc := range []struct {
				name     string
				sections []string // each ended by Next
				trailing string   // written after the last Next
				want     []string
			}{
				{"Next after every section", []string{"a", "b"}, "", []string{"a", "b"}},
				{"data after the last Next", []string{"a", "b"}, "c", []string{"a", "b", "c"}},
				{"nothing written", nil, "", []string{""}},
			} {
				t.Run(tc.name, func(t *testing.T) {
					w := newWriter(t)
					for _, data := range tc.sections {
						if _, err := w.WriteString(data); err != nil {
							t.Fatal(err)
						}
						if _, err := w.Next(); err != nil {
							t.Fatal(err)
						}
					}
					if _, err := w.WriteString(tc.trailing); err != nil {
						t.Fatal(err)
					}
					if got := w.Size(); got != len(tc.want) {
						t.Errorf("writer Size() = %d, want %d", got, len(tc.want))
					}
					r, err := w.Save()
					if err != nil {
						t.Fatal(err)
					}
					defer func() { _ = r.Close() }()
					if got := r.Size(); got != len(tc.want) {
						t.Fatalf("reader Size() = %d, want %d", got, len(tc.want))
					}
					for i, want := range tc.want {
						got, err := io.ReadAll(r.Read(i))
						if err != nil || string(got) != want {
							t.Errorf("section %d = %q, %v; want %q", i, got, err, want)
						}
					}
				})
			}
		})
	}
}

func TestReadBufferSize(t *testing.T) {
	for _, tc := range []struct {
		sections   int
		sectionLen int64
		want       int
	}{
		{1, 1 << 20, fileBufferSize},
		{1000, 1 << 20, fileBufferSize}, // a 64 MiB share per 1000 sections is still above 64 KiB
		{10_000, 1 << 20, readBufferBudget / 10_000},
		{100_000, 1 << 20, minReadBufferSize},
		{1, 100, 100},
		{10_000, 100, 100},
		{0, 0, 0},
	} {
		if got := readBufferSize(tc.sections, tc.sectionLen); got != tc.want {
			t.Errorf("readBufferSize(%d, %d) = %d, want %d", tc.sections, tc.sectionLen, got, tc.want)
		}
	}
}

// allocatedBytes returns how many bytes f allocated.
func allocatedBytes(f func()) uint64 {
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	f()
	runtime.ReadMemStats(&after)
	return after.TotalAlloc - before.TotalAlloc
}

// Save used to allocate a 64 KiB read buffer for every section up front, so merge memory
// grew by 64 KiB per chunk. Buffers are now created on first Read and sized to the section.
func TestReadBuffersAreLazyAndSized(t *testing.T) {
	const sections = 1000 // 62.5 MiB of read buffers before
	for name, newWriter := range testWriters {
		t.Run(name, func(t *testing.T) {
			w := newWriter(t)
			for range sections {
				if _, err := w.WriteString("0123456789"); err != nil {
					t.Fatal(err)
				}
				if _, err := w.Next(); err != nil {
					t.Fatal(err)
				}
			}
			var r TempReader
			var err error
			if n := allocatedBytes(func() { r, err = w.Save() }); n > 1<<20 {
				t.Errorf("Save allocated %d bytes for %d sections", n, sections)
			}
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = r.Close() }()
			if n := allocatedBytes(func() {
				for i := range sections {
					r.Read(i)
				}
			}); n > 1<<20 {
				t.Errorf("reading %d 10-byte sections allocated %d bytes", sections, n)
			}
			if got, err := io.ReadAll(r.Read(sections - 1)); err != nil || string(got) != "0123456789" {
				t.Errorf("last section = %q, %v", got, err)
			}
		})
	}
}
