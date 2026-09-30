package extsort

// Regression tests for correctness bugs in the sort pipeline. Several of them inject
// failing temp files through package internals, so they live in package extsort.

import (
	"bufio"
	"bytes"
	"cmp"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"testing/iotest"
	"time"

	"github.com/lanrat/extsort/tempfile"
)

func itoaBytes(i int) ([]byte, error) { return []byte(strconv.Itoa(i)), nil }
func atoiBytes(b []byte) (int, error) { return strconv.Atoi(string(b)) }

// descendingInts sends n..1 on an unbuffered channel, then closes it.
func descendingInts(n int) chan int {
	ch := make(chan int)
	go func() {
		defer close(ch)
		for i := n; i > 0; i-- {
			ch <- i
		}
	}()
	return ch
}

// runSort calls Sort, drains the output, then reads the error channel.
// It fails the test if that does not finish in time.
func runSort[E any](t *testing.T, ctx context.Context, s *GenericSorter[E]) ([]E, error) {
	t.Helper()
	type result struct {
		got []E
		err error
	}
	done := make(chan result, 1)
	go func() {
		s.Sort(ctx)
		var got []E
		for v := range s.mergeChunkChan {
			got = append(got, v)
		}
		done <- result{got, <-s.mergeErrChan}
	}()
	select {
	case r := <-done:
		return r.got, r.err
	case <-time.After(10 * time.Second):
		t.Fatal("sort did not finish within 10s")
		return nil, nil
	}
}

// drainWithTimeout reads out until it closes, then reads errc.
func drainWithTimeout[E any](t *testing.T, out <-chan E, errc <-chan error) ([]E, error) {
	t.Helper()
	var got []E
	timeout := time.After(10 * time.Second)
	for {
		select {
		case v, ok := <-out:
			if !ok {
				return got, <-errc
			}
			got = append(got, v)
		case <-timeout:
			t.Fatal("sort did not finish within 10s")
		}
	}
}

// trackedTemp hands out in-memory temp files and counts how many the sorter
// leaves open. readErr and closeErr inject failures.
type trackedTemp struct {
	readErr  map[int]error // section index -> error returned when reading it
	closeErr error         // returned by the reader's Close

	mu      sync.Mutex
	created int
	closed  int
}

func (tt *trackedTemp) newWriter() (tempfile.TempWriter, error) {
	tt.mu.Lock()
	defer tt.mu.Unlock()
	tt.created++
	return &trackedWriter{TempWriter: tempfile.Mock(0), tt: tt}, nil
}

func (tt *trackedTemp) markClosed() {
	tt.mu.Lock()
	defer tt.mu.Unlock()
	tt.closed++
}

func (tt *trackedTemp) createdCount() int {
	tt.mu.Lock()
	defer tt.mu.Unlock()
	return tt.created
}

// open returns how many temp files were created but not closed exactly once.
func (tt *trackedTemp) open() int {
	tt.mu.Lock()
	defer tt.mu.Unlock()
	return tt.created - tt.closed
}

type trackedWriter struct {
	tempfile.TempWriter
	tt *trackedTemp
}

func (w *trackedWriter) Close() error {
	w.tt.markClosed()
	return w.TempWriter.Close()
}

func (w *trackedWriter) Save() (tempfile.TempReader, error) {
	r, err := w.TempWriter.Save()
	if err != nil {
		return nil, err
	}
	return &trackedReader{TempReader: r, tt: w.tt}, nil
}

type trackedReader struct {
	tempfile.TempReader
	tt *trackedTemp
}

func (r *trackedReader) Read(i int) *bufio.Reader {
	if err, ok := r.tt.readErr[i]; ok {
		return bufio.NewReader(iotest.ErrReader(err))
	}
	return r.TempReader.Read(i)
}

func (r *trackedReader) Close() error {
	r.tt.markClosed()
	if err := r.TempReader.Close(); err != nil {
		return err
	}
	return r.tt.closeErr
}

// newTrackedSorter returns an int sorter whose temp files are tracked by tt.
func newTrackedSorter(input <-chan int, toBytes ToBytesGeneric[int], compare CompareGeneric[int], config *Config, tt *trackedTemp) *GenericSorter[int] {
	s := newSorter(input, atoiBytes, toBytes, compare, config)
	s.newTempWriter = tt.newWriter
	return s
}

// A failing save stage used to leave the sort workers blocked on saveChunkChan,
// so Sort never returned once the input spanned more than 2+2*NumWorkers chunks.
func TestSaveErrorDoesNotDeadlock(t *testing.T) {
	errEncode := errors.New("encode failed")
	failing := func(int) ([]byte, error) { return nil, errEncode }
	for _, n := range []int{6, 7, 1000} {
		t.Run(strconv.Itoa(n)+" chunks", func(t *testing.T) {
			tt := &trackedTemp{}
			s := newTrackedSorter(descendingInts(n), failing, cmp.Compare[int], &Config{ChunkSize: 1, NumWorkers: 2, ChanBuffSize: 16}, tt)
			_, err := runSort(t, context.Background(), s)
			var serErr *SerializationError
			if !errors.As(err, &serErr) || !errors.Is(err, errEncode) {
				t.Fatalf("got error %v, want a SerializationError wrapping %q", err, errEncode)
			}
			if open := tt.open(); open != 0 {
				t.Errorf("%d temp file(s) left open", open)
			}
		})
	}
}

// An error in the sort stage used to make Sort return without closing saveChunkChan,
// leaking the save goroutine and its temp file.
func TestSortErrorStopsSaveStage(t *testing.T) {
	tt := &trackedTemp{}
	created := make(chan struct{})
	var once sync.Once
	// Chunks are [6 5] [4 3] [2 1]. Sorting [2 1] fails, but only after the
	// first two chunks reached the save stage and it created the temp file.
	compare := func(a, b int) int {
		if a == 1 || b == 1 {
			select {
			case <-created:
			case <-time.After(5 * time.Second):
			}
			panic("comparison failed")
		}
		return cmp.Compare(a, b)
	}
	s := newTrackedSorter(descendingInts(6), itoaBytes, compare, &Config{ChunkSize: 2, NumWorkers: 2}, tt)
	s.newTempWriter = func() (tempfile.TempWriter, error) {
		defer once.Do(func() { close(created) })
		return tt.newWriter()
	}

	_, err := runSort(t, context.Background(), s)
	var cmpErr *ComparisonError
	if !errors.As(err, &cmpErr) {
		t.Fatalf("got error %v, want a ComparisonError", err)
	}
	if tt.createdCount() != 1 {
		t.Fatalf("temp file created %d times, want 1", tt.createdCount())
	}
	if open := tt.open(); open != 0 {
		t.Errorf("%d temp file(s) left open", open)
	}
	buf := make([]byte, 1<<20)
	for deadline := time.Now().Add(2 * time.Second); ; {
		stacks := string(buf[:runtime.Stack(buf, true)])
		if !strings.Contains(stacks, "saveChunksOptimized") {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("save goroutine still running after Sort returned")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// A read error on a chunk's first record used to be treated as an empty chunk,
// silently dropping that chunk's records.
func TestMergeReportsChunkReadErrors(t *testing.T) {
	errDisk := errors.New("disk read error")
	for _, tc := range []struct {
		name    string
		workers int
		section int
	}{
		// 10 records in chunks of 5: two chunks plus the empty section Save appends.
		// NumWorkers 4 >= 3 sections merges single-threaded; NumWorkers 2 merges in parallel.
		{"single-threaded first chunk", 4, 0},
		{"single-threaded second chunk", 4, 1},
		{"parallel first chunk", 2, 0},
		{"parallel second chunk", 2, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tt := &trackedTemp{readErr: map[int]error{tc.section: errDisk}}
			s := newTrackedSorter(descendingInts(10), itoaBytes, cmp.Compare[int], &Config{ChunkSize: 5, NumWorkers: tc.workers}, tt)
			got, err := runSort(t, context.Background(), s)
			if !errors.Is(err, errDisk) {
				t.Fatalf("got %d records and error %v, want error %q", len(got), err, errDisk)
			}
			if open := tt.open(); open != 0 {
				t.Errorf("%d temp file(s) left open", open)
			}
		})
	}
}

// getNext used to treat a length header without its payload as a clean end of chunk.
func TestGetNextTruncatedRecord(t *testing.T) {
	for _, tc := range []struct {
		name   string
		data   []byte
		wantOK bool
		want   error
	}{
		{"end of chunk", nil, false, nil},
		{"complete record", []byte{1, '7'}, true, nil},
		{"header without payload", []byte{5}, false, io.ErrUnexpectedEOF},
		{"partial payload", []byte{5, '1', '2'}, false, io.ErrUnexpectedEOF},
		{"partial header", []byte{0x80}, false, io.ErrUnexpectedEOF},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := &mergeFile[int]{fromBytes: atoiBytes, reader: bufio.NewReader(bytes.NewReader(tc.data))}
			_, ok, err := m.getNext()
			if ok != tc.wantOK || !errors.Is(err, tc.want) {
				t.Fatalf("getNext() = ok %v, err %v; want ok %v, err %v", ok, err, tc.wantOK, tc.want)
			}
			if ok && m.nextRec != 7 {
				t.Errorf("getNext() read %d, want 7", m.nextRec)
			}
		})
	}
}

type legacyInt int

func (v legacyInt) ToBytes() []byte { return []byte(strconv.Itoa(int(v))) }

// The legacy FromBytes wrapper recovered panics into a local variable that was
// never returned, so a panicking FromBytes produced (nil, nil) and nil records.
func TestLegacyFromBytesPanicIsReported(t *testing.T) {
	fromBytes := makeSortTypeFromBytes(func([]byte) SortType { panic("corrupt record") })
	var deserErr *DeserializationError
	if v, err := fromBytes([]byte("x")); v != nil || !errors.As(err, &deserErr) {
		t.Fatalf("got (%v, %v), want (nil, DeserializationError)", v, err)
	}

	in := make(chan SortType, 10)
	for i := 10; i > 0; i-- {
		in <- legacyInt(i)
	}
	close(in)
	panicOn5 := func(b []byte) SortType {
		n, _ := strconv.Atoi(string(b))
		if n == 5 {
			panic("corrupt record")
		}
		return legacyInt(n)
	}
	less := func(a, b SortType) bool { // tolerates nil so the old behavior shows up as output
		ai, aok := a.(legacyInt)
		bi, bok := b.(legacyInt)
		if !aok || !bok {
			return !aok && bok
		}
		return ai < bi
	}
	sorter, out, errc := NewMock(in, panicOn5, less, &Config{ChunkSize: 2}, 0)
	sorter.Sort(context.Background())
	got, err := drainWithTimeout(t, out, errc)
	if !errors.As(err, &deserErr) {
		t.Errorf("got error %v, want a DeserializationError", err)
	}
	if slices.Contains(got, nil) {
		t.Errorf("output contains nil records: %v", got)
	}
}

// Panics in user callbacks during the save and merge stages used to crash the
// process. Each must be reported on the error channel instead.
func TestCallbackPanicsBecomeErrors(t *testing.T) {
	panicCompare := func(a, b int) int { panic("compare failed") }
	panicFromBytes := func([]byte) (int, error) { panic("decode failed") }
	panicToBytes := func(int) ([]byte, error) { panic("encode failed") }
	var cmpErr *ComparisonError
	var deserErr *DeserializationError
	var serErr *SerializationError
	for _, tc := range []struct {
		name      string
		n         int
		workers   int
		fromBytes FromBytesGeneric[int]
		toBytes   ToBytesGeneric[int]
		compare   CompareGeneric[int]
		target    any
	}{
		// With ChunkSize 1, sorting a chunk never calls compare: the first call is in the merge.
		{"compare in single-threaded merge", 3, 4, atoiBytes, itoaBytes, panicCompare, &cmpErr},
		{"compare in parallel merge", 20, 2, atoiBytes, itoaBytes, panicCompare, &cmpErr},
		{"fromBytes in merge", 5, 2, panicFromBytes, itoaBytes, cmp.Compare[int], &deserErr},
		{"toBytes in save", 50, 2, atoiBytes, panicToBytes, cmp.Compare[int], &serErr},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sorter, out, errc := MockGeneric(descendingInts(tc.n), tc.fromBytes, tc.toBytes, tc.compare, &Config{ChunkSize: 1, NumWorkers: tc.workers}, 0)
			sorter.Sort(context.Background())
			if _, err := drainWithTimeout(t, out, errc); !errors.As(err, tc.target) {
				t.Fatalf("got error %v, want %T", err, tc.target)
			}
		})
	}

	t.Run("compare in final merge", func(t *testing.T) {
		s := newSorter(nil, atoiBytes, itoaBytes, panicCompare, nil)
		a, b := make(chan int, 1), make(chan int, 1)
		a <- 1
		b <- 2
		close(a)
		close(b)
		if err := s.finalMergeSimple(context.Background(), []chan int{a, b}); !errors.As(err, &cmpErr) {
			t.Fatalf("got error %v, want a ComparisonError", err)
		}
	})
}

// A failing tempReader.Close used to panic with "send on closed channel", because
// the deferred Close ran after the deferred close(mergeErrChan).
func TestTempFileCloseErrorIsReported(t *testing.T) {
	errClose := errors.New("close failed")
	for _, workers := range []int{4, 2} { // single-threaded and parallel merge
		t.Run("NumWorkers "+strconv.Itoa(workers), func(t *testing.T) {
			tt := &trackedTemp{closeErr: errClose}
			s := newTrackedSorter(descendingInts(10), itoaBytes, cmp.Compare[int], &Config{ChunkSize: 5, NumWorkers: workers}, tt)
			got, err := runSort(t, context.Background(), s)
			if !errors.Is(err, errClose) {
				t.Fatalf("got error %v, want %q", err, errClose)
			}
			if len(got) != 10 || !slices.IsSorted(got) {
				t.Errorf("got %v, want 1..10 in order", got)
			}
		})
	}
}

// The temp file used to be created in the constructor and never closed on the empty,
// single-chunk and error paths. It is now only created for multi-chunk sorts, and
// released on every path.
func TestTempFileLifecycle(t *testing.T) {
	errEncode := errors.New("encode failed")
	failing := func(int) ([]byte, error) { return nil, errEncode }
	for _, tc := range []struct {
		name        string
		n           int
		toBytes     ToBytesGeneric[int]
		wantCreated int
		wantErr     error
	}{
		{"empty input", 0, itoaBytes, 0, nil},
		{"single chunk", 10, itoaBytes, 0, nil},
		{"multiple chunks", 100, itoaBytes, 1, nil},
		{"save error", 100, failing, 1, errEncode},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tt := &trackedTemp{}
			s := newTrackedSorter(descendingInts(tc.n), tc.toBytes, cmp.Compare[int], &Config{ChunkSize: 10}, tt)
			got, err := runSort(t, context.Background(), s)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("got error %v, want %v", err, tc.wantErr)
			}
			if tc.wantErr == nil && (len(got) != tc.n || !slices.IsSorted(got)) {
				t.Errorf("got %d records (sorted: %v), want %d sorted", len(got), slices.IsSorted(got), tc.n)
			}
			if tt.createdCount() != tc.wantCreated {
				t.Errorf("temp file created %d times, want %d", tt.createdCount(), tc.wantCreated)
			}
			if open := tt.open(); open != 0 {
				t.Errorf("%d temp file(s) left open", open)
			}
		})
	}

	t.Run("cancelled while saving", func(t *testing.T) {
		tt := &trackedTemp{}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		in := make(chan int)
		go func() { // endless input, stopped by the cancellation
			defer close(in)
			for i := 0; ; i++ {
				select {
				case in <- i:
				case <-ctx.Done():
					return
				}
			}
		}()
		s := newTrackedSorter(in, itoaBytes, cmp.Compare[int], &Config{ChunkSize: 10}, tt)
		var once sync.Once
		s.newTempWriter = func() (tempfile.TempWriter, error) {
			defer once.Do(cancel)
			return tt.newWriter()
		}
		if _, err := runSort(t, ctx, s); !errors.Is(err, context.Canceled) {
			t.Fatalf("got error %v, want %v", err, context.Canceled)
		}
		if open := tt.open(); open != 0 {
			t.Errorf("%d temp file(s) left open", open)
		}
	})

	t.Run("cancelled while merging", func(t *testing.T) {
		tt := &trackedTemp{}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		s := newTrackedSorter(descendingInts(100), itoaBytes, cmp.Compare[int], &Config{ChunkSize: 10}, tt)
		s.Sort(ctx)
		for i := 0; i < 5; i++ {
			<-s.mergeChunkChan
		}
		cancel()
		if _, err := drainWithTimeout(t, s.mergeChunkChan, s.mergeErrChan); !errors.Is(err, context.Canceled) {
			t.Fatalf("got error %v, want %v", err, context.Canceled)
		}
		if open := tt.open(); open != 0 {
			t.Errorf("%d temp file(s) left open", open)
		}
	})
}

// Temp files on disk must be closed, and on Windows removed, on success and on error.
func TestTempFilesReleasedOnDisk(t *testing.T) {
	errEncode := errors.New("encode failed")
	failOn50 := func(i int) ([]byte, error) {
		if i == 50 {
			return nil, errEncode
		}
		return itoaBytes(i)
	}
	for _, tc := range []struct {
		name    string
		toBytes ToBytesGeneric[int]
		wantErr error
	}{
		{"success", itoaBytes, nil},
		{"save error", failOn50, errEncode},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			s, _, _ := Generic(descendingInts(100), atoiBytes, tc.toBytes, cmp.Compare[int], &Config{ChunkSize: 10, TempFilesDir: dir})
			if _, err := runSort(t, context.Background(), s); !errors.Is(err, tc.wantErr) {
				t.Fatalf("got error %v, want %v", err, tc.wantErr)
			}
			entries, err := os.ReadDir(dir)
			if err != nil {
				t.Fatal(err)
			}
			for _, e := range entries {
				t.Errorf("temp file left on disk: %s", e.Name())
			}
			if n := openFilesUnder(dir); n > 0 {
				t.Errorf("%d temp file(s) still open", n)
			}
		})
	}
}

// openFilesUnder counts this process's open files under dir. It needs /proc/self/fd
// (Linux) and returns 0 where that is unavailable.
func openFilesUnder(dir string) int {
	fds, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		return 0
	}
	if resolved, err := filepath.EvalSymlinks(dir); err == nil {
		dir = resolved
	}
	n := 0
	for _, fd := range fds {
		target, err := os.Readlink(filepath.Join("/proc/self/fd", fd.Name()))
		if err == nil && strings.HasPrefix(target, dir+string(filepath.Separator)) {
			n++
		}
	}
	return n
}

// When the temp file could not be created, the constructors used to return a nil
// sorter, so the README pattern `go sorter.Sort(ctx)` crashed instead of reporting
// the error on the error channel.
func TestTempFileCreationErrorIsReportedOnErrChan(t *testing.T) {
	notADir := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(notADir, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	config := func() *Config { return &Config{ChunkSize: 2, TempFilesDir: notADir} }
	strs := func(n int) chan string {
		ch := make(chan string)
		go func() {
			defer close(ch)
			for i := n; i > 0; i-- {
				ch <- strconv.Itoa(i)
			}
		}()
		return ch
	}
	legacy := func(n int) chan SortType {
		ch := make(chan SortType)
		go func() {
			defer close(ch)
			for i := n; i > 0; i-- {
				ch <- legacyInt(i)
			}
		}()
		return ch
	}
	legacyLess := func(a, b SortType) bool { return a.(legacyInt) < b.(legacyInt) }
	legacyFromBytes := func(b []byte) SortType {
		n, _ := strconv.Atoi(string(b))
		return legacyInt(n)
	}
	wantErr := func(t *testing.T, err error) {
		t.Helper()
		if err == nil {
			t.Fatal("got nil error, want the temp file creation error")
		}
	}

	t.Run("Generic", func(t *testing.T) {
		sorter, out, errc := Generic(descendingInts(10), atoiBytes, itoaBytes, cmp.Compare[int], config())
		if sorter == nil {
			t.Fatal("constructor returned a nil sorter")
		}
		go sorter.Sort(context.Background())
		_, err := drainWithTimeout(t, out, errc)
		wantErr(t, err)
	})
	t.Run("Ordered", func(t *testing.T) {
		sorter, out, errc := Ordered(descendingInts(10), config())
		if sorter == nil {
			t.Fatal("constructor returned a nil sorter")
		}
		go sorter.Sort(context.Background())
		_, err := drainWithTimeout(t, out, errc)
		wantErr(t, err)
	})
	t.Run("Strings", func(t *testing.T) {
		sorter, out, errc := Strings(strs(10), config())
		if sorter == nil {
			t.Fatal("constructor returned a nil sorter")
		}
		go sorter.Sort(context.Background())
		_, err := drainWithTimeout(t, out, errc)
		wantErr(t, err)
	})
	t.Run("New", func(t *testing.T) {
		sorter, out, errc := New(legacy(10), legacyFromBytes, legacyLess, config())
		if sorter == nil {
			t.Fatal("constructor returned a nil sorter")
		}
		go sorter.Sort(context.Background())
		_, err := drainWithTimeout(t, out, errc)
		wantErr(t, err)
	})
	t.Run("single chunk needs no temp file", func(t *testing.T) {
		sorter, out, errc := Ordered(descendingInts(2), config())
		go sorter.Sort(context.Background())
		got, err := drainWithTimeout(t, out, errc)
		if err != nil || !slices.Equal(got, []int{1, 2}) {
			t.Fatalf("got %v, %v; want [1 2], nil", got, err)
		}
	})
}
