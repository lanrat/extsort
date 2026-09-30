package extsort

// Tests for the buffers the sorter reuses: chunk slices, and the encode and decode
// buffers of the built-in codecs.

import (
	"bytes"
	"cmp"
	"context"
	"math/rand"
	"slices"
	"strconv"
	"strings"
	"testing"
)

// The first chunk used to allocate a full ChunkSize slice up front: 8 MB to sort 10 ints
// with the default config. A chunk now grows with its records, to exactly ChunkSize, and
// once a chunk has filled, the later ones take their full size at once.
func TestChunksGrowWithInput(t *testing.T) {
	for _, tc := range []struct {
		name      string
		n         int
		chunkSize int
		wantCaps  []int
	}{
		{"small input", 10, 1 << 20, []int{firstChunkCap}},
		{"exactly one chunk", 5000, 5000, []int{5000}},
		{"several chunks", 12_000, 5000, []int{5000, 5000, 5000}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			in := make(chan int, tc.n)
			for i := range tc.n {
				in <- i
			}
			close(in)
			s := newSorter(in, atoiBytes, itoaBytes, cmp.Compare[int], &Config{ChunkSize: tc.chunkSize, ChanBuffSize: 8})
			s.sortCtx = context.Background()
			if err := s.buildChunks(); err != nil {
				t.Fatal(err)
			}
			var caps []int
			for c := range s.chunkChan {
				caps = append(caps, cap(c.data))
			}
			if !slices.Equal(caps, tc.wantCaps) {
				t.Errorf("chunk capacities %v, want %v", caps, tc.wantCaps)
			}
		})
	}
}

// Strings and Ordered encode every record into one reused buffer, and decode all the
// records of a chunk from one reused buffer. Records of varying length must survive both.
func TestBuiltinCodecsReuseBuffers(t *testing.T) {
	r := rand.New(rand.NewSource(1))
	words := make([]string, 5000)
	for i := range words {
		// 0 to 300 bytes, so a record's buffer is sometimes longer and sometimes shorter than the last
		words[i] = strings.Repeat(string(rune('a'+r.Intn(26))), r.Intn(300)) + strconv.Itoa(r.Intn(100))
	}
	nums := make([]int64, 5000)
	for i := range nums {
		nums[i] = r.Int63n(1<<62) - 1<<61 // varints of 1 to 9 bytes
	}
	config := func() *Config { return &Config{ChunkSize: 100, NumWorkers: 3, TempFilesDir: t.TempDir()} }

	t.Run("Strings", func(t *testing.T) {
		in := make(chan string, len(words))
		for _, w := range words {
			in <- w
		}
		close(in)
		s, out, errc := Strings(in, config())
		if s.appendBytes == nil || !s.reuseReadBuffer {
			t.Fatal("Strings does not reuse its buffers")
		}
		s.Sort(context.Background())
		checkSorted(t, out, errc, words)
	})
	t.Run("Ordered string", func(t *testing.T) {
		in := make(chan string, len(words))
		for _, w := range words {
			in <- w
		}
		close(in)
		s, out, errc := Ordered(in, config())
		if s.appendBytes == nil || !s.reuseReadBuffer {
			t.Fatal("Ordered does not reuse its buffers")
		}
		s.Sort(context.Background())
		checkSorted(t, out, errc, words)
	})
	t.Run("Ordered int64", func(t *testing.T) {
		in := make(chan int64, len(nums))
		for _, v := range nums {
			in <- v
		}
		close(in)
		s, out, errc := Ordered(in, config())
		s.Sort(context.Background())
		checkSorted(t, out, errc, nums)
	})
}

// checkSorted drains out and errc and checks that out delivered input in order.
func checkSorted[E cmp.Ordered](t *testing.T, out <-chan E, errc <-chan error, input []E) {
	t.Helper()
	got, err := drainWithTimeout(t, out, errc)
	if err != nil {
		t.Fatal(err)
	}
	if want := slices.Sorted(slices.Values(input)); !slices.Equal(got, want) {
		t.Errorf("got %d records (sorted: %v), want the %d input records in order", len(got), slices.IsSorted(got), len(want))
	}
}

// A user's fromBytes may keep the slice it is given, so Generic must not decode records
// from a reused buffer: here every record would end up aliasing the last one read.
func TestGenericDoesNotReuseReadBuffer(t *testing.T) {
	r := rand.New(rand.NewSource(1))
	records := make([][]byte, 2000)
	for i := range records {
		records[i] = []byte(strconv.Itoa(r.Int()))
	}
	in := make(chan []byte, len(records))
	for _, rec := range records {
		in <- rec
	}
	close(in)
	keep := func(d []byte) ([]byte, error) { return d, nil } // the record is the slice itself
	s, out, errc := Generic(in, keep, keep, bytes.Compare, &Config{ChunkSize: 50, NumWorkers: 3, TempFilesDir: t.TempDir()})
	if s.appendBytes != nil || s.reuseReadBuffer {
		t.Fatal("Generic reuses buffers for user codecs")
	}
	s.Sort(context.Background())
	got, err := drainWithTimeout(t, out, errc)
	if err != nil {
		t.Fatal(err)
	}
	want := slices.Clone(records)
	slices.SortFunc(want, bytes.Compare)
	if !slices.EqualFunc(got, want, bytes.Equal) {
		t.Error("records changed on their way through the sort")
	}
}
