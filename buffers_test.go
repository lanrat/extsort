package extsort

// Tests for the buffers the sorter reuses: chunk slices.

import (
	"cmp"
	"context"
	"slices"
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
