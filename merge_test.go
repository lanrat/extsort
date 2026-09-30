package extsort

// Tests for the parallel merge, whose workers hand records to the final merge in batches.

import (
	"cmp"
	"context"
	"errors"
	"math/rand"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// sorterGoroutines counts the goroutines running a GenericSorter method.
func sorterGoroutines() (int, string) {
	buf := make([]byte, 1<<22)
	all := string(buf[:runtime.Stack(buf, true)])
	n := 0
	for _, g := range strings.Split(all, "\n\n") {
		if strings.Contains(g, "extsort.(*GenericSorter") {
			n++
		}
	}
	return n, all
}

// checkSorterGoroutinesExit fails the test unless the goroutines running GenericSorter
// methods drop back to before within a few seconds.
func checkSorterGoroutinesExit(t *testing.T, before int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		n, stacks := sorterGoroutines()
		if n <= before {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d sorter goroutine(s) still running after the sort ended:\n%s", n-before, stacks)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// intsChan returns a closed channel holding values.
func intsChan(values []int) chan int {
	ch := make(chan int, len(values))
	for _, v := range values {
		ch <- v
	}
	close(ch)
	return ch
}

// Batches must carry every record, in order, whatever the record count and chunk size.
func TestParallelMergeBatchBoundaries(t *testing.T) {
	r := rand.New(rand.NewSource(1))
	for _, n := range []int{mergeBatchSize - 1, mergeBatchSize, mergeBatchSize + 1, 3*mergeBatchSize + 7} {
		values := make([]int, n)
		for i := range values {
			values[i] = r.Intn(n / 2) // duplicates, so the merge sees ties
		}
		want := slices.Sorted(slices.Values(values))
		for _, chunkSize := range []int{7, mergeBatchSize - 1, mergeBatchSize + 1} {
			for _, workers := range []int{2, 3} {
				t.Run("n="+strconv.Itoa(n)+" chunk="+strconv.Itoa(chunkSize)+" workers="+strconv.Itoa(workers), func(t *testing.T) {
					s, out, errc := MockGeneric(intsChan(values), atoiBytes, itoaBytes, cmp.Compare[int], &Config{ChunkSize: chunkSize, NumWorkers: workers}, 0)
					s.Sort(context.Background())
					got, err := drainWithTimeout(t, out, errc)
					if err != nil {
						t.Fatal(err)
					}
					if !slices.Equal(got, want) {
						t.Errorf("got %d records (sorted: %v), want the %d input records in order", len(got), slices.IsSorted(got), n)
					}
				})
			}
		}
	}
}

// 50 chunks of 1000 records merged by 4 workers, so the parallel merge is used and the
// final merge sends records on while the workers still have batches to send.
const (
	midMergeRecords   = 50_000
	midMergeChunkSize = 1000
	midMergeWorkers   = 4
)

// A worker's read error in the middle of a chunk must reach the error channel, and every
// merge goroutine must stop, including workers blocked sending a batch.
func TestParallelMergeReadErrorMidChunk(t *testing.T) {
	before, _ := sorterGoroutines()
	errDisk := errors.New("disk read error")
	// Section 30 fails a third of the way in. Its worker merges the chunks with
	// smaller records first, so the error comes after many records were sent on.
	tt := &trackedTemp{readErr: map[int]error{30: errDisk}, readErrAfter: map[int]int64{30: 2000}}
	s := newTrackedSorter(descendingInts(midMergeRecords), itoaBytes, cmp.Compare[int],
		&Config{ChunkSize: midMergeChunkSize, NumWorkers: midMergeWorkers}, tt)
	got, err := runSort(t, context.Background(), s)
	if !errors.Is(err, errDisk) {
		t.Fatalf("got error %v, want %q", err, errDisk)
	}
	if len(got) == 0 || len(got) >= midMergeRecords || !slices.IsSorted(got) {
		t.Errorf("got %d records (sorted: %v) before the error, want some but not all, in order", len(got), slices.IsSorted(got))
	}
	if open := tt.open(); open != 0 {
		t.Errorf("%d temp file(s) left open", open)
	}
	checkSorterGoroutinesExit(t, before)
}

// A compareFunc panic in the middle of the merge, in a worker or the final merge, must
// reach the error channel as a ComparisonError, and every merge goroutine must stop.
func TestParallelMergeComparePanicMidMerge(t *testing.T) {
	before, _ := sorterGoroutines()
	var armed atomic.Bool
	var calls atomic.Int64
	compare := func(a, b int) int {
		if armed.Load() && calls.Add(1) == 1000 {
			panic("compare failed")
		}
		return cmp.Compare(a, b)
	}
	tt := &trackedTemp{}
	s := newTrackedSorter(descendingInts(midMergeRecords), itoaBytes, compare,
		&Config{ChunkSize: midMergeChunkSize, NumWorkers: midMergeWorkers}, tt)
	// Sort returns once every chunk is sorted and saved. The merge then blocks on the
	// full output channel well before its end, so only the merge can panic.
	s.Sort(context.Background())
	armed.Store(true)
	got, err := drainWithTimeout(t, s.mergeChunkChan, s.mergeErrChan)
	var cmpErr *ComparisonError
	if !errors.As(err, &cmpErr) {
		t.Fatalf("got error %v, want a ComparisonError", err)
	}
	if len(got) >= midMergeRecords || !slices.IsSorted(got) {
		t.Errorf("got %d records (sorted: %v), want fewer than %d, in order", len(got), slices.IsSorted(got), midMergeRecords)
	}
	if open := tt.open(); open != 0 {
		t.Errorf("%d temp file(s) left open", open)
	}
	checkSorterGoroutinesExit(t, before)
}

// Cancelling in the middle of the merge must reach the error channel, stop the output
// soon after, and stop every merge goroutine.
func TestParallelMergeCancelMidMerge(t *testing.T) {
	before, _ := sorterGoroutines()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tt := &trackedTemp{}
	s := newTrackedSorter(descendingInts(midMergeRecords), itoaBytes, cmp.Compare[int],
		&Config{ChunkSize: midMergeChunkSize, NumWorkers: midMergeWorkers}, tt)
	s.Sort(ctx)
	const read = 5000
	for range read {
		<-s.mergeChunkChan
	}
	cancel()
	got, err := drainWithTimeout(t, s.mergeChunkChan, s.mergeErrChan)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("got error %v, want %v", err, context.Canceled)
	}
	// At most the output channel's buffer, and the record being sent, follow the cancel
	if after := len(got); after > cap(s.mergeChunkChan)+1 {
		t.Errorf("got %d more records after the cancel, want at most %d", after, cap(s.mergeChunkChan)+1)
	}
	if open := tt.open(); open != 0 {
		t.Errorf("%d temp file(s) left open", open)
	}
	checkSorterGoroutinesExit(t, before)
}
