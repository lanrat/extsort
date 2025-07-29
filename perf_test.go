package extsort_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lanrat/extsort"
)

func TestDeadLockContextCancel(t *testing.T) {
	inputChan := make(chan extsort.SortType, 2)
	config := extsort.DefaultConfig()
	config.ChunkSize = 2
	config.SortedChanBuffSize = 2
	// for simplicity, set ChanBuffSize to zero. the deadlock can happen with any value.
	// see https://github.com/lanrat/extsort/issues/7 for details.
	config.ChanBuffSize = 0
	config.NumWorkers = 1 // Single worker for predictability

	sortStarted := make(chan struct{})
	lessFunc := func(a, b extsort.SortType) bool {
		select {
		case <-sortStarted:
			// Already signaled
		default:
			close(sortStarted)
		}
		time.Sleep(300 * time.Millisecond) // emulate long operation
		return false
	}
	sort, _, _ := extsort.New(inputChan, fromBytesForTest, lessFunc, config)
	ctx, cf := context.WithCancel(context.Background())
	defer cf()
	waitCh := make(chan struct{})
	go func() {
		defer close(waitCh)
		sort.Sort(ctx)
	}()
	inputChan <- val{Key: 1, Order: 1}
	inputChan <- val{Key: 2, Order: 2}
	close(inputChan)

	// Wait for sort to start before cancelling
	<-sortStarted
	time.Sleep(50 * time.Millisecond) // Ensure we're in the middle of sort

	// cancel the context. the sort.Sort should now be waiting inside lessFunc.
	cf()
	select {
	case <-waitCh:
	case <-time.After(2 * time.Second): // Give more time for sort to complete
		t.Fatal("deadlock")
	}
}

// TestDeadLockContextCancelDeterministic tests that context cancellation during sorting
// is handled correctly. This test guarantees the deadlock by using synchronization
// to ensure cancellation happens while sort.Sort() is blocked in the comparison function.
func TestDeadLockContextCancelDeterministic(t *testing.T) {
	inputChan := make(chan extsort.SortType, 10)
	config := extsort.DefaultConfig()
	config.ChunkSize = 2
	config.SortedChanBuffSize = 2
	config.ChanBuffSize = 0
	config.NumWorkers = 1 // Single worker to make deadlock more deterministic

	// Synchronization: ensure we cancel exactly when sort.Sort() is blocked
	sortInProgress := make(chan struct{})
	var sortProgressCounter int32

	// Comparison function that blocks indefinitely until context is cancelled
	lessFunc := func(a, b extsort.SortType) bool {
		// Signal that we're now inside sort.Sort()
		if atomic.AddInt32(&sortProgressCounter, 1) == 1 {
			close(sortInProgress)
		}

		// Block for a long time - this simulates a slow comparison that would
		// prevent the sortChunks() function from checking context cancellation
		time.Sleep(10 * time.Second)
		// This should never happen in a working test
		return false
	}

	sort, _, _ := extsort.New(inputChan, fromBytesForTest, lessFunc, config)
	ctx, cf := context.WithCancel(context.Background())
	defer cf()

	// Add exactly enough data to trigger one chunk that needs sorting
	inputChan <- val{Key: 2, Order: 1}
	inputChan <- val{Key: 1, Order: 2}
	close(inputChan)

	waitCh := make(chan struct{})
	go func() {
		defer close(waitCh)
		sort.Sort(ctx)
	}()

	// Wait for the sort operation to be blocked in lessFunc
	select {
	case <-sortInProgress:
		// Now we know sort.Sort() is blocked in the comparison function
		time.Sleep(50 * time.Millisecond) // Ensure it's well into the blocking call
		cf()                              // Cancel the context while sort.Sort() is definitely blocked
	case <-time.After(5 * time.Second):
		cf()
		t.Fatal("sort operation never started")
	}

	// With the deadlock bug, this will timeout because sortChunks() cannot
	// check context cancellation while blocked in sort.Sort()
	select {
	case <-waitCh:
		// Sort completed - this should only happen with the fix
	case <-time.After(3 * time.Second):
		t.Fatal("deadlock detected - context cancellation not handled during sort.Sort()")
	}
}

// TestParallelMerging tests that parallel merging is used for large datasets
func TestParallelMerging(t *testing.T) {
	inputChan := make(chan extsort.SortType, 100)
	config := extsort.DefaultConfig()
	config.ChunkSize = 5       // Small chunks to force many chunks
	config.NumMergeWorkers = 3 // Force parallel merging

	// Add enough data to trigger parallel merging (more chunks than merge workers)
	for i := 0; i < 20; i++ {
		inputChan <- val{Key: 20 - i, Order: i} // Reverse order to test sorting
	}
	close(inputChan)

	// Create cancelable context for proper cleanup
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sort, outChan, errChan := extsort.New(inputChan, fromBytesForTest, KeyLessThan, config)
	sort.Sort(ctx)

	// Collect and verify results
	var results []val
	for {
		select {
		case rec, ok := <-outChan:
			if !ok {
				if err := <-errChan; err != nil {
					t.Fatalf("sort error: %v", err)
				}
				goto done
			}
			results = append(results, rec.(val))
		case err := <-errChan:
			if err != nil {
				t.Fatalf("sort error: %v", err)
			}
			for rec := range outChan {
				results = append(results, rec.(val))
			}
			goto done
		}
	}
done:

	// Verify sorting worked correctly
	if len(results) != 20 {
		t.Fatalf("expected 20 results, got %d", len(results))
	}

	for i := 1; i < len(results); i++ {
		if results[i-1].Key > results[i].Key {
			t.Fatalf("results not sorted: %d > %d at positions %d, %d",
				results[i-1].Key, results[i].Key, i-1, i)
		}
	}
}

// TestMemoryPooling tests that memory pools reduce allocations
func TestMemoryPooling(t *testing.T) {
	inputChan := make(chan extsort.SortType, 50)
	config := extsort.DefaultConfig()
	config.ChunkSize = 10 // Small chunks to force pool reuse

	// Add data that will create multiple chunks
	for i := 0; i < 30; i++ {
		inputChan <- val{Key: 30 - i, Order: i}
	}
	close(inputChan)

	// Create cancelable context for proper cleanup
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sort, outChan, errChan := extsort.New(inputChan, fromBytesForTest, KeyLessThan, config)
	sort.Sort(ctx)

	// Drain output
	var count int
	for {
		select {
		case _, ok := <-outChan:
			if !ok {
				if err := <-errChan; err != nil {
					t.Fatalf("sort error: %v", err)
				}
				goto done
			}
			count++
		case err := <-errChan:
			if err != nil {
				t.Fatalf("sort error: %v", err)
			}
			for range outChan {
				count++
			}
			goto done
		}
	}
done:

	if count != 30 {
		t.Fatalf("expected 30 results, got %d", count)
	}

	// Test passes if no panics occurred and sorting worked correctly
	// The real benefit is reduced GC pressure which is harder to test directly
}
