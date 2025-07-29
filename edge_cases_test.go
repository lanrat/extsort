package extsort_test

import (
	"context"
	"testing"
	"time"

	"github.com/lanrat/extsort"
)

// TestEmptyInput tests sorting an empty input channel
func TestEmptyInput(t *testing.T) {
	inputChan := make(chan extsort.SortType)
	close(inputChan) // Close immediately - empty input

	// Create cancelable context for proper cleanup
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sort, outChan, errChan := extsort.New(inputChan, fromBytesForTest, KeyLessThan, nil)
	sort.Sort(ctx)

	// Should get no output
	var count int
	for {
		select {
		case _, ok := <-outChan:
			if !ok {
				if err := <-errChan; err != nil {
					t.Fatalf("unexpected error with empty input: %v", err)
				}
				goto done2
			}
			count++
		case err := <-errChan:
			if err != nil {
				t.Fatalf("unexpected error with empty input: %v", err)
			}
			for range outChan {
				count++
			}
			goto done2
		}
	}
done2:

	if count != 0 {
		t.Fatalf("expected 0 results, got %d", count)
	}
}

// TestSingleElement tests sorting a single element
func TestSingleElement(t *testing.T) {
	inputChan := make(chan extsort.SortType, 1)
	inputChan <- val{Key: 42, Order: 1}
	close(inputChan)

	// Create cancelable context for proper cleanup
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sort, outChan, errChan := extsort.New(inputChan, fromBytesForTest, KeyLessThan, nil)
	sort.Sort(ctx)

	var results []val
	for {
		select {
		case rec, ok := <-outChan:
			if !ok {
				if err := <-errChan; err != nil {
					t.Fatalf("unexpected error with single element: %v", err)
				}
				goto done3
			}
			results = append(results, rec.(val))
		case err := <-errChan:
			if err != nil {
				t.Fatalf("unexpected error with single element: %v", err)
			}
			for rec := range outChan {
				results = append(results, rec.(val))
			}
			goto done3
		}
	}
done3:

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}

	if results[0].Key != 42 || results[0].Order != 1 {
		t.Fatalf("single element corrupted: got %+v", results[0])
	}
}

// TestAllIdenticalElements tests sorting when all elements are identical
func TestAllIdenticalElements(t *testing.T) {
	inputChan := make(chan extsort.SortType, 100)

	// Add 50 identical elements
	for i := 0; i < 50; i++ {
		inputChan <- val{Key: 7, Order: i} // Same key, different order
	}
	close(inputChan)

	// Create cancelable context for proper cleanup
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sort, outChan, errChan := extsort.New(inputChan, fromBytesForTest, KeyLessThan, nil)
	sort.Sort(ctx)

	var results []val
	for {
		select {
		case rec, ok := <-outChan:
			if !ok {
				if err := <-errChan; err != nil {
					t.Fatalf("unexpected error with identical elements: %v", err)
				}
				goto done1
			}
			results = append(results, rec.(val))
		case err := <-errChan:
			if err != nil {
				t.Fatalf("unexpected error with identical elements: %v", err)
			}
			for rec := range outChan {
				results = append(results, rec.(val))
			}
			goto done1
		}
	}
done1:

	if len(results) != 50 {
		t.Fatalf("expected 50 results, got %d", len(results))
	}

	// All should have same key
	for i, result := range results {
		if result.Key != 7 {
			t.Fatalf("element %d has wrong key: got %d, expected 7", i, result.Key)
		}
	}
}

// TestInvalidConfiguration tests error handling with invalid configs
func TestInvalidConfiguration(t *testing.T) {
	inputChan := make(chan extsort.SortType, 1)
	inputChan <- val{Key: 1, Order: 1}
	close(inputChan)

	// Test with invalid config values
	config := extsort.DefaultConfig()
	config.ChunkSize = 0       // Invalid
	config.NumWorkers = 0      // Invalid
	config.NumMergeWorkers = 0 // Invalid

	sort, outChan, errChan := extsort.New(inputChan, fromBytesForTest, KeyLessThan, config)
	sort.Sort(context.Background())

	// Should still work because mergeConfig fixes invalid values
	var count int
	for range outChan {
		count++
	}

	if err := <-errChan; err != nil {
		t.Fatalf("unexpected error - mergeConfig should fix invalid values: %v", err)
	}

	if count != 1 {
		t.Fatalf("expected 1 result, got %d", count)
	}
}

// TestContextCancellationDuringBuild tests context cancellation during chunk building
func TestContextCancellationDuringBuild(t *testing.T) {
	inputChan := make(chan extsort.SortType)

	sort, outChan, errChan := extsort.New(inputChan, fromBytesForTest, KeyLessThan, nil)

	ctx, cancel := context.WithCancel(context.Background())

	// Start sorting in background
	go sort.Sort(ctx)

	// Add one element then cancel before adding more
	inputChan <- val{Key: 1, Order: 1}
	cancel() // Cancel during build phase
	close(inputChan)

	// Drain channels
	for range outChan {
		// Consume any output
	}

	// Should get a context cancellation error
	if err := <-errChan; err == nil {
		t.Fatal("expected context cancellation error, got nil")
	} else if err != context.Canceled {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

// TestConcurrentSorters tests multiple sorters running simultaneously
func TestConcurrentSorters(t *testing.T) {
	const numSorters = 3
	const elementsPerSorter = 100

	// Create channels for synchronization
	results := make(chan []val, numSorters)
	errors := make(chan error, numSorters)

	// Start multiple sorters concurrently
	for i := 0; i < numSorters; i++ {
		go func(sorterId int) {
			inputChan := make(chan extsort.SortType, elementsPerSorter)

			// Add unique data for each sorter
			for j := 0; j < elementsPerSorter; j++ {
				inputChan <- val{Key: (sorterId*1000 + j) % 50, Order: j}
			}
			close(inputChan)

			sort, outChan, errChan := extsort.New(inputChan, fromBytesForTest, KeyLessThan, nil)
			sort.Sort(context.Background())

			var sortResults []val
			for rec := range outChan {
				sortResults = append(sortResults, rec.(val))
			}

			if err := <-errChan; err != nil {
				errors <- err
				return
			}

			results <- sortResults
		}(i)
	}

	// Collect results
	for i := 0; i < numSorters; i++ {
		select {
		case err := <-errors:
			t.Fatalf("sorter %d failed: %v", i, err)
		case result := <-results:
			if len(result) != elementsPerSorter {
				t.Fatalf("sorter %d returned %d elements, expected %d", i, len(result), elementsPerSorter)
			}

			// Verify sorted
			for j := 1; j < len(result); j++ {
				if result[j-1].Key > result[j].Key {
					t.Fatalf("sorter %d result not sorted at position %d", i, j)
				}
			}
		}
	}
}

// TestNilConfiguration tests behavior with nil config
func TestNilConfiguration(t *testing.T) {
	inputChan := make(chan extsort.SortType, 5)
	for i := 0; i < 5; i++ {
		inputChan <- val{Key: 5 - i, Order: i}
	}
	close(inputChan)

	// Pass nil config - should use defaults
	sort, outChan, errChan := extsort.New(inputChan, fromBytesForTest, KeyLessThan, nil)
	sort.Sort(context.Background())

	var results []val
	for rec := range outChan {
		results = append(results, rec.(val))
	}

	if err := <-errChan; err != nil {
		t.Fatalf("unexpected error with nil config: %v", err)
	}

	if len(results) != 5 {
		t.Fatalf("expected 5 results, got %d", len(results))
	}

	// Verify sorted
	if !IsSorted(results, KeyLessThan) {
		t.Fatal("results not sorted with nil config")
	}
}

// TestVerySmallChunkSize tests with chunk size of 1
func TestVerySmallChunkSize(t *testing.T) {
	inputChan := make(chan extsort.SortType, 10)
	for i := 0; i < 10; i++ {
		inputChan <- val{Key: 10 - i, Order: i}
	}
	close(inputChan)

	config := extsort.DefaultConfig()
	config.ChunkSize = 1 // Very small chunks

	sort, outChan, errChan := extsort.New(inputChan, fromBytesForTest, KeyLessThan, config)
	sort.Sort(context.Background())

	var results []val
	for rec := range outChan {
		results = append(results, rec.(val))
	}

	if err := <-errChan; err != nil {
		t.Fatalf("unexpected error with chunk size 1: %v", err)
	}

	if len(results) != 10 {
		t.Fatalf("expected 10 results, got %d", len(results))
	}

	// Verify sorted
	if !IsSorted(results, KeyLessThan) {
		t.Fatal("results not sorted with chunk size 1")
	}
}

// TestContextTimeout tests behavior with context timeout
func TestContextTimeout(t *testing.T) {
	inputChan := make(chan extsort.SortType, 10)

	// Use a very slow comparison function
	lessFunc := func(a, b extsort.SortType) bool {
		time.Sleep(100 * time.Millisecond) // Slow comparison
		return a.(val).Key < b.(val).Key
	}

	for i := 0; i < 10; i++ {
		inputChan <- val{Key: 10 - i, Order: i}
	}
	close(inputChan)

	sort, outChan, errChan := extsort.New(inputChan, fromBytesForTest, lessFunc, nil)

	// Use a very short timeout that should expire during sorting
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	sort.Sort(ctx)

	// Drain output
	for range outChan {
		// Consume any output
	}

	// Should get a timeout error
	if err := <-errChan; err == nil {
		t.Fatal("expected timeout error, got nil")
	} else if err != context.DeadlineExceeded {
		t.Fatalf("expected context.DeadlineExceeded, got %v", err)
	}
}

// TestExtremeParallelism tests with high worker counts
func TestExtremeParallelism(t *testing.T) {
	inputChan := make(chan extsort.SortType, 100)
	for i := 0; i < 100; i++ {
		inputChan <- val{Key: 100 - i, Order: i}
	}
	close(inputChan)

	config := extsort.DefaultConfig()
	config.NumWorkers = 10     // High worker count
	config.NumMergeWorkers = 8 // High merge worker count
	config.ChunkSize = 5       // Small chunks to force parallelism

	sort, outChan, errChan := extsort.New(inputChan, fromBytesForTest, KeyLessThan, config)
	sort.Sort(context.Background())

	var results []val
	for rec := range outChan {
		results = append(results, rec.(val))
	}

	if err := <-errChan; err != nil {
		t.Fatalf("unexpected error with extreme parallelism: %v", err)
	}

	if len(results) != 100 {
		t.Fatalf("expected 100 results, got %d", len(results))
	}

	// Verify sorted
	if !IsSorted(results, KeyLessThan) {
		t.Fatal("results not sorted with extreme parallelism")
	}
}
