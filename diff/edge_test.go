package diff_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/lanrat/extsort/diff"
)

// Test Generic function with integers
func TestGenericInts(t *testing.T) {
	aChan := make(chan int)
	bChan := make(chan int)
	aErrChan := make(chan error)
	bErrChan := make(chan error)

	var results []string
	resultF := func(d diff.Delta, i int) error {
		results = append(results, fmt.Sprintf("%s %d", d, i))
		return nil
	}

	compareF := func(a, b int) int {
		if a < b {
			return -1
		}
		if a > b {
			return 1
		}
		return 0
	}

	go func() {
		aChan <- 1
		aChan <- 3
		aChan <- 5
		close(aChan)
		close(aErrChan)
	}()

	go func() {
		bChan <- 2
		bChan <- 3
		bChan <- 4
		close(bChan)
		close(bErrChan)
	}()

	r, err := diff.Generic(context.Background(), aChan, bChan, aErrChan, bErrChan, compareF, resultF)
	if err != nil {
		t.Fatal(err)
	}

	if r.ExtraA != 2 || r.ExtraB != 2 || r.Common != 1 {
		t.Fatalf("unexpected result counts: %s", r.String())
	}

	expected := []string{"< 1", "> 2", "> 4", "< 5"}
	if len(results) != len(expected) {
		t.Fatalf("expected %d results, got %d", len(expected), len(results))
	}
}

// Test Ordered function with strings
func TestOrderedStrings(t *testing.T) {
	aChan := make(chan string)
	bChan := make(chan string)
	aErrChan := make(chan error)
	bErrChan := make(chan error)

	var results []string
	resultF := func(d diff.Delta, s string) error {
		results = append(results, fmt.Sprintf("%s %s", d, s))
		return nil
	}

	go func() {
		aChan <- "apple"
		aChan <- "cherry"
		close(aChan)
		close(aErrChan)
	}()

	go func() {
		bChan <- "banana"
		bChan <- "cherry"
		bChan <- "date"
		close(bChan)
		close(bErrChan)
	}()

	r, err := diff.Ordered(context.Background(), aChan, bChan, aErrChan, bErrChan, resultF)
	if err != nil {
		t.Fatal(err)
	}

	if r.ExtraA != 1 || r.ExtraB != 2 || r.Common != 1 {
		t.Fatalf("unexpected result counts: %s", r.String())
	}
}

// Test context cancellation
func TestContextCancellation(t *testing.T) {
	aChan := make(chan string)
	bChan := make(chan string)
	aErrChan := make(chan error)
	bErrChan := make(chan error)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	resultF := func(d diff.Delta, s string) error {
		return nil
	}

	// Don't close channels to force timeout
	go func() {
		time.Sleep(50 * time.Millisecond)
		close(aChan)
		close(bChan)
		close(aErrChan)
		close(bErrChan)
	}()

	_, err := diff.Strings(ctx, aChan, bChan, aErrChan, bErrChan, resultF)
	if err == nil {
		t.Fatal("expected context timeout error")
	}
	if err != context.DeadlineExceeded {
		t.Fatalf("expected DeadlineExceeded, got %v", err)
	}
}

// Test ResultFunc error propagation
func TestResultFuncError(t *testing.T) {
	aChan := make(chan string)
	bChan := make(chan string)
	aErrChan := make(chan error)
	bErrChan := make(chan error)

	testErr := fmt.Errorf("result function error")
	resultF := func(d diff.Delta, s string) error {
		if s == "error" {
			return testErr
		}
		return nil
	}

	go func() {
		aChan <- "error"
		close(aChan)
		close(aErrChan)
	}()

	go func() {
		close(bChan)
		close(bErrChan)
	}()

	_, err := diff.Strings(context.Background(), aChan, bChan, aErrChan, bErrChan, resultF)
	if err != testErr {
		t.Fatalf("expected result function error, got %v", err)
	}
}

// Test empty channels
func TestEmptyChannels(t *testing.T) {
	aChan := make(chan string)
	bChan := make(chan string)
	aErrChan := make(chan error)
	bErrChan := make(chan error)

	resultF := func(d diff.Delta, s string) error {
		t.Fatalf("result function should not be called for empty channels")
		return nil
	}

	close(aChan)
	close(bChan)
	close(aErrChan)
	close(bErrChan)

	r, err := diff.Strings(context.Background(), aChan, bChan, aErrChan, bErrChan, resultF)
	if err != nil {
		t.Fatal(err)
	}

	if r.ExtraA != 0 || r.ExtraB != 0 || r.TotalA != 0 || r.TotalB != 0 || r.Common != 0 {
		t.Fatalf("expected all zero counts, got %s", r.String())
	}
}

// Test PrintDiff utility function
func TestPrintDiff(t *testing.T) {
	// Test that PrintDiff doesn't panic and returns nil error
	err := diff.PrintDiff(diff.NEW, "test")
	if err != nil {
		t.Fatalf("PrintDiff returned error: %v", err)
	}

	err = diff.PrintDiff(diff.OLD, 42)
	if err != nil {
		t.Fatalf("PrintDiff returned error: %v", err)
	}
}

// Test StringResultChan functionality
func TestStringResultChan(t *testing.T) {
	resultFunc, resultChan := diff.StringResultChan()

	aChan := make(chan string)
	bChan := make(chan string)
	aErrChan := make(chan error)
	bErrChan := make(chan error)

	go func() {
		aChan <- "only_in_a"
		close(aChan)
		close(aErrChan)
	}()

	go func() {
		bChan <- "only_in_b"
		close(bChan)
		close(bErrChan)
	}()

	// Start diff operation in background
	go func() {
		defer close(resultChan)
		_, err := diff.Strings(context.Background(), aChan, bChan, aErrChan, bErrChan, resultFunc)
		if err != nil {
			t.Errorf("diff error: %v", err)
		}
	}()

	// Collect results from channel
	var results []*diff.StringChanResult
	for result := range resultChan {
		results = append(results, result)
	}

	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}

	// Verify we got both differences
	hasOldA := false
	hasNewB := false
	for _, result := range results {
		if result.D == diff.OLD && result.S == "only_in_a" {
			hasOldA = true
		}
		if result.D == diff.NEW && result.S == "only_in_b" {
			hasNewB = true
		}
	}

	if !hasOldA || !hasNewB {
		t.Fatal("missing expected diff results")
	}
}

// Test with large dataset to check performance characteristics
func TestLargeDataset(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping large dataset test in short mode")
	}

	const size = 10000
	aChan := make(chan string, 100)
	bChan := make(chan string, 100)
	aErrChan := make(chan error)
	bErrChan := make(chan error)

	resultCount := 0
	resultF := func(d diff.Delta, s string) error {
		resultCount++
		return nil
	}

	go func() {
		// Generate overlapping sequences with zero-padded strings for proper lexicographic sorting
		for i := 0; i < size; i++ {
			aChan <- fmt.Sprintf("%05d", i)  // Zero-pad to 5 digits
		}
		close(aChan)
		close(aErrChan)
	}()

	go func() {
		// Generate overlapping sequences with offset
		for i := size / 2; i < size+size/2; i++ {
			bChan <- fmt.Sprintf("%05d", i)  // Zero-pad to 5 digits
		}
		close(bChan)
		close(bErrChan)
	}()

	r, err := diff.Strings(context.Background(), aChan, bChan, aErrChan, bErrChan, resultF)
	if err != nil {
		t.Fatal(err)
	}

	// Should have size/2 unique in A, size/2 unique in B, and size/2 common
	expectedCommon := size / 2
	expectedExtraA := size / 2
	expectedExtraB := size / 2

	if r.Common != uint64(expectedCommon) {
		t.Errorf("expected %d common items, got %d", expectedCommon, r.Common)
	}
	if r.ExtraA != uint64(expectedExtraA) {
		t.Errorf("expected %d extra A items, got %d", expectedExtraA, r.ExtraA)
	}
	if r.ExtraB != uint64(expectedExtraB) {
		t.Errorf("expected %d extra B items, got %d", expectedExtraB, r.ExtraB)
	}

	if resultCount != expectedExtraA+expectedExtraB {
		t.Errorf("expected %d result callbacks, got %d", expectedExtraA+expectedExtraB, resultCount)
	}
}
