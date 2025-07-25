package extsort_test

// this test package is heavily bases on the one for psilva261's timsort
// https://github.com/psilva261/timsort/blob/master/timsort_test.go

import (
	"context"
	"encoding/json"
	"math/rand"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lanrat/extsort"
)

func fromBytesForTest(data []byte) extsort.SortType {
	var v val
	err := json.Unmarshal(data, &v)
	if err != nil {
		panic(err)
	}
	return v
}

func (v val) ToBytes() []byte {
	bytes, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return bytes
}

func sortForTest(inputData []val, lessFunc extsort.CompareLessFunc) error {
	// make array of all data in chan
	inputChan := make(chan extsort.SortType, 2)
	go func() {
		for _, d := range inputData {
			inputChan <- d
		}
		close(inputChan)
	}()
	config := extsort.DefaultConfig()
	config.ChunkSize = len(inputData)/20 + 100
	sort, outChan, errChan := extsort.New(inputChan, fromBytesForTest, lessFunc, config)
	sort.Sort(context.Background())
	i := 0
	for rec := range outChan {
		inputData[i] = rec.(val)
		i++
	}
	if err := <-errChan; err != nil {
		return err
	}
	return nil
}

type val struct {
	Key, Order int
}

func makeTestArray(size int) []val {
	a := make([]val, size)

	for i := 0; i < size; i++ {
		a[i] = val{i & 0xeeeeee, i}
	}

	return a
}

func IsSorted(a []val, lessThan extsort.CompareLessFunc) bool {
	len := len(a)

	if len < 2 {
		return true
	}

	prev := a[0]
	for i := 1; i < len; i++ {
		if lessThan(a[i], prev) {
			return false
		}
		prev = a[i]
	}

	return true
}

func TestIsSorted(t *testing.T) {
	a := make([]val, 5)
	a[0] = val{3, 1}
	a[1] = val{1, 5}
	a[2] = val{2, 3}
	a[3] = val{3, 4}
	a[4] = val{4, 5}

	if IsSorted(a, OrderLessThan) {
		t.Error("Sorted")
	}
}

// use this comparator for sorting
func KeyLessThan(a, b extsort.SortType) bool {
	return a.(val).Key < b.(val).Key
}

// use this comparator to validate sorted data (and prove its stable)
func KeyOrderLessThan(ar, br extsort.SortType) bool {
	a := ar.(val)
	b := br.(val)
	if a.Key < b.Key {
		return true
	} else if a.Key == b.Key {
		return a.Order < b.Order
	}

	return false
}

// use this comparator to restore the original order of elements (by sorting on order field)
func OrderLessThan(a, b extsort.SortType) bool {
	return a.(val).Order < b.(val).Order
}

func Test50(t *testing.T) {
	a := makeTestArray(50)
	if IsSorted(a, KeyLessThan) {
		t.Error("sorted before starting")
	}

	err := sortForTest(a, KeyLessThan)
	if err != nil {
		t.Fatalf("sort: %v", err)
	}

	if !IsSorted(a, KeyLessThan) {
		t.Error("not sorted")
	}
}

func TestSmoke(t *testing.T) {
	a := make([]val, 3)
	a[0] = val{3, 0}
	a[1] = val{1, 1}
	a[2] = val{2, 2}

	err := sortForTest(a, KeyLessThan)
	if err != nil {
		t.Fatalf("sort: %v", err)
	}

	if !IsSorted(a, KeyLessThan) {
		t.Error("not sorted")
	}
}

func TestSmokeStability(t *testing.T) {
	a := make([]val, 3)
	a[0] = val{3, 0}
	a[1] = val{2, 1}
	a[2] = val{2, 2}

	err := sortForTest(a, KeyOrderLessThan)
	if err != nil {
		t.Fatalf("sort: %v", err)
	}

	if !IsSorted(a, KeyOrderLessThan) {
		t.Error("not sorted")
	}
}

func Test1K(t *testing.T) {
	a := makeTestArray(1024)

	err := sortForTest(a, KeyOrderLessThan)
	if err != nil {
		t.Fatalf("sort: %v", err)
	}
	if !IsSorted(a, KeyOrderLessThan) {
		t.Error("not sorted")
	}
}

func Test1M(t *testing.T) {
	a := makeTestArray(1024 * 1024)

	err := sortForTest(a, KeyOrderLessThan)
	if err != nil {
		t.Fatalf("sort: %v", err)
	}
	if !IsSorted(a, KeyOrderLessThan) {
		t.Error("not sorted")
	}
}

func makeRandomArray(size int) []val {
	a := make([]val, size)

	for i := 0; i < size; i++ {
		a[i] = val{rand.Intn(100), i}
	}

	return a
}

func Equals(a, b val) bool {
	return a.Key == b.Key && a.Order == b.Order
}

func TestRandom1M(t *testing.T) {
	size := 1024 * 1024

	a := makeRandomArray(size)
	b := make([]val, size)
	copy(b, a)

	err := sortForTest(a, KeyLessThan)
	if err != nil {
		t.Fatalf("sort: %v", err)
	}
	if !IsSorted(a, KeyLessThan) {
		t.Error("not sorted")
	}

	// sort by order
	err = sortForTest(a, OrderLessThan)
	if err != nil {
		t.Fatalf("sort: %v", err)
	}
	for i := 0; i < len(b); i++ {
		if !Equals(b[i], a[i]) {
			t.Error("oops")
		}
	}
}

func TestDeadLockContextCancel(t *testing.T) {
	inputChan := make(chan extsort.SortType, 2)
	config := extsort.DefaultConfig()
	config.ChunkSize = 2
	config.SortedChanBuffSize = 2
	// for simplicity, set ChanBuffSize to zero. the deadlock can happen with any value.
	// see https://github.com/lanrat/extsort/issues/7 for details.
	config.ChanBuffSize = 0
	lessFunc := func(a, b extsort.SortType) bool {
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
	// cancel the context. the sort.Sort should now be waiting inside lessFunc.
	cf()
	select {
	case <-waitCh:
	case <-time.After(time.Second):
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
		
		// Block indefinitely - this simulates a slow comparison that would
		// prevent the sortChunks() function from checking context cancellation
		select {
		case <-time.After(10 * time.Second):
			// This should never happen in a working test
			return false  
		}
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
		cf() // Cancel the context while sort.Sort() is definitely blocked
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
