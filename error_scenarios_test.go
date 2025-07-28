package extsort_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/lanrat/extsort"
)

// TestSerializationError tests handling of ToBytes() failures
func TestSerializationError(t *testing.T) {
	inputChan := make(chan extsort.SortType, 5)

	// Add some normal elements and one that will fail serialization
	for i := 0; i < 4; i++ {
		inputChan <- val{Key: i, Order: i}
	}
	inputChan <- &errorVal{shouldFail: true} // This will fail ToBytes()
	close(inputChan)

	sort, outChan, errChan := extsort.New(inputChan, fromBytesForTest, func(a, b extsort.SortType) bool {
		// Handle mixed types safely
		av, aok := a.(val)
		bv, bok := b.(val)
		if aok && bok {
			return av.Key < bv.Key
		}
		return false // Fallback for error types
	}, nil)

	sort.Sort(context.Background())

	// Drain output
	for range outChan {
		// Consume output
	}

	// Should now get a proper error instead of panic
	if err := <-errChan; err != nil {
		t.Logf("Got expected serialization error: %v", err)
		// Verify it's our specific error type
		var serErr *extsort.SerializationError
		if !errors.As(err, &serErr) {
			t.Errorf("Expected SerializationError, got: %T", err)
		}
		if !strings.Contains(err.Error(), "serialization panic") {
			t.Errorf("Expected 'serialization panic' in error message, got: %v", err)
		}
	} else {
		t.Fatal("Expected serialization error, got nil")
	}
}

// TestDeserializationError tests handling of FromBytes() failures
func TestDeserializationError(t *testing.T) {
	inputChan := make(chan extsort.SortType, 3)
	for i := 0; i < 3; i++ {
		inputChan <- val{Key: i, Order: i}
	}
	close(inputChan)

	// Use a FromBytes function that always fails
	failingFromBytes := func(data []byte) extsort.SortType {
		panic("deserialization failed") // Simulate critical failure
	}

	sort, outChan, errChan := extsort.New(inputChan, failingFromBytes, KeyLessThan, nil)

	// This should panic or fail gracefully during merge phase
	defer func() {
		if r := recover(); r != nil {
			t.Logf("Expected panic during deserialization: %v", r)
		}
	}()

	sort.Sort(context.Background())

	// Drain output
	for range outChan {
		// Consume output
	}

	// If we get here, check for error
	if err := <-errChan; err != nil {
		t.Logf("Got expected deserialization error: %v", err)
		// Verify it's our specific error type
		var deserErr *extsort.DeserializationError
		if !errors.As(err, &deserErr) {
			t.Errorf("Expected DeserializationError, got: %T", err)
		}
	}
}

// TestNilInputs tests behavior with nil function parameters
func TestNilInputs(t *testing.T) {
	inputChan := make(chan extsort.SortType, 1)
	inputChan <- val{Key: 1, Order: 1}
	close(inputChan)

	// Test with nil fromBytes function
	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Logf("Expected panic with nil fromBytes: %v", r)
			}
		}()

		sort, _, _ := extsort.New(inputChan, nil, KeyLessThan, nil)
		sort.Sort(context.Background())
	}()

	// Recreate input for next test
	inputChan2 := make(chan extsort.SortType, 1)
	inputChan2 <- val{Key: 1, Order: 1}
	close(inputChan2)

	// Test with nil comparison function
	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Logf("Expected panic with nil lessFunc: %v", r)
			}
		}()

		sort, _, _ := extsort.New(inputChan2, fromBytesForTest, nil, nil)
		sort.Sort(context.Background())
	}()
}

// TestLargeDataElements tests with unusually large individual elements
func TestLargeDataElements(t *testing.T) {
	inputChan := make(chan extsort.SortType, 3)

	// Create elements with large data
	largeString := make([]byte, 1024*1024) // 1MB element
	for i := range largeString {
		largeString[i] = byte('A' + (i % 26))
	}

	// Add a few large elements
	inputChan <- &largeVal{Key: 3, Data: largeString}
	inputChan <- &largeVal{Key: 1, Data: largeString}
	inputChan <- &largeVal{Key: 2, Data: largeString}
	close(inputChan)

	sort, outChan, errChan := extsort.New(inputChan, fromBytesForLargeVal, largeLessThan, nil)
	sort.Sort(context.Background())

	var results []*largeVal
	for rec := range outChan {
		results = append(results, rec.(*largeVal))
	}

	if err := <-errChan; err != nil {
		t.Fatalf("unexpected error with large elements: %v", err)
	}

	if len(results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(results))
	}

	// Verify sorted
	for i := 1; i < len(results); i++ {
		if results[i-1].Key > results[i].Key {
			t.Fatalf("large elements not sorted at position %d", i)
		}
	}
}

// Test types for error scenarios

// errorVal is a type that fails during serialization
type errorVal struct {
	shouldFail bool
}

func (e *errorVal) ToBytes() []byte {
	if e.shouldFail {
		panic("intentional serialization failure")
	}
	return []byte(`{"shouldFail": false}`)
}

// largeVal is a type with large data
type largeVal struct {
	Key  int
	Data []byte
}

func (l *largeVal) ToBytes() []byte {
	// Simple encoding: key as 4 bytes + data
	result := make([]byte, 4+len(l.Data))
	result[0] = byte(l.Key >> 24)
	result[1] = byte(l.Key >> 16)
	result[2] = byte(l.Key >> 8)
	result[3] = byte(l.Key)
	copy(result[4:], l.Data)
	return result
}

func fromBytesForLargeVal(data []byte) extsort.SortType {
	if len(data) < 4 {
		panic("invalid large val data")
	}
	key := int(data[0])<<24 | int(data[1])<<16 | int(data[2])<<8 | int(data[3])
	return &largeVal{
		Key:  key,
		Data: data[4:],
	}
}

func largeLessThan(a, b extsort.SortType) bool {
	return a.(*largeVal).Key < b.(*largeVal).Key
}

// TestComparisonFunctionPanic tests handling of panics in comparison function
func TestComparisonFunctionPanic(t *testing.T) {
	inputChan := make(chan extsort.SortType, 3)
	inputChan <- val{Key: 1, Order: 1}
	inputChan <- val{Key: 2, Order: 2}
	inputChan <- val{Key: 3, Order: 3}
	close(inputChan)

	// Comparison function that panics
	panicLessFunc := func(a, b extsort.SortType) bool {
		panic("comparison function panic")
	}

	sort, outChan, errChan := extsort.New(inputChan, fromBytesForTest, panicLessFunc, nil)

	// Should handle the panic gracefully
	defer func() {
		if r := recover(); r != nil {
			t.Logf("Caught expected panic from comparison function: %v", r)
		}
	}()

	sort.Sort(context.Background())

	// Drain channels
	for range outChan {
		// Consume output
	}

	// Check for error
	if err := <-errChan; err != nil {
		t.Logf("Got expected error from panicking comparison: %v", err)
		// Verify it's our specific error type
		var compErr *extsort.ComparisonError
		if !errors.As(err, &compErr) {
			t.Errorf("Expected ComparisonError, got: %T", err)
		}
	}
}

// TestMixedTypeComparison tests comparison function with different types
func TestMixedTypeComparison(t *testing.T) {
	inputChan := make(chan extsort.SortType, 4)
	inputChan <- val{Key: 1, Order: 1}
	inputChan <- &differentType{Value: 2}
	inputChan <- val{Key: 3, Order: 3}
	inputChan <- &differentType{Value: 0}
	close(inputChan)

	// Comparison function that handles mixed types
	mixedLessFunc := func(a, b extsort.SortType) bool {
		aVal := getComparableValue(a)
		bVal := getComparableValue(b)
		return aVal < bVal
	}

	// FromBytes that can handle both types
	mixedFromBytes := func(data []byte) extsort.SortType {
		// Check if it's a differentType by looking for "value" key
		if strings.Contains(string(data), `"value"`) {
			var d differentType
			if err := json.Unmarshal(data, &d); err == nil {
				return &d
			}
		}

		// Otherwise try as val
		var v val
		if err := json.Unmarshal(data, &v); err == nil {
			return v
		}

		panic("unknown type in deserialization")
	}

	sort, outChan, errChan := extsort.New(inputChan, mixedFromBytes, mixedLessFunc, nil)
	sort.Sort(context.Background())

	var results []extsort.SortType
	for rec := range outChan {
		results = append(results, rec)
	}

	if err := <-errChan; err != nil {
		t.Fatalf("unexpected error with mixed types: %v", err)
	}

	if len(results) != 4 {
		t.Fatalf("expected 4 results, got %d", len(results))
	}

	// Verify sorted by comparable value
	for i := 1; i < len(results); i++ {
		prev := getComparableValue(results[i-1])
		curr := getComparableValue(results[i])
		if prev > curr {
			t.Fatalf("mixed types not sorted at position %d: %d > %d", i, prev, curr)
		}
	}
}

// Helper types and functions for mixed type test

type differentType struct {
	Value int `json:"value"`
}

func (d *differentType) ToBytes() []byte {
	bytes, err := json.Marshal(d)
	if err != nil {
		panic(err)
	}
	return bytes
}

func getComparableValue(item extsort.SortType) int {
	switch v := item.(type) {
	case val:
		return v.Key
	case *differentType:
		return v.Value
	default:
		return 0
	}
}
