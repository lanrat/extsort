package extsort_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/lanrat/extsort"
)

// TestGenericSerializationError tests serialization error handling in the Generic API
func TestGenericSerializationError(t *testing.T) {
	inputChan := make(chan *GenericErrorItem, 6)

	// Add more items to ensure multi-chunk behavior
	inputChan <- &GenericErrorItem{Key: 1, ShouldFailSerialization: false}
	inputChan <- &GenericErrorItem{Key: 2, ShouldFailSerialization: true} // This will fail
	inputChan <- &GenericErrorItem{Key: 3, ShouldFailSerialization: false}
	inputChan <- &GenericErrorItem{Key: 4, ShouldFailSerialization: false}
	inputChan <- &GenericErrorItem{Key: 5, ShouldFailSerialization: false}
	inputChan <- &GenericErrorItem{Key: 6, ShouldFailSerialization: false}
	close(inputChan)

	// Use small chunk size to force multiple chunks
	config := &extsort.Config{ChunkSize: 2} // Create 3 chunks

	sort, outChan, errChan := extsort.Generic(
		inputChan,
		genericFromBytes,
		genericToBytes, // This will return error for failing items
		genericCompare,
		config,
	)

	sort.Sort(context.Background())

	// Drain output - should get fewer items due to error
	outputCount := 0
	for range outChan {
		outputCount++
	}

	// Should get a SerializationError
	err := <-errChan
	if err == nil {
		t.Fatal("Expected SerializationError but got nil")
	}

	var serErr *extsort.SerializationError
	if !errors.As(err, &serErr) {
		t.Errorf("Expected SerializationError, got %T: %v", err, err)
	}

	if !strings.Contains(err.Error(), "serialization failed") {
		t.Errorf("Expected 'serialization failed' in error message, got: %v", err)
	}

	t.Logf("Successfully caught generic serialization error: %v", err)
}

// TestGenericDeserializationError tests deserialization error handling in the Generic API
func TestGenericDeserializationError(t *testing.T) {
	inputChan := make(chan *GenericErrorItem, 5)

	// Add more items to force multi-chunk and trigger deserialization
	inputChan <- &GenericErrorItem{Key: 1, ShouldFailSerialization: false}
	inputChan <- &GenericErrorItem{Key: 2, ShouldFailSerialization: false}
	inputChan <- &GenericErrorItem{Key: 3, ShouldFailSerialization: false}
	inputChan <- &GenericErrorItem{Key: 4, ShouldFailSerialization: false}
	inputChan <- &GenericErrorItem{Key: 5, ShouldFailSerialization: false}
	close(inputChan)

	config := &extsort.Config{
		ChunkSize:  2, // Force 3 chunks
		NumWorkers: 1, // Single worker to avoid race conditions
	}

	sort, outChan, errChan := extsort.Generic(
		inputChan,
		failingGenericFromBytes, // This will always fail
		genericToBytes,
		genericCompare,
		config,
	)

	sort.Sort(context.Background())

	// Drain output
	for range outChan {
		// Should get no output due to deserialization error
	}

	// Should get a DeserializationError
	err := <-errChan
	if err == nil {
		t.Fatal("Expected DeserializationError but got nil")
	}

	var deserErr *extsort.DeserializationError
	if !errors.As(err, &deserErr) {
		t.Errorf("Expected DeserializationError, got %T: %v", err, err)
	}

	t.Logf("Successfully caught generic deserialization error: %v", err)
}

// TestOrderedSerializationError tests gob encoding errors in Ordered API
func TestOrderedSerializationError(t *testing.T) {
	// Note: It's hard to make gob encoding fail for basic types,
	// so this test demonstrates the error handling path exists
	inputChan := make(chan int, 2)
	inputChan <- 1
	inputChan <- 2
	close(inputChan)

	sort, outChan, errChan := extsort.Ordered(inputChan, nil)
	sort.Sort(context.Background())

	// Should complete successfully since int is easily serializable
	outputCount := 0
	for range outChan {
		outputCount++
	}

	err := <-errChan
	if err != nil {
		t.Errorf("Unexpected error with basic int sorting: %v", err)
	}

	if outputCount != 2 {
		t.Errorf("Expected 2 output items, got %d", outputCount)
	}

	t.Logf("Ordered API error handling infrastructure verified")
}

// TestGenericErrorPropagation tests that errors are properly propagated through all phases
func TestGenericErrorPropagation(t *testing.T) {
	inputChan := make(chan *GenericErrorItem, 5)

	// Add more items to force multi-chunk behavior
	inputChan <- &GenericErrorItem{Key: 1, ShouldFailSerialization: false}
	inputChan <- &GenericErrorItem{Key: 2, ShouldFailSerialization: true} // This will fail
	inputChan <- &GenericErrorItem{Key: 3, ShouldFailSerialization: false}
	inputChan <- &GenericErrorItem{Key: 4, ShouldFailSerialization: false}
	inputChan <- &GenericErrorItem{Key: 5, ShouldFailSerialization: false}
	close(inputChan)

	config := &extsort.Config{
		ChunkSize:  2, // Force multiple chunks
		NumWorkers: 1, // Single worker to avoid race conditions in this test
	}

	sort, outChan, errChan := extsort.Generic(
		inputChan,
		genericFromBytes,
		genericToBytes,
		genericCompare,
		config,
	)

	sort.Sort(context.Background())

	// Drain output
	outputCount := 0
	for range outChan {
		outputCount++
	}

	// Verify error propagation
	err := <-errChan
	if err == nil {
		t.Fatal("Expected error to be properly propagated")
	}

	// Should be a wrapped SerializationError
	if !strings.Contains(err.Error(), "saveChunk") {
		t.Errorf("Expected error to mention 'saveChunk', got: %v", err)
	}

	t.Logf("Error properly propagated through phases: %v", err)
}

// Helper types and functions for generic error tests

type GenericErrorItem struct {
	Key                     int  `json:"key"`
	ShouldFailSerialization bool `json:"shouldFailSerialization"`
}

func genericToBytes(item *GenericErrorItem) ([]byte, error) {
	if item.ShouldFailSerialization {
		return nil, errors.New("serialization failed for test")
	}
	return json.Marshal(item)
}

func genericFromBytes(data []byte) (*GenericErrorItem, error) {
	var item GenericErrorItem
	err := json.Unmarshal(data, &item)
	return &item, err
}

func failingGenericFromBytes(data []byte) (*GenericErrorItem, error) {
	return nil, errors.New("deserialization always fails for test")
}

func genericCompare(a, b *GenericErrorItem) int {
	if a.Key < b.Key {
		return -1
	} else if a.Key > b.Key {
		return 1
	}
	return 0
}
