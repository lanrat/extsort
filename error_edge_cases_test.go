package extsort_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/lanrat/extsort"
)

// TestSerializationErrorInFirstChunk tests error handling when the first chunk fails serialization
func TestSerializationErrorInFirstChunk(t *testing.T) {
	inputChan := make(chan *ErrorEdgeItem, 1)

	// Only item fails serialization
	inputChan <- &ErrorEdgeItem{Key: 1, FailSerialize: true}
	close(inputChan)

	config := &extsort.Config{ChunkSize: 1}
	sort, outChan, errChan := extsort.Generic(
		inputChan,
		edgeFromBytes,
		edgeToBytes,
		edgeCompare,
		config,
	)

	sort.Sort(context.Background())

	// Should get no output
	outputCount := 0
	for range outChan {
		outputCount++
	}

	if outputCount != 0 {
		t.Errorf("Expected 0 output items, got %d", outputCount)
	}

	// Should get error
	err := <-errChan
	if err == nil {
		t.Fatal("Expected serialization error but got nil")
	}

	var serErr *extsort.SerializationError
	if !errors.As(err, &serErr) {
		t.Errorf("Expected SerializationError, got %T", err)
	}

	t.Logf("First chunk serialization error handled correctly: %v", err)
}

// TestSerializationErrorInLastChunk tests error handling when the last chunk fails serialization
func TestSerializationErrorInLastChunk(t *testing.T) {
	inputChan := make(chan *ErrorEdgeItem, 3)

	// Good items followed by failing item
	inputChan <- &ErrorEdgeItem{Key: 1, FailSerialize: false}
	inputChan <- &ErrorEdgeItem{Key: 2, FailSerialize: false}
	inputChan <- &ErrorEdgeItem{Key: 3, FailSerialize: true} // Last chunk fails
	close(inputChan)

	config := &extsort.Config{ChunkSize: 1} // Each item in its own chunk
	sort, outChan, errChan := extsort.Generic(
		inputChan,
		edgeFromBytes,
		edgeToBytes,
		edgeCompare,
		config,
	)

	sort.Sort(context.Background())

	// Should get no output due to error
	outputCount := 0
	for range outChan {
		outputCount++
	}

	// Should get error even though first chunks were fine
	err := <-errChan
	if err == nil {
		t.Fatal("Expected serialization error but got nil")
	}

	t.Logf("Last chunk serialization error handled correctly: %v", err)
}

// TestEmptyChunkWithError tests error handling when chunks become empty due to errors
func TestEmptyChunkWithError(t *testing.T) {
	inputChan := make(chan *ErrorEdgeItem, 1)

	inputChan <- &ErrorEdgeItem{Key: 1, FailSerialize: true}
	close(inputChan)

	// Use larger chunk size - the single failing item should still cause error
	config := &extsort.Config{ChunkSize: 10}
	sort, outChan, errChan := extsort.Generic(
		inputChan,
		edgeFromBytes,
		edgeToBytes,
		edgeCompare,
		config,
	)

	sort.Sort(context.Background())

	// Drain output
	for range outChan {
		// Should be empty
	}

	// Should still get error
	err := <-errChan
	if err == nil {
		t.Fatal("Expected error even with large chunk size")
	}

	t.Logf("Empty chunk error handling works: %v", err)
}

// TestDeserializationErrorDuringMerge tests deserialization errors during the merge phase
func TestDeserializationErrorDuringMerge(t *testing.T) {
	inputChan := make(chan *ErrorEdgeItem, 2)

	// Items that serialize fine
	inputChan <- &ErrorEdgeItem{Key: 1, FailSerialize: false}
	inputChan <- &ErrorEdgeItem{Key: 2, FailSerialize: false}
	close(inputChan)

	config := &extsort.Config{ChunkSize: 1} // Force multiple chunks to trigger merge
	sort, outChan, errChan := extsort.Generic(
		inputChan,
		edgeFailingFromBytes, // This will fail during merge
		edgeToBytes,
		edgeCompare,
		config,
	)

	sort.Sort(context.Background())

	// Should get no output due to deserialization failure during merge
	outputCount := 0
	for range outChan {
		outputCount++
	}

	err := <-errChan
	if err == nil {
		t.Fatal("Expected deserialization error during merge")
	}

	var deserErr *extsort.DeserializationError
	if !errors.As(err, &deserErr) {
		t.Errorf("Expected DeserializationError, got %T", err)
	}

	t.Logf("Merge phase deserialization error handled: %v", err)
}

// TestConcurrentErrorHandling tests error handling with multiple workers
func TestConcurrentErrorHandling(t *testing.T) {
	inputChan := make(chan *ErrorEdgeItem, 4)

	// Simpler test - just a few items with one that fails early
	inputChan <- &ErrorEdgeItem{Key: 1, FailSerialize: false}
	inputChan <- &ErrorEdgeItem{Key: 2, FailSerialize: true} // This will fail
	inputChan <- &ErrorEdgeItem{Key: 3, FailSerialize: false}
	close(inputChan)

	config := &extsort.Config{
		ChunkSize:  1, // Each item in its own chunk for predictable behavior
		NumWorkers: 2, // Multiple workers
	}
	sort, outChan, errChan := extsort.Generic(
		inputChan,
		edgeFromBytes,
		edgeToBytes,
		edgeCompare,
		config,
	)

	sort.Sort(context.Background())

	// Drain output
	outputCount := 0
	for range outChan {
		outputCount++
	}

	// Should get error from one of the workers
	err := <-errChan
	if err == nil {
		t.Fatal("Expected error with concurrent processing")
	}

	t.Logf("Concurrent error handling works: %v (got %d outputs)", err, outputCount)
}

// Helper types for edge case testing

type ErrorEdgeItem struct {
	Key           int  `json:"key"`
	FailSerialize bool `json:"failSerialize"`
}

func edgeToBytes(item *ErrorEdgeItem) ([]byte, error) {
	if item.FailSerialize {
		return nil, errors.New("edge case serialization failure")
	}
	return json.Marshal(item)
}

func edgeFromBytes(data []byte) (*ErrorEdgeItem, error) {
	var item ErrorEdgeItem
	err := json.Unmarshal(data, &item)
	return &item, err
}

func edgeFailingFromBytes(data []byte) (*ErrorEdgeItem, error) {
	return nil, errors.New("edge case deserialization failure")
}

func edgeCompare(a, b *ErrorEdgeItem) int {
	if a.Key < b.Key {
		return -1
	} else if a.Key > b.Key {
		return 1
	}
	return 0
}
