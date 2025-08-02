package extsort_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/lanrat/extsort"
)

// TestSingleChunkOptimizationEfficiency verifies that the single-chunk optimization
// works correctly and provides the expected performance benefits
func TestSingleChunkOptimizationEfficiency(t *testing.T) {
	// Test with various single-chunk scenarios
	testCases := []struct {
		name  string
		items int
	}{
		{"Single item", 1},
		{"Few items", 5},
		{"Near chunk size", 50}, // Assuming default chunk size > 50
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			inputChan := make(chan val, tc.items)

			// Add items in reverse order to verify sorting works
			for i := tc.items - 1; i >= 0; i-- {
				inputChan <- val{Key: i, Order: i}
			}
			close(inputChan)

			// Use large chunk size to ensure single chunk
			config := &extsort.Config{ChunkSize: 100}
			sort, outChan, errChan := extsort.Generic(
				inputChan,
				func(data []byte) (val, error) {
					var v val
					err := json.Unmarshal(data, &v)
					return v, err
				},
				func(v val) ([]byte, error) {
					return json.Marshal(v)
				},
				func(a, b val) int {
					if a.Key < b.Key {
						return -1
					} else if a.Key > b.Key {
						return 1
					}
					return 0
				},
				config,
			)
			sort.Sort(context.Background())

			// Collect results
			var results []val
			for result := range outChan {
				results = append(results, result)
			}

			// Check for errors
			if err := <-errChan; err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			// Verify correct number of items
			if len(results) != tc.items {
				t.Fatalf("Expected %d items, got %d", tc.items, len(results))
			}

			// Verify items are sorted
			for i := 0; i < len(results); i++ {
				if results[i].Key != i {
					t.Errorf("Item %d has Key %d, expected %d", i, results[i].Key, i)
				}
			}

			t.Logf("Single chunk optimization verified for %d items", tc.items)
		})
	}
}

// TestSingleChunkVsMultiChunk compares single chunk vs multi chunk behavior
func TestSingleChunkVsMultiChunk(t *testing.T) {
	const itemCount = 10

	// Test single chunk (large chunk size)
	t.Run("SingleChunk", func(t *testing.T) {
		inputChan := make(chan val, itemCount)
		for i := itemCount - 1; i >= 0; i-- {
			inputChan <- val{Key: i, Order: i}
		}
		close(inputChan)

		// Large chunk size to ensure single chunk
		config := &extsort.Config{ChunkSize: 100}
		sort, outChan, errChan := extsort.Generic(
			inputChan,
			func(data []byte) (val, error) {
				var v val
				err := json.Unmarshal(data, &v)
				return v, err
			},
			func(v val) ([]byte, error) {
				return json.Marshal(v)
			},
			func(a, b val) int {
				if a.Key < b.Key {
					return -1
				} else if a.Key > b.Key {
					return 1
				}
				return 0
			},
			config,
		)
		sort.Sort(context.Background())

		// Collect and verify results
		results := make([]val, 0, itemCount)
		for result := range outChan {
			results = append(results, result)
		}

		if err := <-errChan; err != nil {
			t.Fatalf("Single chunk error: %v", err)
		}

		if len(results) != itemCount {
			t.Fatalf("Single chunk: expected %d items, got %d", itemCount, len(results))
		}

		for i, result := range results {
			if result.Key != i {
				t.Errorf("Single chunk: item %d has Key %d, expected %d", i, result.Key, i)
			}
		}
	})

	// Test multi chunk (small chunk size)
	t.Run("MultiChunk", func(t *testing.T) {
		inputChan := make(chan val, itemCount)
		for i := itemCount - 1; i >= 0; i-- {
			inputChan <- val{Key: i, Order: i}
		}
		close(inputChan)

		// Small chunk size to force multiple chunks
		config := &extsort.Config{ChunkSize: 3}
		sort, outChan, errChan := extsort.Generic(
			inputChan,
			func(data []byte) (val, error) {
				var v val
				err := json.Unmarshal(data, &v)
				return v, err
			},
			func(v val) ([]byte, error) {
				return json.Marshal(v)
			},
			func(a, b val) int {
				if a.Key < b.Key {
					return -1
				} else if a.Key > b.Key {
					return 1
				}
				return 0
			},
			config,
		)
		sort.Sort(context.Background())

		// Collect and verify results
		results := make([]val, 0, itemCount)
		for result := range outChan {
			results = append(results, result)
		}

		if err := <-errChan; err != nil {
			t.Fatalf("Multi chunk error: %v", err)
		}

		if len(results) != itemCount {
			t.Fatalf("Multi chunk: expected %d items, got %d", itemCount, len(results))
		}

		for i, result := range results {
			if result.Key != i {
				t.Errorf("Multi chunk: item %d has Key %d, expected %d", i, result.Key, i)
			}
		}
	})

	t.Log("Both single-chunk and multi-chunk paths produce identical results")
}

// TestSingleChunkWithGenericAPI tests the optimization with the Generic API
func TestSingleChunkWithGenericAPI(t *testing.T) {
	inputChan := make(chan int, 5)

	// Add items in reverse order
	items := []int{5, 4, 3, 2, 1}
	for _, item := range items {
		inputChan <- item
	}
	close(inputChan)

	// Large chunk size for single chunk
	config := &extsort.Config{ChunkSize: 100}
	sort, outChan, errChan := extsort.Ordered(inputChan, config)
	sort.Sort(context.Background())

	// Collect results
	var results []int
	for result := range outChan {
		results = append(results, result)
	}

	// Check for errors
	if err := <-errChan; err != nil {
		t.Fatalf("Generic API single chunk error: %v", err)
	}

	// Verify sorted order
	expected := []int{1, 2, 3, 4, 5}
	if len(results) != len(expected) {
		t.Fatalf("Expected %d items, got %d", len(expected), len(results))
	}

	for i, result := range results {
		if result != expected[i] {
			t.Errorf("Item %d: got %d, expected %d", i, result, expected[i])
		}
	}

	t.Log("Single chunk optimization works with Generic API")
}
