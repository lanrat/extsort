package extsort_test

import (
	"context"
	"reflect"
	"testing"

	"github.com/lanrat/extsort"
)

// Test int sorting using Ordered function
func TestOrderedIntSort(t *testing.T) {
	data := []int{64, 34, 25, 12, 22, 11, 90}

	inputChan := make(chan int, len(data))
	for _, v := range data {
		inputChan <- v
	}
	close(inputChan)

	config := extsort.DefaultConfig()
	config.ChunkSize = 3

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sorter, outChan, errChan := extsort.Ordered(inputChan, config)
	sorter.Sort(ctx)

	var result []int
	for {
		select {
		case val, ok := <-outChan:
			if !ok {
				if err := <-errChan; err != nil {
					t.Fatalf("sort error: %v", err)
				}
				goto done1
			}
			result = append(result, val)
		case err := <-errChan:
			if err != nil {
				t.Fatalf("sort error: %v", err)
			}
			for val := range outChan {
				result = append(result, val)
			}
			goto done1
		}
	}
done1:

	expected := []int{11, 12, 22, 25, 34, 64, 90}
	if !reflect.DeepEqual(result, expected) {
		t.Errorf("Expected %v, got %v", expected, result)
	}
}

// Test string sorting using Ordered function
func TestOrderedStringSort(t *testing.T) {
	data := []string{"banana", "apple", "cherry", "date", "elderberry"}

	inputChan := make(chan string, len(data))
	for _, v := range data {
		inputChan <- v
	}
	close(inputChan)

	config := extsort.DefaultConfig()
	config.ChunkSize = 2

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sorter, outChan, errChan := extsort.Ordered(inputChan, config)
	sorter.Sort(ctx)

	var result []string
	for {
		select {
		case val, ok := <-outChan:
			if !ok {
				if err := <-errChan; err != nil {
					t.Fatalf("sort error: %v", err)
				}
				goto done2
			}
			result = append(result, val)
		case err := <-errChan:
			if err != nil {
				t.Fatalf("sort error: %v", err)
			}
			for val := range outChan {
				result = append(result, val)
			}
			goto done2
		}
	}
done2:

	expected := []string{"apple", "banana", "cherry", "date", "elderberry"}
	if !reflect.DeepEqual(result, expected) {
		t.Errorf("Expected %v, got %v", expected, result)
	}
}

// Test float64 sorting using Ordered function
func TestOrderedFloat64Sort(t *testing.T) {
	data := []float64{3.14, 2.71, 1.41, 1.73, 0.57}

	inputChan := make(chan float64, len(data))
	for _, v := range data {
		inputChan <- v
	}
	close(inputChan)

	config := extsort.DefaultConfig()
	config.ChunkSize = 2

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sorter, outChan, errChan := extsort.Ordered(inputChan, config)
	sorter.Sort(ctx)

	var result []float64
	for {
		select {
		case val, ok := <-outChan:
			if !ok {
				if err := <-errChan; err != nil {
					t.Fatalf("sort error: %v", err)
				}
				goto done3
			}
			result = append(result, val)
		case err := <-errChan:
			if err != nil {
				t.Fatalf("sort error: %v", err)
			}
			for val := range outChan {
				result = append(result, val)
			}
			goto done3
		}
	}
done3:

	expected := []float64{0.57, 1.41, 1.73, 2.71, 3.14}
	if !reflect.DeepEqual(result, expected) {
		t.Errorf("Expected %v, got %v", expected, result)
	}
}

// Test OrderedMock with int
func TestOrderedMockIntSort(t *testing.T) {
	data := []int{5, 2, 8, 1, 9}

	inputChan := make(chan int, len(data))
	for _, v := range data {
		inputChan <- v
	}
	close(inputChan)

	config := extsort.DefaultConfig()
	config.ChunkSize = 2

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sorter, outChan, errChan := extsort.OrderedMock(inputChan, config, 1024)
	sorter.Sort(ctx)

	var result []int
	for {
		select {
		case val, ok := <-outChan:
			if !ok {
				if err := <-errChan; err != nil {
					t.Fatalf("sort error: %v", err)
				}
				goto done4
			}
			result = append(result, val)
		case err := <-errChan:
			if err != nil {
				t.Fatalf("sort error: %v", err)
			}
			for val := range outChan {
				result = append(result, val)
			}
			goto done4
		}
	}
done4:

	expected := []int{1, 2, 5, 8, 9}
	if !reflect.DeepEqual(result, expected) {
		t.Errorf("Expected %v, got %v", expected, result)
	}
}

// Test empty input with Ordered
func TestOrderedEmptyInput(t *testing.T) {
	inputChan := make(chan int)
	close(inputChan)

	config := extsort.DefaultConfig()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sorter, outChan, errChan := extsort.Ordered(inputChan, config)
	sorter.Sort(ctx)

	var result []int
	for {
		select {
		case val, ok := <-outChan:
			if !ok {
				if err := <-errChan; err != nil {
					t.Fatalf("sort error: %v", err)
				}
				goto done5
			}
			result = append(result, val)
		case err := <-errChan:
			if err != nil {
				t.Fatalf("sort error: %v", err)
			}
			for val := range outChan {
				result = append(result, val)
			}
			goto done5
		}
	}
done5:

	if len(result) != 0 {
		t.Errorf("Expected empty result, got %v", result)
	}
}