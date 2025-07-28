package extsort_test

import (
	"context"
	"encoding/binary"
	"math/rand"
	"reflect"
	"testing"
	"unsafe"

	"github.com/lanrat/extsort"
)

// Custom types for testing generics

// Person represents a simple struct for testing
type Person struct {
	Name string
	Age  int
}

// PersonToBytes serializes Person to bytes
func PersonToBytes(p Person) []byte {
	nameBytes := []byte(p.Name)
	ageBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(ageBytes, uint32(p.Age))
	
	// Format: [nameLen][name][age]
	result := make([]byte, 0, 4+len(nameBytes)+4)
	lenBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(lenBytes, uint32(len(nameBytes)))
	
	result = append(result, lenBytes...)
	result = append(result, nameBytes...)
	result = append(result, ageBytes...)
	
	return result
}

// PersonFromBytes deserializes bytes to Person
func PersonFromBytes(data []byte) Person {
	if len(data) < 8 {
		return Person{}
	}
	
	nameLen := binary.LittleEndian.Uint32(data[0:4])
	if len(data) < int(4+nameLen+4) {
		return Person{}
	}
	
	name := string(data[4 : 4+nameLen])
	age := int(binary.LittleEndian.Uint32(data[4+nameLen : 4+nameLen+4]))
	
	return Person{Name: name, Age: age}
}

// PersonLessFunc compares two Person structs by age, then by name
func PersonLessFunc(a, b Person) bool {
	if a.Age != b.Age {
		return a.Age < b.Age
	}
	return a.Name < b.Name
}

// Test custom struct sorting
func TestGenericPersonSort(t *testing.T) {
	people := []Person{
		{Name: "Alice", Age: 30},
		{Name: "Bob", Age: 25},
		{Name: "Charlie", Age: 35},
		{Name: "Diana", Age: 25},
		{Name: "Eve", Age: 28},
	}
	
	inputChan := make(chan Person, len(people))
	for _, p := range people {
		inputChan <- p
	}
	close(inputChan)
	
	config := extsort.DefaultConfig()
	config.ChunkSize = 3
	
	sorter, outChan, errChan := extsort.Generic(inputChan, PersonFromBytes, PersonToBytes, PersonLessFunc, config)
	sorter.Sort(context.Background())
	
	var result []Person
	for person := range outChan {
		result = append(result, person)
	}
	
	if err := <-errChan; err != nil {
		t.Fatalf("sort error: %v", err)
	}
	
	// Verify sorting: should be sorted by age first, then by name
	expected := []Person{
		{Name: "Bob", Age: 25},
		{Name: "Diana", Age: 25},
		{Name: "Eve", Age: 28},
		{Name: "Alice", Age: 30},
		{Name: "Charlie", Age: 35},
	}
	
	if !reflect.DeepEqual(result, expected) {
		t.Errorf("Expected %v, got %v", expected, result)
	}
}

// Test integer sorting with generics
func TestGenericIntSort(t *testing.T) {
	data := []int{64, 34, 25, 12, 22, 11, 90}
	
	inputChan := make(chan int, len(data))
	for _, v := range data {
		inputChan <- v
	}
	close(inputChan)
	
	config := extsort.DefaultConfig()
	config.ChunkSize = 3
	
	sorter, outChan, errChan := extsort.Generic(
		inputChan,
		func(b []byte) int {
			if len(b) < 8 {
				return 0
			}
			return int(binary.LittleEndian.Uint64(b))
		},
		func(i int) []byte {
			b := make([]byte, 8)
			binary.LittleEndian.PutUint64(b, uint64(i))
			return b
		},
		func(a, b int) bool { return a < b },
		config,
	)
	
	sorter.Sort(context.Background())
	
	var result []int
	for val := range outChan {
		result = append(result, val)
	}
	
	if err := <-errChan; err != nil {
		t.Fatalf("sort error: %v", err)
	}
	
	expected := []int{11, 12, 22, 25, 34, 64, 90}
	if !reflect.DeepEqual(result, expected) {
		t.Errorf("Expected %v, got %v", expected, result)
	}
}

// Test float64 sorting with generics
func TestGenericFloat64Sort(t *testing.T) {
	data := []float64{3.14, 2.71, 1.41, 1.73, 0.57}
	
	inputChan := make(chan float64, len(data))
	for _, v := range data {
		inputChan <- v
	}
	close(inputChan)
	
	config := extsort.DefaultConfig()
	config.ChunkSize = 2
	
	sorter, outChan, errChan := extsort.Generic(
		inputChan,
		func(b []byte) float64 {
			if len(b) < 8 {
				return 0
			}
			bits := binary.LittleEndian.Uint64(b)
			return *(*float64)(unsafe.Pointer(&bits))
		},
		func(f float64) []byte {
			bits := *(*uint64)(unsafe.Pointer(&f))
			b := make([]byte, 8)
			binary.LittleEndian.PutUint64(b, bits)
			return b
		},
		func(a, b float64) bool { return a < b },
		config,
	)
	
	sorter.Sort(context.Background())
	
	var result []float64
	for val := range outChan {
		result = append(result, val)
	}
	
	if err := <-errChan; err != nil {
		t.Fatalf("sort error: %v", err)
	}
	
	expected := []float64{0.57, 1.41, 1.73, 2.71, 3.14}
	if !reflect.DeepEqual(result, expected) {
		t.Errorf("Expected %v, got %v", expected, result)
	}
}

// Test string sorting with generics (alternative to strings_test.go)
func TestGenericStringSort(t *testing.T) {
	data := []string{"banana", "apple", "cherry", "date", "elderberry"}
	
	inputChan := make(chan string, len(data))
	for _, v := range data {
		inputChan <- v
	}
	close(inputChan)
	
	config := extsort.DefaultConfig()
	config.ChunkSize = 2
	
	sorter, outChan, errChan := extsort.Generic(
		inputChan,
		func(b []byte) string { return string(b) },
		func(s string) []byte { return []byte(s) },
		func(a, b string) bool { return a < b },
		config,
	)
	
	sorter.Sort(context.Background())
	
	var result []string
	for val := range outChan {
		result = append(result, val)
	}
	
	if err := <-errChan; err != nil {
		t.Fatalf("sort error: %v", err)
	}
	
	expected := []string{"apple", "banana", "cherry", "date", "elderberry"}
	if !reflect.DeepEqual(result, expected) {
		t.Errorf("Expected %v, got %v", expected, result)
	}
}

// Test mock version with generics
func TestGenericMockSort(t *testing.T) {
	data := []int{5, 2, 8, 1, 9}
	
	inputChan := make(chan int, len(data))
	for _, v := range data {
		inputChan <- v
	}
	close(inputChan)
	
	config := extsort.DefaultConfig()
	config.ChunkSize = 2
	
	sorter, outChan, errChan := extsort.MockGeneric(
		inputChan,
		func(b []byte) int {
			if len(b) < 8 {
				return 0
			}
			return int(binary.LittleEndian.Uint64(b))
		},
		func(i int) []byte {
			b := make([]byte, 8)
			binary.LittleEndian.PutUint64(b, uint64(i))
			return b
		},
		func(a, b int) bool { return a < b },
		config,
		1024, // mock buffer size
	)
	
	sorter.Sort(context.Background())
	
	var result []int
	for val := range outChan {
		result = append(result, val)
	}
	
	if err := <-errChan; err != nil {
		t.Fatalf("sort error: %v", err)
	}
	
	expected := []int{1, 2, 5, 8, 9}
	if !reflect.DeepEqual(result, expected) {
		t.Errorf("Expected %v, got %v", expected, result)
	}
}

// Test large dataset with generics to ensure chunking works
func TestGenericLargeDataset(t *testing.T) {
	size := 10000
	data := make([]int, size)
	for i := 0; i < size; i++ {
		data[i] = rand.Intn(size)
	}
	
	inputChan := make(chan int, size)
	for _, v := range data {
		inputChan <- v
	}
	close(inputChan)
	
	config := extsort.DefaultConfig()
	config.ChunkSize = 1000
	
	sorter, outChan, errChan := extsort.Generic(
		inputChan,
		func(b []byte) int {
			if len(b) < 8 {
				return 0
			}
			return int(binary.LittleEndian.Uint64(b))
		},
		func(i int) []byte {
			b := make([]byte, 8)
			binary.LittleEndian.PutUint64(b, uint64(i))
			return b
		},
		func(a, b int) bool { return a < b },
		config,
	)
	
	sorter.Sort(context.Background())
	
	var result []int
	for val := range outChan {
		result = append(result, val)
	}
	
	if err := <-errChan; err != nil {
		t.Fatalf("sort error: %v", err)
	}
	
	if len(result) != size {
		t.Errorf("Expected %d elements, got %d", size, len(result))
	}
	
	// Verify it's sorted
	for i := range result[1:] {
		idx := i + 1
		if result[idx-1] > result[idx] {
			t.Errorf("Result not sorted at index %d: %d > %d", idx, result[idx-1], result[idx])
			break
		}
	}
}

// Test interface satisfaction
func TestGenericSorterInterface(t *testing.T) {
	inputChan := make(chan int, 1)
	inputChan <- 42
	close(inputChan)
	
	config := extsort.DefaultConfig()
	
	sorter, _, _ := extsort.Generic(
		inputChan,
		func(b []byte) int { return 0 },
		func(i int) []byte { return nil },
		func(a, b int) bool { return a < b },
		config,
	)
	
	// Verify that GenericSorter implements the Sorter interface
	onlySortersAllowed(sorter)
}

// Test empty input
func TestGenericEmptyInput(t *testing.T) {
	inputChan := make(chan int)
	close(inputChan)
	
	config := extsort.DefaultConfig()
	
	sorter, outChan, errChan := extsort.Generic(
		inputChan,
		func(b []byte) int { return 0 },
		func(i int) []byte { return []byte{} },
		func(a, b int) bool { return a < b },
		config,
	)
	
	sorter.Sort(context.Background())
	
	var result []int
	for val := range outChan {
		result = append(result, val)
	}
	
	if err := <-errChan; err != nil {
		t.Fatalf("sort error: %v", err)
	}
	
	if len(result) != 0 {
		t.Errorf("Expected empty result, got %v", result)
	}
}

// Benchmark generic sorting performance
func BenchmarkGenericIntSort(b *testing.B) {
	size := 100000
	
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		data := make([]int, size)
		for j := range data {
			data[j] = rand.Intn(size)
		}
		
		inputChan := make(chan int, size)
		for _, v := range data {
			inputChan <- v
		}
		close(inputChan)
		
		config := extsort.DefaultConfig()
		config.ChunkSize = 10000
		
		sorter, outChan, errChan := extsort.Generic(
			inputChan,
			func(b []byte) int {
				return int(binary.LittleEndian.Uint64(b))
			},
			func(i int) []byte {
				b := make([]byte, 8)
				binary.LittleEndian.PutUint64(b, uint64(i))
				return b
			},
			func(a, b int) bool { return a < b },
			config,
		)
		
		b.StartTimer()
		sorter.Sort(context.Background())
		
		count := 0
		for range outChan {
			count++
		}
		
		if err := <-errChan; err != nil {
			b.Fatalf("sort error: %v", err)
		}
	}
}

