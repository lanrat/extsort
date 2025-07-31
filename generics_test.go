package extsort_test

import (
	"cmp"
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
func PersonToBytes(p Person) ([]byte, error) {
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

	return result, nil
}

// PersonFromBytes deserializes bytes to Person
func PersonFromBytes(data []byte) (Person, error) {
	if len(data) < 8 {
		return Person{}, nil
	}

	nameLen := binary.LittleEndian.Uint32(data[0:4])
	if len(data) < int(4+nameLen+4) {
		return Person{}, nil
	}

	name := string(data[4 : 4+nameLen])
	age := int(binary.LittleEndian.Uint32(data[4+nameLen : 4+nameLen+4]))

	return Person{Name: name, Age: age}, nil
}

// PersonCmpFunc compares two Person structs by age, then by name
func PersonCmpFunc(a, b Person) int {
	if a.Age != b.Age {
		if a.Age < b.Age {
			return -1
		}
		return 1
	}
	if a.Name < b.Name {
		return -1
	}
	if a.Name > b.Name {
		return 1
	}
	return 0
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

	// Create cancelable context for proper cleanup
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sorter, outChan, errChan := extsort.Generic(inputChan, PersonFromBytes, PersonToBytes, PersonCmpFunc, config)
	sorter.Sort(ctx)

	var result []Person
	for {
		select {
		case person, ok := <-outChan:
			if !ok {
				if err := <-errChan; err != nil {
					t.Fatalf("sort error: %v", err)
				}
				goto done1
			}
			result = append(result, person)
		case err := <-errChan:
			if err != nil {
				t.Fatalf("sort error: %v", err)
			}
			for person := range outChan {
				result = append(result, person)
			}
			goto done1
		}
	}
done1:

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

	// Create cancelable context for proper cleanup
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sorter, outChan, errChan := extsort.Generic(
		inputChan,
		func(b []byte) (int, error) {
			if len(b) < 8 {
				return 0, nil
			}
			return int(binary.LittleEndian.Uint64(b)), nil
		},
		func(i int) ([]byte, error) {
			b := make([]byte, 8)
			binary.LittleEndian.PutUint64(b, uint64(i))
			return b, nil
		},
		cmp.Compare[int],
		config,
	)

	sorter.Sort(ctx)

	var result []int
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

	// Create cancelable context for proper cleanup
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sorter, outChan, errChan := extsort.Generic(
		inputChan,
		func(b []byte) (float64, error) {
			if len(b) < 8 {
				return 0, nil
			}
			bits := binary.LittleEndian.Uint64(b)
			return *(*float64)(unsafe.Pointer(&bits)), nil
		},
		func(f float64) ([]byte, error) {
			bits := *(*uint64)(unsafe.Pointer(&f))
			b := make([]byte, 8)
			binary.LittleEndian.PutUint64(b, bits)
			return b, nil
		},
		cmp.Compare[float64],
		config,
	)

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

	// Create cancelable context for proper cleanup
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sorter, outChan, errChan := extsort.Generic(
		inputChan,
		func(b []byte) (string, error) { return string(b), nil },
		func(s string) ([]byte, error) { return []byte(s), nil },
		cmp.Compare[string],
		config,
	)

	sorter.Sort(ctx)

	var result []string
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

	// Create cancelable context for proper cleanup
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sorter, outChan, errChan := extsort.MockGeneric(
		inputChan,
		func(b []byte) (int, error) {
			if len(b) < 8 {
				return 0, nil
			}
			return int(binary.LittleEndian.Uint64(b)), nil
		},
		func(i int) ([]byte, error) {
			b := make([]byte, 8)
			binary.LittleEndian.PutUint64(b, uint64(i))
			return b, nil
		},
		cmp.Compare[int],
		config,
		1024, // mock buffer size
	)

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

	// Create cancelable context for proper cleanup
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sorter, outChan, errChan := extsort.Generic(
		inputChan,
		func(b []byte) (int, error) {
			if len(b) < 8 {
				return 0, nil
			}
			return int(binary.LittleEndian.Uint64(b)), nil
		},
		func(i int) ([]byte, error) {
			b := make([]byte, 8)
			binary.LittleEndian.PutUint64(b, uint64(i))
			return b, nil
		},
		cmp.Compare[int],
		config,
	)

	sorter.Sort(ctx)

	var result []int
	for {
		select {
		case val, ok := <-outChan:
			if !ok {
				if err := <-errChan; err != nil {
					t.Fatalf("sort error: %v", err)
				}
				goto done6
			}
			result = append(result, val)
		case err := <-errChan:
			if err != nil {
				t.Fatalf("sort error: %v", err)
			}
			for val := range outChan {
				result = append(result, val)
			}
			goto done6
		}
	}
done6:

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
		func(b []byte) (int, error) { return 0, nil },
		func(i int) ([]byte, error) { return nil, nil },
		cmp.Compare[int],
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

	// Create cancelable context for proper cleanup
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sorter, outChan, errChan := extsort.Generic(
		inputChan,
		func(b []byte) (int, error) { return 0, nil },
		func(i int) ([]byte, error) { return []byte{}, nil },
		cmp.Compare[int],
		config,
	)

	sorter.Sort(ctx)

	var result []int
	for {
		select {
		case val, ok := <-outChan:
			if !ok {
				if err := <-errChan; err != nil {
					t.Fatalf("sort error: %v", err)
				}
				goto done7
			}
			result = append(result, val)
		case err := <-errChan:
			if err != nil {
				t.Fatalf("sort error: %v", err)
			}
			for val := range outChan {
				result = append(result, val)
			}
			goto done7
		}
	}
done7:

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

		// Create cancelable context for proper cleanup
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		sorter, outChan, errChan := extsort.Generic(
			inputChan,
			func(b []byte) (int, error) {
				return int(binary.LittleEndian.Uint64(b)), nil
			},
			func(i int) ([]byte, error) {
				b := make([]byte, 8)
				binary.LittleEndian.PutUint64(b, uint64(i))
				return b, nil
			},
			cmp.Compare[int],
			config,
		)

		b.StartTimer()
		sorter.Sort(ctx)

		count := 0
		for {
			select {
			case _, ok := <-outChan:
				if !ok {
					if err := <-errChan; err != nil {
						b.Fatalf("sort error: %v", err)
					}
					goto done
				}
				count++
			case err := <-errChan:
				if err != nil {
					b.Fatalf("sort error: %v", err)
				}
				for range outChan {
					count++
				}
				goto done
			}
		}
	done:
	}
}
