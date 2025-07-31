package extsort_test

import (
	"cmp"
	"context"
	"encoding/binary"
	"fmt"
	"math/rand"
	"sort"
	"testing"
	"time"

	"github.com/lanrat/extsort"
)

// Benchmark configurations
var benchmarkSizes = []int{1000, 10000, 100000, 1000000}

// BenchmarkGenericIntSortComparison benchmarks the new generic implementation
func BenchmarkGenericIntSortComparison(b *testing.B) {
	for _, size := range benchmarkSizes {
		b.Run(fmt.Sprintf("size_%d", size), func(b *testing.B) {
			// Pre-generate data to avoid timing issues
			data := generateRandomInts(size)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// Use anonymous function to ensure proper cleanup per iteration
				func() {
					inputChan := make(chan int, size)
					for _, v := range data {
						inputChan <- v
					}
					close(inputChan)

					config := extsort.DefaultConfig()
					config.ChunkSize = size / 10
					if config.ChunkSize < 100 {
						config.ChunkSize = 100
					}

					// Create context with timeout for proper cleanup
					ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
					defer cancel()

					sorter, outChan, errChan := extsort.Generic(
						inputChan,
						intFromBytes,
						intToBytes,
						cmp.Compare[int],
						config,
					)

					sorter.Sort(ctx)

					count := 0
					consumptionComplete := false
					for !consumptionComplete {
						select {
						case _, ok := <-outChan:
							if !ok {
								// outChan closed, check for final error
								select {
								case err := <-errChan:
									if err != nil {
										b.Fatalf("sort error: %v", err)
									}
								default:
									// No error in errChan
								}
								consumptionComplete = true
							} else {
								count++
							}
						case err := <-errChan:
							if err != nil {
								b.Fatalf("sort error: %v", err)
							}
							// No error, drain remaining output
							for range outChan {
								count++
							}
							consumptionComplete = true
						case <-ctx.Done():
							// Context cancelled, non-blocking drain and exit
						drainLoop:
							for {
								select {
								case _, ok := <-outChan:
									if !ok {
										break drainLoop
									}
									count++
								default:
									break drainLoop
								}
							}
							consumptionComplete = true
						}
					}
				}() // Execute anonymous function immediately
			}
			b.StopTimer()
		})
	}
}

// BenchmarkLegacyStringSort benchmarks the existing string implementation
func BenchmarkLegacyStringSort(b *testing.B) {
	for _, size := range benchmarkSizes {
		b.Run(fmt.Sprintf("size_%d", size), func(b *testing.B) {
			// Pre-generate data to avoid timing issues
			data := generateRandomStringInts(size)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// Use anonymous function to ensure proper cleanup per iteration
				func() {
					inputChan := make(chan string, size)
					for _, v := range data {
						inputChan <- v
					}
					close(inputChan)

					config := extsort.DefaultConfig()
					config.ChunkSize = size / 10
					if config.ChunkSize < 100 {
						config.ChunkSize = 100
					}

					// Create context with timeout for proper cleanup
					ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
					defer cancel()

					sorter, outChan, errChan := extsort.Strings(inputChan, config)
					sorter.Sort(ctx)

					count := 0
					consumptionComplete := false
					for !consumptionComplete {
						select {
						case _, ok := <-outChan:
							if !ok {
								// outChan closed, check for final error
								select {
								case err := <-errChan:
									if err != nil {
										b.Fatalf("sort error: %v", err)
									}
								default:
									// No error in errChan
								}
								consumptionComplete = true
							} else {
								count++
							}
						case err := <-errChan:
							if err != nil {
								b.Fatalf("sort error: %v", err)
							}
							// No error, drain remaining output
							for range outChan {
								count++
							}
							consumptionComplete = true
						case <-ctx.Done():
							// Context cancelled, non-blocking drain and exit
						drainLoop:
							for {
								select {
								case _, ok := <-outChan:
									if !ok {
										break drainLoop
									}
									count++
								default:
									break drainLoop
								}
							}
							consumptionComplete = true
						}
					}
				}() // Execute anonymous function immediately
			}
			b.StopTimer()
		})
	}
}

// BenchmarkStandardLibSort benchmarks Go's standard library sort
func BenchmarkStandardLibSort(b *testing.B) {
	for _, size := range benchmarkSizes {
		b.Run(fmt.Sprintf("size_%d", size), func(b *testing.B) {
			// Pre-generate data to avoid timing issues
			data := generateRandomInts(size)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// Make a copy since sort.Ints modifies in place
				sortData := make([]int, len(data))
				copy(sortData, data)
				sort.Ints(sortData)
			}
			b.StopTimer()
		})
	}
}

// Memory usage benchmarks
func BenchmarkGenericIntSortMemory(b *testing.B) {
	size := 100000
	b.ReportAllocs()

	// Pre-generate data to avoid timing issues
	data := generateRandomInts(size)

	for i := 0; i < b.N; i++ {

		inputChan := make(chan int, size)
		for _, v := range data {
			inputChan <- v
		}
		close(inputChan)

		config := extsort.DefaultConfig()
		config.ChunkSize = 10000

		// Create context with timeout for proper cleanup
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		sorter, outChan, errChan := extsort.Generic(
			inputChan,
			intFromBytes,
			intToBytes,
			cmp.Compare[int],
			config,
		)

		sorter.Sort(ctx)

		for {
			select {
			case _, ok := <-outChan:
				if !ok {
					if err := <-errChan; err != nil {
						b.Fatalf("sort error: %v", err)
					}
					goto done3
				}
			case err := <-errChan:
				if err != nil {
					b.Fatalf("sort error: %v", err)
				}
				for range outChan {
					// Consume remaining output
				}
				goto done3
			case <-ctx.Done():
				// Context cancelled, non-blocking drain and exit
			drainLoop3:
				for {
					select {
					case _, ok := <-outChan:
						if !ok {
							break drainLoop3
						}
						// Consume remaining output
					default:
						break drainLoop3
					}
				}
				goto done3
			}
		}
	done3:
	}
}

func BenchmarkStandardLibSortMemory(b *testing.B) {
	size := 100000
	b.ReportAllocs()

	// Pre-generate data to avoid timing issues
	data := generateRandomInts(size)

	for i := 0; i < b.N; i++ {

		sortData := make([]int, len(data))
		copy(sortData, data)
		sort.Ints(sortData)
	}
}

// Helper functions
func generateRandomInts(size int) []int {
	data := make([]int, size)
	for i := range data {
		data[i] = rand.Intn(size * 2)
	}
	return data
}

func generateRandomStringInts(size int) []string {
	data := make([]string, size)
	for i := range data {
		data[i] = fmt.Sprintf("%d", rand.Intn(size*2))
	}
	return data
}

func intFromBytes(b []byte) int {
	if len(b) < 8 {
		return 0
	}
	return int(binary.LittleEndian.Uint64(b))
}

func intToBytes(i int) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, uint64(i))
	return b
}
