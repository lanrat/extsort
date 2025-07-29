package main

import (
	"context"
	"fmt"
	"sync"

	"github.com/lanrat/extsort/diff"
)

func main() {
	streamA := make(chan string, 100)
	streamB := make(chan string, 100)
	errA := make(chan error, 1)
	errB := make(chan error, 1)

	// Populate streams with test data
	go func() {
		defer close(streamA)
		defer close(errA)
		for i := 0; i < 50; i += 2 {
			streamA <- fmt.Sprintf("item_%03d", i)
		}
	}()

	go func() {
		defer close(streamB)
		defer close(errB)
		for i := 1; i < 50; i += 2 {
			streamB <- fmt.Sprintf("item_%03d", i)
		}
	}()

	// Use channel-based result processing for parallel handling
	resultFunc, resultChan := diff.StringResultChan()

	var wg sync.WaitGroup
	wg.Add(1)

	// Process results in parallel
	go func() {
		defer wg.Done()
		for result := range resultChan {
			fmt.Printf("Difference: %s %s\n", result.D, result.S)
		}
	}()

	// Start diff operation
	go func() {
		defer close(resultChan)
		_, err := diff.Strings(
			context.Background(),
			streamA, streamB,
			errA, errB,
			resultFunc,
		)
		if err != nil {
			fmt.Printf("Diff error: %v\n", err)
		}
	}()

	wg.Wait()
	fmt.Println("Diff processing complete")
}
