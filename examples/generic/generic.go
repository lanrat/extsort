package main

import (
	"context"
	"fmt"
	"math/rand"

	"github.com/lanrat/extsort"
)

func main() {
	// Create input channel with unsorted integers
	inputChan := make(chan int, 100)
	go func() {
		defer close(inputChan)
		for i := 0; i < 1000000; i++ {
			inputChan <- rand.Int()
		}
	}()

	// Sort using the generic API
	sorter, outputChan, errChan := extsort.Ordered(inputChan, nil)

	// Start sorting in background
	go sorter.Sort(context.Background())

	// Process sorted results
	for value := range outputChan {
		fmt.Println(value)
	}

	// Check for errors
	if err := <-errChan; err != nil {
		panic(err)
	}
}
