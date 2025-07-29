package main

import (
	"context"
	"fmt"

	"github.com/lanrat/extsort/diff"
)

func main() {
	// Create channels with integer data
	streamA := make(chan int, 5)
	streamB := make(chan int, 5)
	errA := make(chan error, 1)
	errB := make(chan error, 1)

	// Populate streams
	go func() {
		defer close(streamA)
		defer close(errA)
		for _, num := range []int{1, 3, 5, 7, 9} {
			streamA <- num
		}
	}()

	go func() {
		defer close(streamB)
		defer close(errB)
		for _, num := range []int{2, 4, 5, 6, 8} {
			streamB <- num
		}
	}()

	// Compare using generic diff
	compareFunc := func(a, b int) int {
		if a < b {
			return -1
		}
		if a > b {
			return 1
		}
		return 0
	}

	resultFunc := func(delta diff.Delta, item int) error {
		symbol := map[diff.Delta]string{diff.OLD: "<", diff.NEW: ">"}[delta]
		fmt.Printf("%s %d\n", symbol, item)
		return nil
	}

	result, err := diff.Generic(
		context.Background(),
		streamA, streamB,
		errA, errB,
		compareFunc,
		resultFunc,
	)

	if err != nil {
		panic(err)
	}

	fmt.Printf("Differences found: %d\n", result.ExtraA+result.ExtraB)
}
