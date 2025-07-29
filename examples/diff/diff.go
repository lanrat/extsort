package main

import (
	"context"
	"fmt"

	"github.com/lanrat/extsort/diff"
)

func main() {
	// Create two sorted string channels
	streamA := make(chan string, 5)
	streamB := make(chan string, 5)

	// Populate stream A
	go func() {
		defer close(streamA)
		for _, item := range []string{"apple", "banana", "cherry", "elderberry"} {
			streamA <- item
		}
	}()

	// Populate stream B
	go func() {
		defer close(streamB)
		for _, item := range []string{"banana", "cherry", "date", "fig"} {
			streamB <- item
		}
	}()

	// Create error channels
	errA := make(chan error, 1)
	errB := make(chan error, 1)
	close(errA)
	close(errB)

	// Process differences
	result, err := diff.Strings(
		context.Background(),
		streamA, streamB,
		errA, errB,
		func(delta diff.Delta, item string) error {
			switch delta {
			case diff.OLD:
				fmt.Printf("Only in A: %s\n", item)
			case diff.NEW:
				fmt.Printf("Only in B: %s\n", item)
			}
			return nil
		},
	)

	if err != nil {
		panic(err)
	}

	fmt.Printf("Summary: %d items only in A, %d items only in B, %d common items\n",
		result.ExtraA, result.ExtraB, result.Common)
}
