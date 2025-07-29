package main

import (
	"context"
	"fmt"

	"github.com/lanrat/extsort"
)

func main() {
	words := []string{"zebra", "apple", "banana", "cherry"}

	inputChan := make(chan string, len(words))
	for _, word := range words {
		inputChan <- word
	}
	close(inputChan)

	sorter, outputChan, errChan := extsort.Strings(inputChan, nil)
	go sorter.Sort(context.Background())

	fmt.Println("Sorted words:")
	for word := range outputChan {
		fmt.Println(word)
	}

	if err := <-errChan; err != nil {
		panic(err)
	}
}
