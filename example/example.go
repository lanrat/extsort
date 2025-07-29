// Package main demonstrates how to use the extsort library for external sorting.
// This example shows how to sort a large number of random integers using the
// legacy SortType interface, including the required serialization methods.
package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"math/rand"

	"github.com/lanrat/extsort"
)

// count specifies the number of random integers to generate and sort
var count = int(1e7) // 10M

// sortInt wraps an int64 and implements the extsort.SortType interface.
// This demonstrates how custom types can be made sortable by the library.
type sortInt struct {
	i int64
}

// ToBytes serializes the sortInt to a byte slice using variable-length encoding.
// This method is required by the extsort.SortType interface.
func (s sortInt) ToBytes() []byte {
	buf := make([]byte, binary.MaxVarintLen64)
	binary.PutVarint(buf, s.i)
	return buf
}

// sortIntFromBytes deserializes a byte slice back to a sortInt.
// This function is passed to the extsort library for data reconstruction.
func sortIntFromBytes(b []byte) extsort.SortType {
	i, _ := binary.Varint(b)
	return sortInt{i: i}
}

// compareSortIntLess compares two sortInt values for ordering.
// This function is passed to the extsort library for comparison during sorting.
func compareSortIntLess(a, b extsort.SortType) bool {
	return a.(sortInt).i < b.(sortInt).i
}

// main demonstrates external sorting by generating random integers,
// sorting them using the extsort library, and verifying the results.
func main() {
	// create an input channel with unsorted data
	inputChan := make(chan extsort.SortType)
	go func() {
		for i := 0; i < count; i++ {
			inputChan <- sortInt{i: rand.Int63()}
		}
		close(inputChan)
	}()

	// create the sorter and start sorting
	sorter, outputChan, errChan := extsort.New(inputChan, sortIntFromBytes, compareSortIntLess, nil)
	sorter.Sort(context.Background())

	// print output sorted data
	for data := range outputChan {
		fmt.Printf("%d\n", data.(sortInt).i)
	}
	if err := <-errChan; err != nil {
		fmt.Printf("err: %s", err.Error())
	}
}
