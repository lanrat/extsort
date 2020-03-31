# extsort

[![GoDoc](https://godoc.org/github.com/lanrat/extsort?status.svg)](https://godoc.org/github.com/lanrat/extsort)

[![Go Report Card](https://goreportcard.com/badge/github.com/lanrat/extsort)](https://goreportcard.com/report/github.com/lanrat/extsort)

An [external sorting](https://en.wikipedia.org/wiki/External_sorting) library for golang (i.e. on disk sorting) on an arbitrarily channel, even if the generated content doesn't all fit in memory at once. Once sorted, it returns a new channel returning data in sorted order.

In order to remain efficient for all implementations, extsort doesn't handle serialization, but leaves that to the user by operating on types that implement the [`SortType.ToBytes`](https://godoc.org/github.com/lanrat/extsort#SortType) and [`FromBytes`](https://godoc.org/github.com/lanrat/extsort#FromBytes) interfaces.

extsort is not a [stable sort](https://en.wikipedia.org/wiki/Sorting_algorithm#Stability).

## Example

```go
package main

import (
	"encoding/binary"
	"fmt"
	"math/rand"

	"github.com/lanrat/extsort"
)

var count = int(1e7) // 10M

type sortInt struct {
	i int64
}

func (s sortInt) ToBytes() []byte {
	buf := make([]byte, binary.MaxVarintLen64)
	binary.PutVarint(buf, s.i)
	return buf
}

func sortIntFromBytes(b []byte) extsort.SortType {
	i, _ := binary.Varint(b)
	return sortInt{i: i}
}

func compareSortIntLess(a, b extsort.SortType) bool {
	return a.(sortInt).i < b.(sortInt).i
}

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
	sorter := extsort.New(inputChan, sortIntFromBytes, compareSortIntLess, nil)
	outputChan, errChan := sorter.Sort()

	// print output sorted data
	for {
		select {
		case err, more := <-errChan:
			if !more {
				return
			}
			fmt.Printf("err: %s", err.Error())
			return
		case data, more := <-outputChan:
			if !more {
				return
			}
			fmt.Printf("%d\n", data.(sortInt).i)
		}
	}
}
```

## Tuning

The number of temporary files creates will be (total number of records) / (`ChunkSize`). If this is larger than the open file handle limit (`ulimit -n`) then the sort will fail and you should increase `ChunkSize` to reduce the number of temporary files used.

## TODO

* parallelize merging after sorting
