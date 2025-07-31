package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"math/rand"

	"github.com/lanrat/extsort"
)

type sortInt struct {
	value int64
}

func (s sortInt) ToBytes() []byte {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, uint64(s.value))
	return buf
}

//nolint:staticcheck // SA1019: This example intentionally demonstrates the legacy API
func sortIntFromBytes(data []byte) extsort.SortType {
	value := int64(binary.LittleEndian.Uint64(data))
	return sortInt{value: value}
}

//nolint:staticcheck // SA1019: This example intentionally demonstrates the legacy API
func compareSortInt(a, b extsort.SortType) bool {
	return a.(sortInt).value < b.(sortInt).value
}

func main() {
	//nolint:staticcheck // SA1019: This example intentionally demonstrates the legacy API
	inputChan := make(chan extsort.SortType, 100)
	go func() {
		defer close(inputChan)
		for i := 0; i < 100000; i++ {
			inputChan <- sortInt{value: rand.Int63()}
		}
	}()

	//nolint:staticcheck // SA1019: This example intentionally demonstrates the legacy API
	sorter, outputChan, errChan := extsort.New(
		inputChan,
		sortIntFromBytes,
		compareSortInt,
		nil,
	)

	go sorter.Sort(context.Background())

	for item := range outputChan {
		fmt.Println(item.(sortInt).value)
	}

	if err := <-errChan; err != nil {
		panic(err)
	}
}
