package extsort_test

import (
	"context"
	"encoding/binary"
	"fmt"
	"math/rand"
	"testing"

	"github.com/lanrat/extsort"
)

var count = int(1e7) // 10M

type sortInt struct {
	i int64
}

func (s sortInt) ToBytes(buf []byte) []byte {
	for i := 0; i < binary.MaxVarintLen64; i++ {
		buf = append(buf, 0)
	}
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

func testMain(t *testing.T) {
	main()
}
