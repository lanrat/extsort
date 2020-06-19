package extsort_test

// this test package is heavily bases on the one for psilva261's timsort
// https://github.com/psilva261/timsort/blob/master/timsort_test.go

import (
	"context"
	"testing"

	"github.com/lanrat/extsort"
)

func sortForMockTest(inputData []val, lessFunc extsort.CompareLessFunc) error {
	// make array of all data in chan
	inputChan := make(chan extsort.SortType, 2)
	go func() {
		for _, d := range inputData {
			inputChan <- d
		}
		close(inputChan)
	}()
	config := extsort.DefaultConfig()
	config.ChunkSize = len(inputData)/20 + 100
	sort, outChan, errChan := extsort.NewMock(inputChan, fromBytesForTest, lessFunc, config, 0)
	sort.Sort(context.Background())
	i := 0
	for rec := range outChan {
		inputData[i] = rec.(val)
		i++
	}
	if err := <-errChan; err != nil {
		return err
	}
	return nil
}

func TestMock50(t *testing.T) {
	a := makeTestArray(50)
	if IsSorted(a, KeyLessThan) {
		t.Error("sorted before starting")
	}

	err := sortForMockTest(a, KeyLessThan)
	if err != nil {
		t.Fatalf("sort: %v", err)
	}

	if !IsSorted(a, KeyLessThan) {
		t.Error("not sorted")
	}
}

func TestMockSmoke(t *testing.T) {
	a := make([]val, 3)
	a[0] = val{3, 0}
	a[1] = val{1, 1}
	a[2] = val{2, 2}

	err := sortForMockTest(a, KeyLessThan)
	if err != nil {
		t.Fatalf("sort: %v", err)
	}

	if !IsSorted(a, KeyLessThan) {
		t.Error("not sorted")
	}
}

func TestMockSmokeStability(t *testing.T) {
	a := make([]val, 3)
	a[0] = val{3, 0}
	a[1] = val{2, 1}
	a[2] = val{2, 2}

	err := sortForMockTest(a, KeyOrderLessThan)
	if err != nil {
		t.Fatalf("sort: %v", err)
	}

	if !IsSorted(a, KeyOrderLessThan) {
		t.Error("not sorted")
	}
}

func TestMock1K(t *testing.T) {
	a := makeTestArray(1024)

	err := sortForMockTest(a, KeyOrderLessThan)
	if err != nil {
		t.Fatalf("sort: %v", err)
	}
	if !IsSorted(a, KeyOrderLessThan) {
		t.Error("not sorted")
	}
}

func TestMock1M(t *testing.T) {
	a := makeTestArray(1024 * 1024)

	err := sortForMockTest(a, KeyOrderLessThan)
	if err != nil {
		t.Fatalf("sort: %v", err)
	}
	if !IsSorted(a, KeyOrderLessThan) {
		t.Error("not sorted")
	}
}

func TestMockRandom1M(t *testing.T) {
	size := 1024 * 1024

	a := makeRandomArray(size)
	b := make([]val, size)
	copy(b, a)

	err := sortForMockTest(a, KeyLessThan)
	if err != nil {
		t.Fatalf("sort: %v", err)
	}
	if !IsSorted(a, KeyLessThan) {
		t.Error("not sorted")
	}

	// sort by order
	err = sortForTest(a, OrderLessThan)
	if err != nil {
		t.Fatalf("sort: %v", err)
	}
	for i := 0; i < len(b); i++ {
		if !Equals(b[i], a[i]) {
			t.Error("oops")
		}
	}
}
