package extsort

// this test package is heavily bases on the one for psilva261's timsort
// https://github.com/psilva261/timsort/blob/master/timsort_test.go

import (
	"context"
	"encoding/json"
	"math/rand"
	"testing"
)

func fromBytesForTest(data []byte) SortType {
	var v val
	err := json.Unmarshal(data, &v)
	if err != nil {
		panic(err)
	}
	return v
}

func (v val) ToBytes() []byte {
	bytes, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return bytes
}

func sortForTest(inputData []val, lessFunc CompareLessFunc) error {
	// make array of all data in chan
	inputChan := make(chan SortType, 2)
	go func() {
		for _, d := range inputData {
			inputChan <- d
		}
		close(inputChan)
	}()
	config := DefaultConfig()
	config.ChunkSize = len(inputData)/20 + 100
	sort := New(context.Background(), inputChan, fromBytesForTest, lessFunc, config)
	outChan, errChan := sort.Sort()
	i := 0
	for {
		select {
		case err := <-errChan:
			return err
		case rec, more := <-outChan:
			if !more {
				return nil
			}
			inputData[i] = rec.(val)
			i++
		}
	}
}

type val struct {
	Key, Order int
}

func makeTestArray(size int) []val {
	a := make([]val, size)

	for i := 0; i < size; i++ {
		a[i] = val{i & 0xeeeeee, i}
	}

	return a
}

func IsSorted(a []val, lessThan CompareLessFunc) bool {
	len := len(a)

	if len < 2 {
		return true
	}

	prev := a[0]
	for i := 1; i < len; i++ {
		if lessThan(a[i], prev) {
			return false
		}
		prev = a[i]
	}

	return true
}

func TestIsSorted(t *testing.T) {
	a := make([]val, 5)
	a[0] = val{3, 1}
	a[1] = val{1, 5}
	a[2] = val{2, 3}
	a[3] = val{3, 4}
	a[4] = val{4, 5}

	if IsSorted(a, OrderLessThan) {
		t.Error("Sorted")
	}
}

// use this comparator for sorting
func KeyLessThan(a, b SortType) bool {
	return a.(val).Key < b.(val).Key
}

type KeyLessThanSlice []val

func (s KeyLessThanSlice) Len() int {
	return len(s)
}

func (s KeyLessThanSlice) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s KeyLessThanSlice) Less(i, j int) bool {
	return s[i].Key < s[j].Key
}

// use this comparator to validate sorted data (and prove its stable)
func KeyOrderLessThan(ar, br SortType) bool {
	a := ar.(val)
	b := br.(val)
	if a.Key < b.Key {
		return true
	} else if a.Key == b.Key {
		return a.Order < b.Order
	}

	return false
}

// use this comparator to restore the original order of elements (by sorting on order field)
func OrderLessThan(a, b SortType) bool {
	return a.(val).Order < b.(val).Order
}

type OrderLessThanSlice []val

func (s OrderLessThanSlice) Len() int {
	return len(s)
}

func (s OrderLessThanSlice) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s OrderLessThanSlice) Less(i, j int) bool {
	return s[i].Order < s[j].Order
}

func Test50(t *testing.T) {
	a := makeTestArray(50)
	if IsSorted(a, KeyLessThan) {
		t.Error("sorted before starting")
	}

	err := sortForTest(a, KeyLessThan)
	if err != nil {
		t.Fatalf("sort: %v", err)
	}

	if !IsSorted(a, KeyLessThan) {
		t.Error("not sorted")
	}
}

func TestSmoke(t *testing.T) {
	a := make([]val, 3)
	a[0] = val{3, 0}
	a[1] = val{1, 1}
	a[2] = val{2, 2}

	err := sortForTest(a, KeyLessThan)
	if err != nil {
		t.Fatalf("sort: %v", err)
	}

	if !IsSorted(a, KeyLessThan) {
		t.Error("not sorted")
	}
}

func TestSmokeStability(t *testing.T) {
	a := make([]val, 3)
	a[0] = val{3, 0}
	a[1] = val{2, 1}
	a[2] = val{2, 2}

	err := sortForTest(a, KeyOrderLessThan)
	if err != nil {
		t.Fatalf("sort: %v", err)
	}

	if !IsSorted(a, KeyOrderLessThan) {
		t.Error("not sorted")
	}
}

func Test1K(t *testing.T) {
	a := makeTestArray(1024)

	err := sortForTest(a, KeyOrderLessThan)
	if err != nil {
		t.Fatalf("sort: %v", err)
	}
	if !IsSorted(a, KeyOrderLessThan) {
		t.Error("not sorted")
	}
}

func Test1M(t *testing.T) {
	a := makeTestArray(1024 * 1024)

	err := sortForTest(a, KeyOrderLessThan)
	if err != nil {
		t.Fatalf("sort: %v", err)
	}
	if !IsSorted(a, KeyOrderLessThan) {
		t.Error("not sorted")
	}
}

func makeRandomArray(size int) []val {
	a := make([]val, size)

	for i := 0; i < size; i++ {
		a[i] = val{rand.Intn(100), i}
	}

	return a
}

func Equals(a, b val) bool {
	return a.Key == b.Key && a.Order == b.Order
}

func TestRandom1M(t *testing.T) {
	size := 1024 * 1024

	a := makeRandomArray(size)
	b := make([]val, size)
	copy(b, a)

	err := sortForTest(a, KeyLessThan)
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
