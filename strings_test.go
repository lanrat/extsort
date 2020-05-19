package extsort

// this test package is heavily bases on the one for psilva261's timsort
// https://github.com/psilva261/timsort/blob/master/timsort_test.go

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
)

func sortStringForTest(inputData []string) error {
	// make array of all data in chan
	inputChan := make(chan string, 2)
	go func() {
		for _, d := range inputData {
			inputChan <- d
		}
		close(inputChan)
	}()
	config := DefaultConfig()
	config.ChunkSize = len(inputData)/20 + 100
	sort := StringsContext(context.Background(), inputChan, config)
	outChan, errChan := sort.Sort()
	i := 0
	for rec := range outChan {
		inputData[i] = rec
		i++
	}
	if err := <-errChan; err != nil {
		return err
	}
	return nil
}

func makeTestStringArray(size int) []string {
	a := make([]string, size)

	for i := 0; i < size; i++ {
		a[i] = fmt.Sprintf("%d", i)
	}

	return a
}

func Test50String(t *testing.T) {
	a := makeTestStringArray(50)
	if IsStringsSorted(a) {
		t.Error("sorted before starting")
	}

	err := sortStringForTest(a)
	if err != nil {
		t.Fatalf("sort: %v", err)
	}

	if !IsStringsSorted(a) {
		t.Error("not sorted")
	}
}

func IsStringsSorted(a []string) bool {
	len := len(a)

	if len < 2 {
		return true
	}

	prev := a[0]
	for i := 1; i < len; i++ {
		if a[i] < prev {
			return false
		}
		prev = a[i]
	}

	return true
}

func TestStringSmoke(t *testing.T) {
	a := make([]string, 3)
	a[0] = "banana"
	a[1] = "orange"
	a[2] = "apple"

	err := sortStringForTest(a)
	if err != nil {
		t.Fatalf("sort: %v", err)
	}

	if !IsStringsSorted(a) {
		t.Error("not sorted")
	}
}

func Test1KStrings(t *testing.T) {
	a := makeTestStringArray(1024)

	err := sortStringForTest(a)
	if err != nil {
		t.Fatalf("sort: %v", err)
	}
	if !IsStringsSorted(a) {
		t.Error("not sorted")
	}
}

func makeRandomStringArray(size int) []string {
	a := make([]string, size)

	for i := 0; i < size; i++ {
		a[i] = fmt.Sprintf("%d %d", rand.Intn(100), i)
	}

	return a
}

func TestRandom1MString(t *testing.T) {
	size := 1024 * 1024

	a := makeRandomStringArray(size)

	err := sortStringForTest(a)
	if err != nil {
		t.Fatalf("sort: %v", err)
	}
	if !IsStringsSorted(a) {
		t.Error("not sorted")
	}
}
