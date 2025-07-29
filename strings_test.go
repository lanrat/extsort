package extsort_test

// this test package is heavily bases on the one for psilva261's timsort
// https://github.com/psilva261/timsort/blob/master/timsort_test.go

import (
	"context"
	"fmt"
	"math/rand"
	"testing"

	"github.com/lanrat/extsort"
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
	config := extsort.DefaultConfig()
	config.ChunkSize = len(inputData)/20 + 100

	// Create cancelable context for proper cleanup
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sort, outChan, errChan := extsort.Strings(inputChan, config)
	sort.Sort(ctx)

	i := 0
	for {
		select {
		case rec, ok := <-outChan:
			if !ok {
				if err := <-errChan; err != nil {
					return err
				}
				return nil
			}
			inputData[i] = rec
			i++
		case err := <-errChan:
			if err != nil {
				return err
			}
			for rec := range outChan {
				inputData[i] = rec
				i++
			}
			return nil
		}
	}
}

func makeTestStringArray(size int) []string {
	a := make([]string, size)

	for i := 0; i < size; i++ {
		a[i] = fmt.Sprintf("%d", i&0xeeeeee)
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
