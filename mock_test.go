package extsort_test

import (
	"context"
	"testing"

	"github.com/lanrat/extsort"
)

func Test50StringMock(t *testing.T) {
	a := makeTestStringArray(50)
	if IsStringsSorted(a) {
		t.Error("sorted before starting")
	}

	err := sortStringForTestMock(a)
	if err != nil {
		t.Fatalf("sort: %v", err)
	}

	if !IsStringsSorted(a) {
		t.Error("not sorted")
	}
}

func TestStringSmokeMock(t *testing.T) {
	a := make([]string, 3)
	a[0] = "banana"
	a[1] = "orange"
	a[2] = "apple"

	err := sortStringForTestMock(a)
	if err != nil {
		t.Fatalf("sort: %v", err)
	}

	if !IsStringsSorted(a) {
		t.Error("not sorted")
	}
}

func Test1KStringsMock(t *testing.T) {
	a := makeTestStringArray(1024)

	err := sortStringForTestMock(a)
	if err != nil {
		t.Fatalf("sort: %v", err)
	}
	if !IsStringsSorted(a) {
		t.Error("not sorted")
	}
}

func TestRandom1MStringMock(t *testing.T) {
	size := 1024 * 1024

	a := makeRandomStringArray(size)

	err := sortStringForTestMock(a)
	if err != nil {
		t.Fatalf("sort: %v", err)
	}
	if !IsStringsSorted(a) {
		t.Error("not sorted")
	}
}

func sortStringForTestMock(inputData []string) error {
	// make array of all data in chan
	inputChan := make(chan string, 2)
	go func() {
		for _, d := range inputData {
			inputChan <- d
		}
		close(inputChan)
	}()
	sort := extsort.StringsMock(inputChan, nil, 0)
	outChan, errChan := sort.Sort(context.Background())
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
