package diff

import (
	"fmt"
	"testing"
)

func TestSortDiff(t *testing.T) {
	// TODO make this test to find the best pattern to using sort & diff togeather

	aChan := make(chan string)
	bChan := make(chan string)
	aErrChan := make(chan error)
	bErrChan := make(chan error)
	resultF := func(d Delta, s string) error {
		//log.Printf("result: %s %q", d.String(), s)
		return nil
	}
	go func() {
		for i := 0; i < 30; i++ {
			aChan <- fmt.Sprintf("%d", i)
			bChan <- fmt.Sprintf("%d", i)
		}
		for i := 30; i < 60; i++ {
			if i%2 == 0 {
				aChan <- fmt.Sprintf("%d", i)
			} else {
				bChan <- fmt.Sprintf("%d", i)
			}
		}
		for i := 60; i < 90; i++ {
			aChan <- fmt.Sprintf("%d", i)
			bChan <- fmt.Sprintf("%d", i)
		}
		close(bChan)
		close(bErrChan)
		close(aChan)
		close(aErrChan)
	}()
	r, err := Strings(aChan, bChan, aErrChan, bErrChan, resultF)
	if err != nil {
		t.Fatal(err)
	}
	if r.ExtraA != 15 || r.ExtraB != 15 || r.TotalA != 75 || r.TotalB != 75 || r.Common != 60 {
		t.Fatalf("results count not 30/15/15/30 common %s", r.String())
	}
}
