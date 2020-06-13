package diff_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/lanrat/extsort/diff"
)

func TestNil(t *testing.T) {
	r, err := diff.Strings(context.Background(), nil, nil, nil, nil, nil)
	if err == nil {
		t.Fatal("diff.Strings(nil, nil, nil, nil, nil) should error")
	}
	if r.ExtraA+r.ExtraB+r.TotalA+r.TotalB+r.Common != 0 {
		t.Fatalf("results Count not 0 %s", r.String())
	}
}

func Test1A(t *testing.T) {
	aChan := make(chan string)
	bChan := make(chan string)
	aErrChan := make(chan error)
	bErrChan := make(chan error)
	resultF := func(d diff.Delta, s string) error {
		//log.Printf("result: %s %q", d.String(), s)
		return nil
	}
	close(bChan)
	close(bErrChan)
	go func() {
		aChan <- "Hello A"
		close(aChan)
		close(aErrChan)
	}()
	r, err := diff.Strings(context.Background(), aChan, bChan, aErrChan, bErrChan, resultF)
	if err != nil {
		t.Fatal(err)
	}
	if r.ExtraA != 1 || r.ExtraB != 0 || r.TotalA != 1 || r.TotalB != 0 || r.Common != 0 {
		t.Fatalf("results count not a+1 %s", r.String())
	}
}

func Test1B(t *testing.T) {
	aChan := make(chan string)
	bChan := make(chan string)
	aErrChan := make(chan error)
	bErrChan := make(chan error)
	resultF := func(d diff.Delta, s string) error {
		//log.Printf("result: %s %q", d.String(), s)
		return nil
	}
	close(aChan)
	close(aErrChan)
	go func() {
		bChan <- "Hello B"
		close(bChan)
		close(bErrChan)
	}()
	r, err := diff.Strings(context.Background(), aChan, bChan, aErrChan, bErrChan, resultF)
	if err != nil {
		t.Fatal(err)
	}
	if r.ExtraA != 0 || r.ExtraB != 1 || r.TotalA != 0 || r.TotalB != 1 || r.Common != 0 {
		t.Fatalf("results count not a+1 %s", r.String())
	}
}

func TestCommon(t *testing.T) {
	aChan := make(chan string)
	bChan := make(chan string)
	aErrChan := make(chan error)
	bErrChan := make(chan error)
	resultF := func(d diff.Delta, s string) error {
		t.Fatalf("common resultF called for %s %q", d, s)
		return nil
	}
	go func() {
		for i := 0; i < 30; i++ {
			aChan <- fmt.Sprintf("%d", i)
			bChan <- fmt.Sprintf("%d", i)
		}
		close(bChan)
		close(bErrChan)
		close(aChan)
		close(aErrChan)
	}()
	r, err := diff.Strings(context.Background(), aChan, bChan, aErrChan, bErrChan, resultF)
	if err != nil {
		t.Fatal(err)
	}
	if r.ExtraA != 0 || r.ExtraB != 0 || r.TotalA != 30 || r.TotalB != 30 || r.Common != 30 {
		t.Fatalf("results count not 30 common %s", r.String())
	}
}

func TestMix(t *testing.T) {
	aChan := make(chan string)
	bChan := make(chan string)
	aErrChan := make(chan error)
	bErrChan := make(chan error)
	resultF := func(d diff.Delta, s string) error {
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
	r, err := diff.Strings(context.Background(), aChan, bChan, aErrChan, bErrChan, resultF)
	if err != nil {
		t.Fatal(err)
	}
	if r.ExtraA != 15 || r.ExtraB != 15 || r.TotalA != 75 || r.TotalB != 75 || r.Common != 60 {
		t.Fatalf("results count not 30/15/15/30 common %s", r.String())
	}
}

func TestError(t *testing.T) {
	aChan := make(chan string)
	bChan := make(chan string)
	aErrChan := make(chan error, 1)
	bErrChan := make(chan error, 1)
	resultF := func(d diff.Delta, s string) error {
		//log.Printf("result: %s %q", d.String(), s)
		return nil
	}
	testErr := fmt.Errorf("random error")
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
		go func() {
			bErrChan <- testErr
			close(bChan)
			close(bErrChan)
		}()
		for i := 60; i < 90; i++ {
			aChan <- fmt.Sprintf("%d", i)
		}
		close(aChan)
		close(aErrChan)
	}()
	r, err := diff.Strings(context.Background(), aChan, bChan, aErrChan, bErrChan, resultF)
	if err != testErr {
		t.Fatalf("err was not expected %s", err)
	}
	if r.ExtraA != 15 || r.ExtraB != 15 || r.TotalA != 45 || r.TotalB != 45 || r.Common != 30 {
		t.Fatalf("results count not 15/45 15/45 30 common %s", r.String())
	}
}
