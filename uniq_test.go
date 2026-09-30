package extsort_test

import (
	"context"
	"fmt"
	"slices"
	"testing"

	"github.com/lanrat/extsort"
)

func TestUniqString(t *testing.T) {
	in := make(chan string, 10)

	go func() {
		for i := 0; i < 30; i++ {
			in <- fmt.Sprintf("%d", i)
			if i%2 == 0 {
				in <- fmt.Sprintf("%d", i)
			}
		}
		close(in)
	}()

	uniq := extsort.UniqStringChan(in)

	past := ""
	for u := range uniq {
		if u == past {
			t.Fatalf("got duplicate %q", u)
		}
		past = u
	}
}

// UniqStringChan used to take a bidirectional chan string, so it could not accept the
// <-chan string returned by Strings, and its output channel was unbuffered.
func TestUniqStringChanAcceptsSorterOutput(t *testing.T) {
	in := make(chan string, 6)
	for _, s := range []string{"b", "a", "b", "c", "a", "b"} {
		in <- s
	}
	close(in)
	sorter, sorted, errc := extsort.Strings(in, nil)
	sorter.Sort(context.Background())

	uniq := extsort.UniqStringChan(sorted)
	if cap(uniq) == 0 {
		t.Error("output channel is unbuffered")
	}
	var got []string
	for s := range uniq {
		got = append(got, s)
	}
	if err := <-errc; err != nil {
		t.Fatal(err)
	}
	if want := []string{"a", "b", "c"}; !slices.Equal(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}
