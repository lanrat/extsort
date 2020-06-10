package extsort

import (
	"fmt"
	"testing"
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

	uniq := UniqString(in)

	past := ""
	for u := range uniq {
		if u == past {
			t.Fatalf("got duplicate %q", u)
		}
		past = u
	}
}
