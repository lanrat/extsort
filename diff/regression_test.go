package diff_test

// Regression tests for diff hangs and the Delta constants.

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/lanrat/extsort/diff"
)

// stream returns a closed channel holding items.
func stream(items ...string) chan string {
	ch := make(chan string, len(items))
	for _, s := range items {
		ch <- s
	}
	close(ch)
	return ch
}

func ignoreResult(diff.Delta, string) error { return nil }

// runDiff runs diff.Strings and fails the test if it does not return in time.
func runDiff(t *testing.T, ctx context.Context, a, b <-chan string, aErr, bErr <-chan error, f diff.StringResultFunc) (diff.Result, error) {
	t.Helper()
	type result struct {
		r   diff.Result
		err error
	}
	done := make(chan result, 1)
	go func() {
		r, err := diff.Strings(ctx, a, b, aErr, bErr, f)
		done <- result{r, err}
	}()
	select {
	case res := <-done:
		return res.r, res.err
	case <-time.After(5 * time.Second):
		t.Fatal("diff did not return within 5s")
		return diff.Result{}, nil
	}
}

// The error channel of the stream that ended first used to be read twice, so a
// caller that sent one value without closing the channel hung the diff.
func TestErrChanOfShorterStreamIsReadOnce(t *testing.T) {
	for _, tc := range []struct {
		name string
		a, b []string
	}{
		{"A ends first", []string{"a"}, []string{"a", "b", "c"}},
		{"B ends first", []string{"a", "b", "c"}, []string{"a"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			aErr, bErr := make(chan error, 1), make(chan error, 1)
			aErr <- nil // one value each, never closed
			bErr <- nil
			r, err := runDiff(t, context.Background(), stream(tc.a...), stream(tc.b...), aErr, bErr, ignoreResult)
			if err != nil {
				t.Fatal(err)
			}
			if r.Common != 1 || r.ExtraA+r.ExtraB != 2 {
				t.Errorf("unexpected result %s", r.String())
			}
		})
	}
}

// Reading an error channel ignored ctx, so a channel that is never sent to or closed
// hung the diff even past the ctx deadline.
func TestErrChanReadRespectsContext(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	never := make(chan error)
	_, err := runDiff(t, ctx, stream("a"), stream("b"), never, never, ignoreResult)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("got error %v, want %v", err, context.DeadlineExceeded)
	}
}

// NEW and OLD used to be untyped integer constants, so they printed as 0 and 1
// instead of using Delta's String method.
func TestDeltaConstantsAreTyped(t *testing.T) {
	if got := fmt.Sprint(diff.NEW, diff.OLD); got != "> <" {
		t.Errorf("fmt.Sprint(NEW, OLD) = %q, want %q", got, "> <")
	}
}

// The function returned by StringResultChan blocks on its send even after ctx is done,
// so a diff whose results are no longer read hangs. StringResultChanContext stops waiting.
func TestStringResultChanContextStopsWaiting(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	resultFunc, results := diff.StringResultChanContext(ctx)
	defer close(results)
	noErr := make(chan error)
	close(noErr)
	time.AfterFunc(50*time.Millisecond, cancel)
	// three differences, and nobody reads the results channel
	_, err := runDiff(t, ctx, stream("a1", "a2", "a3"), stream(), noErr, noErr, resultFunc)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("got error %v, want %v", err, context.Canceled)
	}
}
