package diff

import (
	"cmp"
	"context"
)

// Ordered performs a diff operation on two sorted channels of cmp.Ordered types.
// It uses the standard comparison operators (<, ==, >) for ordering, making it
// convenient for built-in types like numbers and strings. This is a wrapper around
// Generic that provides the comparison function automatically.
//
// Parameters:
//   - ctx: Context for cancellation and timeout control
//   - aChan, bChan: Sorted channels to compare (MUST be pre-sorted)
//   - aErrChan, bErrChan: Error channels corresponding to each data channel
//   - resultFunc: Callback function called for each difference found
//
// Returns statistical information about the comparison and any errors encountered.
// The channels must provide items in ascending sorted order.
func Ordered[T cmp.Ordered](ctx context.Context, aChan, bChan <-chan T, aErrChan, bErrChan <-chan error, resultFunc ResultFunc[T]) (r Result, err error) {
	return Generic(ctx, aChan, bChan, aErrChan, bErrChan, cmp.Compare[T], resultFunc)
}

// Strings performs a diff operation on two sorted string channels.
// This is a convenience function that uses lexicographic string comparison.
// It's equivalent to calling Ordered[string] but uses the StringResultFunc type
// for backward compatibility.
//
// Parameters:
//   - ctx: Context for cancellation and timeout control
//   - aChan, bChan: Sorted string channels to compare (MUST be pre-sorted)
//   - aErrChan, bErrChan: Error channels corresponding to each string channel
//   - resultFunc: Callback function called for each string difference found
//
// Returns statistical information about the comparison and any errors encountered.
func Strings(ctx context.Context, aChan, bChan <-chan string, aErrChan, bErrChan <-chan error, resultFunc StringResultFunc) (r Result, err error) {
	return Ordered(ctx, aChan, bChan, aErrChan, bErrChan, ResultFunc[string](resultFunc))
}
