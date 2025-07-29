// Package diff provides functionality for comparing two sorted data streams
// and identifying differences between them. It operates efficiently on pre-sorted
// input channels and reports items that exist in only one stream or both streams.
//
// The package supports three main use cases:
//
//  1. Generic comparison using Generic() with custom comparison functions
//  2. Ordered type comparison using Ordered() with built-in comparison operators
//  3. String comparison using Strings() for backward compatibility
//
// All diff operations assume that input channels provide data in sorted order.
// This assumption is not validated for performance reasons.
//
// Example usage:
//
//	func printDifferences(a, b <-chan string, aErr, bErr <-chan error) {
//		result, err := diff.Strings(context.Background(), a, b, aErr, bErr,
//			func(d diff.Delta, s string) error {
//				fmt.Printf("%s %s\n", d, s)
//				return nil
//			})
//		if err != nil {
//			log.Fatal(err)
//		}
//		fmt.Printf("Compared %d items\n", result.TotalA+result.TotalB-result.Common)
//	}
//
// The package is designed to work efficiently with the extsort library's
// sorted output streams for comparing large datasets.
package diff

import (
	"fmt"
)

// Delta represents the type of difference found when comparing two sorted streams.
// It indicates whether an item is unique to the first stream (OLD) or second stream (NEW).
type Delta int

const (
	// NEW indicates an item that exists only in the second stream (B).
	// This represents a "new" or "added" item when comparing A to B.
	NEW = iota // +

	// OLD indicates an item that exists only in the first stream (A).
	// This represents an "old" or "removed" item when comparing A to B.
	OLD // -
)

// ResultFunc is a generic callback function type for processing diff results.
// It is called once for each item that appears in only one of the two streams.
// The Delta parameter indicates which stream the item belongs to (NEW or OLD).
// The T parameter contains the actual item value.
// If the function returns an error, the diff operation will be terminated.
type ResultFunc[T any] func(Delta, T) error

// StringResultFunc is a type alias for ResultFunc[string] that provides
// backward compatibility with the original string-specific diff API.
type StringResultFunc ResultFunc[string]

// CompareFunc defines a comparison function for ordering items of type T.
// It should return a negative value if the first argument is less than the second,
// zero if they are equal, and a positive value if the first is greater than the second.
// This follows the same convention as strings.Compare and cmp.Compare.
type CompareFunc[T any] func(T, T) int

// StringResultFunc is a callback function type for processing diff results.
// It is called once for each item that appears in only one of the two streams.
// The Delta parameter indicates which stream the item belongs to (NEW or OLD).
// The string parameter contains the actual item value.
// If the function returns an error, the diff operation will be terminated.
//type StringResultFunc func(Delta, string) error

func (d Delta) String() string {
	switch d {
	case NEW:
		return ">"
	case OLD:
		return "<"
	default:
		return "?"
	}
}

// Result contains statistical information about the differences between two sorted streams.
// It provides counts of items that are unique to each stream as well as common items.
type Result struct {
	// ExtraA is the count of items that exist only in stream A (OLD items)
	ExtraA uint64

	// ExtraB is the count of items that exist only in stream B (NEW items)
	ExtraB uint64

	// TotalA is the total count of items processed from stream A
	TotalA uint64

	// TotalB is the total count of items processed from stream B
	TotalB uint64

	// Common is the count of items that exist in both streams
	Common uint64
}

func (r *Result) String() string {
	out := fmt.Sprintf("A: %d/%d\tB: %d/%d\tC: %d", r.ExtraA, r.TotalA, r.ExtraB, r.TotalB, r.Common)
	return out
}
