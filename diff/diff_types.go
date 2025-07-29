package diff

import "fmt"

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

// StringResultFunc is a callback function type for processing diff results.
// It is called once for each item that appears in only one of the two streams.
// The Delta parameter indicates which stream the item belongs to (NEW or OLD).
// The string parameter contains the actual item value.
// If the function returns an error, the diff operation will be terminated.
type StringResultFunc func(Delta, string) error

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
