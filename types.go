package extsort

import "context"

// Sorter is the interface that all extsort sorter implementations must satisfy.
// It provides a single Sort method that performs the complete external sorting operation
// within the provided context, allowing for cancellation and timeout control.
type Sorter interface {
	// Sort performs the complete external sorting operation.
	// It reads from the input channel, sorts the data using temporary storage,
	// and delivers results to the output channel. The operation can be cancelled
	// or timed out using the provided context.
	Sort(context.Context)
}

// FromBytesGeneric is a function type for deserializing bytes back to type E.
// It's used during the merge phase to reconstruct items from temporary storage.
// The function should be the inverse of the corresponding ToBytesGeneric function,
// and must handle any errors by panicking (which will be caught and wrapped).
type FromBytesGeneric[E any] func([]byte) E

// ToBytesGeneric is a function type for serializing type E to bytes.
// It's used during chunk saving to store items in temporary files.
// The function should produce deterministic output that can be read back
// by the corresponding FromBytesGeneric function, and must handle any errors by panicking.
type ToBytesGeneric[E any] func(E) []byte

// CompareLessFuncGeneric is a function type for comparing two items of type E.
// It must implement a strict weak ordering: reflexivity, antisymmetry, and transitivity.
// Returns true if the first argument should be ordered before the second in the final sorted output.
// The function must be consistent and must handle any errors by panicking.
type CompareLessFuncGeneric[E any] func(a, b E) bool
