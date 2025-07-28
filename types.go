package extsort

import "context"

// Sorter is the interface that all extsort sorters must satisfy
type Sorter interface {
	Sort(context.Context)
}

// FromBytesGeneric unmarshal bytes to create a E when reading back the sorted items
type FromBytesGeneric[E any] func([]byte) E

// ToBytesGeneric marshal the type E to []byte
type ToBytesGeneric[E any] func(E) []byte

// CompareLessFuncGeneric compares two E items and returns true if a is less than b
type CompareLessFuncGeneric[E any] func(a, b E) bool
