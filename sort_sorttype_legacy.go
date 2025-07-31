package extsort

// SortType defines the interface required by the extsort library to be able to sort the items
//
// Deprecated: Use Generic() with custom types instead for new code. This interface is maintained for backward compatibility.
type SortType interface {
	ToBytes() []byte // ToBytes used for marshaling
}

// FromBytes unmarshal bytes to create a SortType when reading back the sorted items
//
// Deprecated: Use FromBytesGeneric[T] instead for new code. This type is maintained for backward compatibility.
type FromBytes func([]byte) SortType

// CompareLessFunc compares two SortType items and returns true if a is less than b
//
// Deprecated: Use CompareGeneric[T] instead for new code. This type is maintained for backward compatibility.
type CompareLessFunc func(a, b SortType) bool

// SortTypeSorter provides external sorting for types implementing the SortType interface,
// maintaining backward compatibility with the legacy interface-based API.
// It embeds GenericSorter[SortType] and adapts the interface methods to the generic implementation.
//
// Deprecated: Use GenericSorter[T] instead for new code. This type is maintained for backward compatibility.
type SortTypeSorter struct {
	GenericSorter[SortType]
}

// sortTypeToBytes converts a SortType to bytes by calling its ToBytes method.
// This adapter function enables SortType interface compatibility with the generic sorter.
func sortTypeToBytes(a SortType) []byte {
	return a.ToBytes()
}

func makeCompareSortType(lessFunc CompareLessFunc) func(a, b SortType) int {
	return func(a, b SortType) int {
		if lessFunc(a, b) {
			return -1
		}
		return 1
	}
}

// New performs external sorting on a channel of SortType items using the legacy interface-based API.
// It takes a FromBytes function for deserialization and a CompareLessFunc for comparison.
// Returns the sorter instance, output channel with sorted items, and error channel.
// This function provides backward compatibility with the original extsort API.
//
// Deprecated: Use Generic() instead for new code. This function is maintained for backward compatibility.
func New(input <-chan SortType, fromBytes FromBytes, lessFunc CompareLessFunc, config *Config) (*SortTypeSorter, <-chan SortType, <-chan error) {
	// Convert legacy types to generic types
	fromBytesGeneric := FromBytesGeneric[SortType](fromBytes)
	compareGeneric := makeCompareSortType(lessFunc)

	genericSorter, output, errChan := Generic(input, fromBytesGeneric, sortTypeToBytes, compareGeneric, config)
	s := &SortTypeSorter{GenericSorter: *genericSorter}
	return s, output, errChan
}

// NewMock performs external sorting on SortType items with a mock implementation that limits
// the number of items to sort. Useful for testing with a controlled dataset size.
// The parameter n specifies the maximum number of items to process.
// Uses the same interface-based API as New for backward compatibility.
//
// Deprecated: Use MockGeneric() instead for new code. This function is maintained for backward compatibility.
func NewMock(input <-chan SortType, fromBytes FromBytes, lessFunc CompareLessFunc, config *Config, n int) (*SortTypeSorter, <-chan SortType, <-chan error) {
	// Convert legacy types to generic types
	fromBytesGeneric := FromBytesGeneric[SortType](fromBytes)
	compareGeneric := makeCompareSortType(lessFunc)

	genericSorter, output, errChan := MockGeneric(input, fromBytesGeneric, sortTypeToBytes, compareGeneric, config, n)
	s := &SortTypeSorter{GenericSorter: *genericSorter}
	return s, output, errChan
}
