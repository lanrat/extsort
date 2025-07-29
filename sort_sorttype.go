package extsort

// SortType defines the interface required by the extsort library to be able to sort the items
type SortType interface {
	ToBytes() []byte // ToBytes used for marshaling
}

// FromBytes unmarshal bytes to create a SortType when reading back the sorted items
type FromBytes func([]byte) SortType

// CompareLessFunc compares two SortType items and returns true if a is less than b
type CompareLessFunc func(a, b SortType) bool

// SortTypeSorter provides external sorting for types implementing the SortType interface,
// maintaining backward compatibility with the legacy interface-based API.
// It embeds GenericSorter[SortType] and adapts the interface methods to the generic implementation.
type SortTypeSorter struct {
	GenericSorter[SortType]
}

// sortTypeToBytes converts a SortType to bytes by calling its ToBytes method.
// This adapter function enables SortType interface compatibility with the generic sorter.
func sortTypeToBytes(a SortType) []byte {
	return a.ToBytes()
}

// New performs external sorting on a channel of SortType items using the legacy interface-based API.
// It takes a FromBytes function for deserialization and a CompareLessFunc for comparison.
// Returns the sorter instance, output channel with sorted items, and error channel.
// This function provides backward compatibility with the original extsort API.
func New(input <-chan SortType, fromBytes FromBytes, lessFunc CompareLessFunc, config *Config) (*SortTypeSorter, <-chan SortType, <-chan error) {
	// Convert legacy types to generic types
	fromBytesGeneric := FromBytesGeneric[SortType](fromBytes)
	lessFuncGeneric := CompareLessFuncGeneric[SortType](lessFunc)

	genericSorter, output, errChan := Generic(input, fromBytesGeneric, sortTypeToBytes, lessFuncGeneric, config)
	s := &SortTypeSorter{GenericSorter: *genericSorter}
	return s, output, errChan
}

// NewMock performs external sorting on SortType items with a mock implementation that limits
// the number of items to sort. Useful for testing with a controlled dataset size.
// The parameter n specifies the maximum number of items to process.
// Uses the same interface-based API as New for backward compatibility.
func NewMock(input <-chan SortType, fromBytes FromBytes, lessFunc CompareLessFunc, config *Config, n int) (*SortTypeSorter, <-chan SortType, <-chan error) {
	// Convert legacy types to generic types
	fromBytesGeneric := FromBytesGeneric[SortType](fromBytes)
	lessFuncGeneric := CompareLessFuncGeneric[SortType](lessFunc)

	genericSorter, output, errChan := MockGeneric(input, fromBytesGeneric, sortTypeToBytes, lessFuncGeneric, config, n)
	s := &SortTypeSorter{GenericSorter: *genericSorter}
	return s, output, errChan
}
