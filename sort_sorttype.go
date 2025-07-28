package extsort

// SortType defines the interface required by the extsort library to be able to sort the items
type SortType interface {
	ToBytes() []byte // ToBytes used for marshaling
}

// FromBytes unmarshal bytes to create a SortType when reading back the sorted items
type FromBytes func([]byte) SortType

// CompareLessFunc compares two SortType items and returns true if a is less than b
type CompareLessFunc func(a, b SortType) bool

// SortTypeSorter stores adapter for legacy SortType to Generics
type SortTypeSorter struct {
	GenericSorter[SortType]
}

func sortTypeToBytes(a SortType) []byte {
	return a.ToBytes()
}

func New(input chan SortType, fromBytes FromBytes, lessFunc CompareLessFunc, config *Config) (*SortTypeSorter, chan SortType, chan error) {
	// Convert legacy types to generic types
	fromBytesGeneric := FromBytesGeneric[SortType](fromBytes)
	lessFuncGeneric := CompareLessFuncGeneric[SortType](lessFunc)

	genericSorter, output, errChan := Generic(input, fromBytesGeneric, sortTypeToBytes, lessFuncGeneric, config)
	s := &SortTypeSorter{GenericSorter: *genericSorter}
	return s, output, errChan
}

func NewMock(input chan SortType, fromBytes FromBytes, lessFunc CompareLessFunc, config *Config, n int) (*SortTypeSorter, chan SortType, chan error) {
	// Convert legacy types to generic types
	fromBytesGeneric := FromBytesGeneric[SortType](fromBytes)
	lessFuncGeneric := CompareLessFuncGeneric[SortType](lessFunc)

	genericSorter, output, errChan := MockGeneric(input, fromBytesGeneric, sortTypeToBytes, lessFuncGeneric, config, n)
	s := &SortTypeSorter{GenericSorter: *genericSorter}
	return s, output, errChan
}
