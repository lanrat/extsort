package extsort

// StringSorter provides external sorting for strings, maintaining backward compatibility
// with the legacy string-specific API. It embeds GenericSorter[string] and uses
// simple byte slice conversion for serialization.
type StringSorter struct {
	GenericSorter[string]
}

// lessFuncStrings provides lexicographic string comparison.
// Returns true if string a is lexicographically less than string b.
func lessFuncStrings(a, b string) bool {
	return a < b
}

// fromBytesString converts a byte slice back to a string.
// This is used for deserialization during the external sort process.
func fromBytesString(d []byte) string {
	return string(d)
}

// toBytesString converts a string to a byte slice for serialization.
// This enables strings to be written to and read from temporary files during sorting.
func toBytesString(s string) []byte {
	return []byte(s)
}

// Strings performs external sorting on a channel of strings using lexicographic ordering.
// Returns the sorter instance, output channel with sorted strings, and error channel.
// This function provides backward compatibility with the legacy string-specific API.
func Strings(input <-chan string, config *Config) (*StringSorter, <-chan string, <-chan error) {
	genericSorter, output, errChan := Generic(input, fromBytesString, toBytesString, lessFuncStrings, config)
	s := &StringSorter{GenericSorter: *genericSorter}
	return s, output, errChan
}

// StringsMock performs external sorting on strings with a mock implementation that limits
// the number of strings to sort. Useful for testing with a controlled dataset size.
// The parameter n specifies the maximum number of strings to process.
func StringsMock(input <-chan string, config *Config, n int) (*StringSorter, <-chan string, <-chan error) {
	genericSorter, output, errChan := MockGeneric(input, fromBytesString, toBytesString, lessFuncStrings, config, n)
	s := &StringSorter{GenericSorter: *genericSorter}
	return s, output, errChan
}
