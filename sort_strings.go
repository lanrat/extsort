package extsort

// StringSorter stores adapter for legacy StringSorter to Generics
type StringSorter struct {
	GenericSorter[string]
}

func lessFuncStrings(a, b string) bool {
	return a < b
}

func fromBytesString(d []byte) string {
	return string(d)
}

func toBytesString(s string) []byte {
	return []byte(s)
}

func Strings(input chan string, config *Config) (*StringSorter, chan string, chan error) {

	genericSorter, output, errChan := Generic(input, fromBytesString, toBytesString, lessFuncStrings, config)
	s := &StringSorter{GenericSorter: *genericSorter}
	return s, output, errChan
}

func StringsMock(input chan string, config *Config, n int) (*StringSorter, chan string, chan error) {
	// Convert legacy types to generic types

	genericSorter, output, errChan := MockGeneric(input, fromBytesString, toBytesString, lessFuncStrings, config, n)
	s := &StringSorter{GenericSorter: *genericSorter}
	return s, output, errChan
}
