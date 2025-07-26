package extsort_test

import (
	"testing"

	"github.com/lanrat/extsort"
)

func TestSortTypeInterface(t *testing.T) {
	s, _, _ := extsort.New(nil, nil, nil, nil)
	onlySortersAllowed(s)
}

func TestStringsInterface(t *testing.T) {
	s, _, _ := extsort.Strings(nil, nil)
	onlySortersAllowed(s)
}

func onlySortersAllowed(_ extsort.Sorter) bool {
	return true
}
