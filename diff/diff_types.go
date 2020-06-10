package diff

import "fmt"

// Delta represents the differential value passed to ResultFunc: NEW/OLD
type Delta int

const (
	// NEW is an enum for a new/added value in a diff passed to ResultFunc
	NEW = iota // +
	// OLD is an enum for a removed value in a diff  passed to ResultFunc
	OLD // -
)

// StringResultFunc defines the interface for a function to
// be called for each delta record
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

// Result stores statistics generated from diffing two streams
type Result struct {
	ExtraA int64
	ExtraB int64
	TotalA int64
	TotalB int64
	Common int64
}

func (r *Result) String() string {
	out := fmt.Sprintf("A: %d/%d\tB: %d/%d\tC: %d", r.ExtraA, r.TotalA, r.ExtraB, r.TotalB, r.Common)
	return out
}
