package diff

// StringChanResult holds a single diff result from a string comparison.
// It contains both the difference type (NEW/OLD) and the actual string value.
// This type is used with StringResultChan to enable parallel processing of diff results.
type StringChanResult struct {
	// D indicates whether the string is NEW (only in stream B) or OLD (only in stream A)
	D Delta
	// S contains the actual string value that differs between streams
	S string
}

// StringResultChan creates a channel-based result processing system for string diffs.
// It returns a StringResultFunc that can be passed to diff.Strings() and a channel
// for consuming the results in a separate goroutine. This enables parallel processing
// where the diff operation runs in one goroutine while results are processed in another.
//
// Returns:
//   - StringResultFunc: A callback function to pass to diff.Strings()
//   - chan *StringChanResult: A channel to receive diff results from
//
// The caller is responsible for closing the returned channel when done.
func StringResultChan() (StringResultFunc, chan *StringChanResult) {
	c := make(chan *StringChanResult, 1)
	f := func(d Delta, s string) error {
		c <- &StringChanResult{D: d, S: s}
		return nil
	}
	return f, c
}
