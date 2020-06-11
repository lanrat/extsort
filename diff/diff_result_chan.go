package diff

// StringChanResult hold diff results for DiffStringResultChan
type StringChanResult struct {
	D Delta
	S string
}

// StringResultChan allows diff.Strings() to be called in parallel with its resultFunc prosessing
// this function returns a StringResultFunc for use in diff.Strings(), and a channel for consuming
// diff results in another goroutine
func StringResultChan() (StringResultFunc, chan *StringChanResult) {
	c := make(chan *StringChanResult, 1)
	f := func(d Delta, s string) error {
		c <- &StringChanResult{D: d, S: s}
		return nil
	}
	return f, c
}
