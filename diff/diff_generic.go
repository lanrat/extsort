package diff

import (
	"context"
	"fmt"
)

// differ is an internal struct that holds the state for performing diff operations
// between two sorted channels of type T. It manages the comparison logic and
// result reporting through callback functions.
type differ[T any] struct {
	ctx                context.Context
	aChan, bChan       <-chan T
	aErrChan, bErrChan <-chan error
	resultFunc         ResultFunc[T]
	compare            CompareFunc[T]
}

// Generic performs a diff operation on two sorted channels of any comparable type T.
// It compares items from both channels using the provided comparison function and calls
// resultFunc for each item that exists in only one channel (differences).
//
// Parameters:
//   - ctx: Context for cancellation and timeout control
//   - aChan, bChan: Sorted channels to compare (MUST be pre-sorted)
//   - aErrChan, bErrChan: Error channels corresponding to each data channel
//   - compareFunc: Function that returns <0, 0, or >0 for ordering comparison
//   - resultFunc: Callback function called for each difference found
//
// Returns statistical information about the comparison and any errors encountered.
// The function assumes both input channels provide items in sorted order according
// to the comparison function. This assumption is not validated for performance reasons.
func Generic[T any](ctx context.Context, aChan, bChan <-chan T, aErrChan, bErrChan <-chan error, compareFunc CompareFunc[T], resultFunc ResultFunc[T]) (r Result, err error) {
	if ctx == nil || aChan == nil || bChan == nil || aErrChan == nil || bErrChan == nil || resultFunc == nil {
		return Result{}, fmt.Errorf("arguments must not be nil")
	}

	d := differ[T]{
		ctx:        ctx,
		aChan:      aChan,
		aErrChan:   aErrChan,
		bChan:      bChan,
		bErrChan:   bErrChan,
		resultFunc: resultFunc,
		compare:    compareFunc,
	}
	return d.diff()
}

func (d *differ[T]) diff() (r Result, err error) {
	// get first sets of values
	var dataA, dataB T
	var okA, okB bool

	// read from channel A
	select {
	case dataA, okA = <-d.aChan:
	case <-d.ctx.Done():
		return r, d.ctx.Err()
	}
	// read from channel B
	select {
	case dataB, okB = <-d.bChan:
	case <-d.ctx.Done():
		return r, d.ctx.Err()
	}
	for okA && okB {
		c := d.compare(dataA, dataB)
		if c > 0 {
			r.TotalB++
			r.ExtraB++
			err = d.resultFunc(NEW, dataB)
			if err != nil {
				return
			}
			select {
			case dataB, okB = <-d.bChan:
			case <-d.ctx.Done():
				return r, d.ctx.Err()
			}
		} else if c < 0 {
			r.TotalA++
			r.ExtraA++
			err = d.resultFunc(OLD, dataA)
			if err != nil {
				return
			}
			select {
			case dataA, okA = <-d.aChan:
			case <-d.ctx.Done():
				return r, d.ctx.Err()
			}
		} else {
			// common
			r.Common++
			r.TotalA++
			r.TotalB++
			select {
			case dataA, okA = <-d.aChan:
			case <-d.ctx.Done():
				return r, d.ctx.Err()
			}
			select {
			case dataB, okB = <-d.bChan:
			case <-d.ctx.Done():
				return r, d.ctx.Err()
			}
		}
	}
	// check for errors just in case
	if !okA {
		if err = <-d.aErrChan; err != nil {
			return
		}
	}
	if !okB {
		if err = <-d.bErrChan; err != nil {
			return
		}
	}
	// if only A has data left
	for okA {
		r.TotalA++
		r.ExtraA++
		err = d.resultFunc(OLD, dataA)
		if err != nil {
			return
		}
		select {
		case dataA, okA = <-d.aChan:
		case <-d.ctx.Done():
			return r, d.ctx.Err()
		}
	}
	// check for A errors once again
	if err = <-d.aErrChan; err != nil {
		return
	}
	// if only B has data left
	for okB {
		r.TotalB++
		r.ExtraB++
		err = d.resultFunc(NEW, dataB)
		if err != nil {
			return
		}
		select {
		case dataB, okB = <-d.bChan:
		case <-d.ctx.Done():
			return r, d.ctx.Err()
		}
	}
	// check for B errors once again
	if err = <-d.bErrChan; err != nil {
		return
	}
	return
}

// PrintDiff is a utility function that can be used as a ResultFunc to print
// differences to stdout. It formats each difference with the Delta symbol
// (< for OLD, > for NEW) followed by the item value.
func PrintDiff[T any](d Delta, s T) error {
	_, err := fmt.Printf("%s %v\n", d, s)
	return err
}
