package diff

import (
	"context"
	"fmt"
)

// ctxCheckInterval is how many values recv reads between checks of the context
// while its non-blocking receive keeps succeeding.
const ctxCheckInterval = 1024

// differ is an internal struct that holds the state for performing diff operations
// between two sorted channels of type T. It manages the comparison logic and
// result reporting through callback functions.
type differ[T any] struct {
	ctx                context.Context
	aChan, bChan       <-chan T
	aErrChan, bErrChan <-chan error
	resultFunc         ResultFunc[T]
	compare            CompareFunc[T]
	reads              int // values read by recv
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
	if dataA, okA, err = d.recv(d.aChan); err != nil {
		return
	}
	// read from channel B
	if dataB, okB, err = d.recv(d.bChan); err != nil {
		return
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
			if dataB, okB, err = d.recv(d.bChan); err != nil {
				return
			}
		} else if c < 0 {
			r.TotalA++
			r.ExtraA++
			err = d.resultFunc(OLD, dataA)
			if err != nil {
				return
			}
			if dataA, okA, err = d.recv(d.aChan); err != nil {
				return
			}
		} else {
			// common
			r.Common++
			r.TotalA++
			r.TotalB++
			if dataA, okA, err = d.recv(d.aChan); err != nil {
				return
			}
			if dataB, okB, err = d.recv(d.bChan); err != nil {
				return
			}
		}
	}
	// check for errors just in case. Each error channel is read once: here if its
	// stream has ended, otherwise after the stream is drained below.
	aErrPending, bErrPending := okA, okB
	if !okA {
		if err = d.readErr(d.aErrChan); err != nil {
			return
		}
	}
	if !okB {
		if err = d.readErr(d.bErrChan); err != nil {
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
		if dataA, okA, err = d.recv(d.aChan); err != nil {
			return
		}
	}
	// check for A errors if not read above
	if aErrPending {
		if err = d.readErr(d.aErrChan); err != nil {
			return
		}
	}
	// if only B has data left
	for okB {
		r.TotalB++
		r.ExtraB++
		err = d.resultFunc(NEW, dataB)
		if err != nil {
			return
		}
		if dataB, okB, err = d.recv(d.bChan); err != nil {
			return
		}
	}
	// check for B errors if not read above
	if bErrPending {
		if err = d.readErr(d.bErrChan); err != nil {
			return
		}
	}
	return
}

// recv reads the next value from ch. It tries a non-blocking receive first: unlike a
// select with ctx.Done(), that does not lock the context's channel, so a stream with a
// value ready costs one channel operation. ctx is then checked every ctxCheckInterval
// values, starting with the first, and whenever ch has nothing ready.
func (d *differ[T]) recv(ch <-chan T) (v T, ok bool, err error) {
	if d.reads%ctxCheckInterval == 0 {
		if err := d.ctx.Err(); err != nil {
			return v, false, err
		}
	}
	d.reads++
	select {
	case v, ok = <-ch:
		return v, ok, nil
	default:
	}
	select {
	case v, ok = <-ch:
		return v, ok, nil
	case <-d.ctx.Done():
		return v, false, d.ctx.Err()
	}
}

// readErr waits for the error from a stream whose data channel has closed.
// It gives up when ctx is done, so an error channel that is never closed cannot hang the diff.
func (d *differ[T]) readErr(errChan <-chan error) error {
	select {
	case err := <-errChan:
		return err
	case <-d.ctx.Done():
		return d.ctx.Err()
	}
}

// PrintDiff is a utility function that can be used as a ResultFunc to print
// differences to stdout. It formats each difference with the Delta symbol
// (< for OLD, > for NEW) followed by the item value.
func PrintDiff[T any](d Delta, s T) error {
	_, err := fmt.Printf("%s %v\n", d, s)
	return err
}
