// Package diff performs diffs on sorted channels of data
package diff

import (
	"context"
	"fmt"
)

type stringDiffer struct {
	ctx                context.Context
	aChan, bChan       chan string
	aErrChan, bErrChan chan error
	resultFunc         StringResultFunc
}

// Strings takes 4 chan inputs, 2 for strings, and 2 for their corresponding error channels, and a StringResultFunc to be called for every new/old record found
// when done, counter results and errors (if any) are returned
// string chan input MUST be sorted, for performance reasons this is not checked!
func Strings(ctx context.Context, aChan, bChan chan string, aErrChan chan error, bErrChan chan error, resultFunc StringResultFunc) (r Result, err error) {
	var d stringDiffer
	d.ctx = ctx
	d.aChan = aChan
	d.aErrChan = aErrChan
	d.bChan = bChan
	d.bErrChan = bErrChan
	d.resultFunc = resultFunc

	if ctx == nil || aChan == nil || bChan == nil || aErrChan == nil || bErrChan == nil || resultFunc == nil {
		return Result{}, fmt.Errorf("diff.Strings() arguments must not be nil")
	}

	return d.diff()
}

func (d *stringDiffer) diff() (r Result, err error) {
	// get first sets of values
	var dataA, dataB string
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
		if dataB < dataA {
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
		} else if dataA < dataB {
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

	return r, nil
}

// PrintStringDiff satisfies StringResultFunc can can be used as
// resultFunc in diff.Strings()
func PrintStringDiff(d Delta, s string) error {
	_, err := fmt.Printf("%s %s\n", d, s)
	return err
}
