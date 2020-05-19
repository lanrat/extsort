// Package extsort implements an unstable external sort for all the records in a chan or iterator
package extsort

import (
	"context"
	"sort"
)

// StringMockSorter stores an input chan and feeds Sort to return a sorted chan
type StringMockSorter struct {
	ctx            context.Context
	input          chan string
	mergeChunkChan chan string
	mergeErrChan   chan error
}

// StringsMockContext returns a new Sorter instance that can be used to sort the input chan
// this is a mock sorter that exposes the same interface as the other extsort methods, but this is in-memory only
// usefull for testing on smaller workloads.
func StringsMockContext(ctx context.Context, i chan string) *StringMockSorter {
	s := new(StringMockSorter)
	s.input = i
	s.ctx = ctx
	s.mergeChunkChan = make(chan string, 1)
	s.mergeErrChan = make(chan error, 1)
	return s
}

// StringsMock is the same as NewContext without a context
func StringsMock(i chan string) *StringMockSorter {
	return StringsMockContext(context.Background(), i)
}

// Sort sorts the Sorter's input chan and returns a new sorted chan, and error Chan
// Sort is a chunking operation that runs multiple workers asynchronously
func (s *StringMockSorter) Sort() (chan string, chan error) {

	//create single chunk
	chunk, err := s.buildChunk()
	if err != nil {
		s.mergeErrChan <- err
		close(s.mergeErrChan)
		close(s.mergeChunkChan)
		return s.mergeChunkChan, s.mergeErrChan
	}

	// sort chunk
	sort.Sort(chunk)

	// if this errors, it is returned in the errorChan
	go s.mergeChunk(chunk)

	return s.mergeChunkChan, s.mergeErrChan
}

// buildChunks reads data from the input chan to builds chunks and pushes them to chunkChan
func (s *StringMockSorter) buildChunk() (*stringChunk, error) {
	c := newStringChunk(100) // hardcode initial capacity to 100
	for {
		select {
		case rec, ok := <-s.input:
			if !ok {
				return c, nil
			}
			c.data = append(c.data, rec)
		case <-s.ctx.Done():
			return nil, s.ctx.Err()
		}
	}
}

// mergeChunk runs asynchronously in the background feeding data to the output chanel
// sends errors to s.mergeErrorChan
func (s *StringMockSorter) mergeChunk(chunk *stringChunk) {
	defer close(s.mergeChunkChan)
	defer close(s.mergeErrChan)

	for _, v := range chunk.data {
		//log.Printf("sort.MergeChunk send %s", v)
		s.mergeChunkChan <- v
	}
}
