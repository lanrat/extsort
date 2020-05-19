// Package extsort implements an unstable external sort for all the records in a chan or iterator
package extsort

import (
	"context"
	"sort"

	"github.com/lanrat/extsort/queue"
	"golang.org/x/sync/errgroup"
)

// StringMockSorter stores an input chan and feeds Sort to return a sorted chan
type StringMockSorter struct {
	config          Config
	ctx             context.Context
	input           chan string
	chunkNumberChan chan int
	chunks          []*stringChunk
	mergeChunkChan  chan string
	mergeErrChan    chan error
}

// StringsMockContext returns a new Sorter instance that can be used to sort the input chan
// this is a mock sorter that exposes the same interface as the other extsort methods, but this is in-memory only
// usefull for testing on smaller workloads.
func StringsMockContext(ctx context.Context, i chan string, config *Config) *StringMockSorter {
	s := new(StringMockSorter)
	s.input = i
	s.ctx = ctx
	s.config = *mergeConfig(config)
	s.chunkNumberChan = make(chan int, s.config.ChanBuffSize)
	s.chunks = make([]*stringChunk, 0)
	s.mergeChunkChan = make(chan string, 1)
	s.mergeErrChan = make(chan error, 1)
	return s
}

// StringsMock is the same as NewContext without a context
func StringsMock(i chan string, config *Config) *StringMockSorter {
	return StringsMockContext(context.Background(), i, config)
}

// Sort sorts the Sorter's input chan and returns a new sorted chan, and error Chan
// Sort is a chunking operation that runs multiple workers asynchronously
func (s *StringMockSorter) Sort() (chan string, chan error) {
	var errGroup *errgroup.Group
	errGroup, s.ctx = errgroup.WithContext(s.ctx)

	//start saving chunks
	errGroup.Go(s.buildChunks)

	for i := 0; i < s.config.NumWorkers; i++ {
		errGroup.Go(s.sortChunksInMemory)
	}

	err := errGroup.Wait()
	if err != nil {
		s.mergeErrChan <- err
		close(s.mergeErrChan)
		close(s.mergeChunkChan)
		return s.mergeChunkChan, s.mergeErrChan
	}

	// if this errors, it is returned in the errorChan
	go s.mergeNChunks()

	return s.mergeChunkChan, s.mergeErrChan
}

// sortChunks is a worker for sorting the data stored in a chunk prior to save
func (s *StringMockSorter) sortChunksInMemory() error {
	for {
		select {
		case n, more := <-s.chunkNumberChan:
			if more {
				// sort
				sort.Sort(s.chunks[n])
			} else {
				return nil
			}
		case <-s.ctx.Done():
			return s.ctx.Err()
		}
	}
}

// buildChunks reads data from the input chan to builds chunks and pushes them to chunkChan
// identical to StringSorter.buildChunks()
func (s *StringMockSorter) buildChunks() error {
	defer close(s.chunkNumberChan) // if this is not called on error, causes a deadlock

	chunkNum := 0
	for {
		c := newStringChunk(s.config.ChunkSize)
		err := func() error {
			for i := 0; i < s.config.ChunkSize; i++ {
				select {
				case rec, ok := <-s.input:
					if !ok {
						return nil
					}
					c.data = append(c.data, rec)
				case <-s.ctx.Done():
					return s.ctx.Err()
				}
			}
			return nil
		}()
		if err != nil {
			return err
		}
		if len(c.data) == 0 {
			// the chunk is empty
			break
		}

		// chunk is now full
		s.chunks = append(s.chunks, c)
		s.chunkNumberChan <- chunkNum
		chunkNum++
	}

	return nil
}

// mergeNChunks runs asynchronously in the background feeding data to getNext
// sends errors to s.mergeErrorChan
func (s *StringMockSorter) mergeNChunks() {
	//populate queue with data from mergeFile list
	defer close(s.mergeChunkChan)
	defer close(s.mergeErrChan)
	pq := queue.NewPriorityQueue(func(a, b interface{}) bool {
		am := a.(*mergeStringMemory)
		bm := b.(*mergeStringMemory)
		return am.chunk.data[am.offset] < bm.chunk.data[bm.offset]
	})
	for n := range s.chunks {
		merge := new(mergeStringMemory)
		merge.chunk = s.chunks[n]
		pq.Push(merge)
	}

	for pq.Len() > 0 {
		merge := pq.Peek().(*mergeStringMemory)
		rec, more, err := merge.getNext()
		if err != nil {
			s.mergeErrChan <- err
			return
		}
		if more {
			pq.PeekUpdate()
		} else {
			pq.Pop()
		}
		s.mergeChunkChan <- rec
	}
}

// mergefile represents each sorted chunk on disk and its next value
type mergeStringMemory struct {
	chunk  *stringChunk
	offset int
}

// getNext returns the next value from the sorted chunk on disk
// the first call will return nil while the struct is initialized
func (m *mergeStringMemory) getNext() (string, bool, error) {
	if m.offset >= len(m.chunk.data) {
		return "", false, nil
	}
	out := m.chunk.data[m.offset]
	m.offset++
	return out, m.offset < len(m.chunk.data), nil
}
