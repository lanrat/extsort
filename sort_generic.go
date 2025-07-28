// Package extsort implements an unstable external sort for all the records in a chan or iterator
package extsort

import (
	"bufio"
	"context"
	"encoding/binary"
	"io"
	"sort"
	"sync"

	"github.com/lanrat/extsort/queue"
	"github.com/lanrat/extsort/tempfile"

	"golang.org/x/sync/errgroup"
)

// genericChunk represents a collection of any data that can be sorted.
// It implements sort.Interface for efficient sorting operations.
type genericChunk[E any] struct {
	data []E
	less CompareLessFuncGeneric[E]
}

// getChunk retrieves a chunk from the pool and initializes it
func (s *GenericSorter[E]) getChunk() *genericChunk[E] {
	c := s.pools.chunkPool.Get().(*genericChunk[E])

	// Get a slice pointer from the pool
	slicePtr := s.pools.slicePool.Get().(*[]E)
	*slicePtr = (*slicePtr)[:0] // Reset length but keep capacity

	c.data = *slicePtr
	c.less = s.lessFunc
	return c
}

// putChunk returns a chunk to the pool for reuse
func (s *GenericSorter[E]) putChunk(c *genericChunk[E]) {
	if c != nil && c.data != nil {
		// Return the slice to the pool
		data := c.data
		c.data = nil // Clear reference before putting
		s.pools.slicePool.Put(&data)

		// Return the chunk to the pool
		s.pools.chunkPool.Put(c)
	}
}

// Len returns the number of elements in the chunk (implements sort.Interface).
func (c *genericChunk[E]) Len() int {
	return len(c.data)
}

// Swap swaps the elements with indexes i and j (implements sort.Interface).
func (c *genericChunk[E]) Swap(i, j int) {
	c.data[i], c.data[j] = c.data[j], c.data[i]
}

// Less reports whether the element with index i should sort before the element with index j (implements sort.Interface).
func (c *genericChunk[E]) Less(i, j int) bool {
	return c.less(c.data[i], c.data[j])
}

// memoryPools holds sync.Pool instances for memory reuse
type memoryPools struct {
	chunkPool     sync.Pool // *chunk objects
	slicePool     sync.Pool // []any slices
	byteSlicePool sync.Pool // []byte slices for serialization
	scratchPool   sync.Pool // scratch buffers for binary encoding
}

// GenericSorter stores an input chan and feeds Sort to return a sorted chan
type GenericSorter[E any] struct {
	config         Config
	buildSortCtx   context.Context
	saveCtx        context.Context
	mergeErrChan   chan error
	tempWriter     tempfile.TempWriter
	tempReader     tempfile.TempReader
	input          chan E
	chunkChan      chan *genericChunk[E]
	saveChunkChan  chan *genericChunk[E]
	mergeChunkChan chan E
	lessFunc       CompareLessFuncGeneric[E]
	fromBytes      FromBytesGeneric[E]
	toBytes        ToBytesGeneric[E]
	pools          *memoryPools
}

// newSorter creates a new Sorter instance with the given configuration.
// This is the internal constructor used by New() and NewMock().
func newSorter[E any](input chan E, fromBytes FromBytesGeneric[E], toBytes ToBytesGeneric[E], lessFunc CompareLessFuncGeneric[E], config *Config) *GenericSorter[E] {
	s := new(GenericSorter[E])
	s.input = input
	s.lessFunc = lessFunc
	s.fromBytes = fromBytes
	s.toBytes = toBytes
	s.config = *mergeConfig(config)
	s.chunkChan = make(chan *genericChunk[E], s.config.ChanBuffSize)
	s.saveChunkChan = make(chan *genericChunk[E], s.config.ChanBuffSize)
	s.mergeChunkChan = make(chan E, s.config.SortedChanBuffSize)
	s.mergeErrChan = make(chan error, 1)
	s.pools = s.initMemoryPools()
	return s
}

// initMemoryPools initializes sync.Pool instances for memory reuse
func (s *GenericSorter[E]) initMemoryPools() *memoryPools {
	pools := &memoryPools{}

	// Pool for chunk objects
	pools.chunkPool = sync.Pool{
		New: func() any {
			return &genericChunk[E]{
				less: s.lessFunc,
			}
		},
	}

	// Pool for slices - store pointers to slices
	pools.slicePool = sync.Pool{
		New: func() any {
			slice := make([]E, 0, s.config.ChunkSize)
			return &slice
		},
	}

	// Pool for byte slices (for serialization) - store pointers to slices
	pools.byteSlicePool = sync.Pool{
		New: func() any {
			slice := make([]byte, 0, 1024) // Start with 1KB capacity
			return &slice
		},
	}

	// Pool for scratch buffers (for binary encoding) - store pointers to slices
	pools.scratchPool = sync.Pool{
		New: func() any {
			slice := make([]byte, binary.MaxVarintLen64)
			return &slice
		},
	}

	return pools
}

// Generic returns a new Sorter instance that can be used to sort the input chan
// fromBytes is needed to unmarshal E from []byte on disk
// lessfunc is the comparator used for E
// config can be nil to use the defaults, or only set the non-default values desired
// if errors or interrupted, may leave temp files behind in config.TempFilesDir
// the returned channels contain the data returned from calling Sort()
func Generic[E any](input chan E, fromBytes FromBytesGeneric[E], toBytes ToBytesGeneric[E], lessFunc CompareLessFuncGeneric[E], config *Config) (*GenericSorter[E], chan E, chan error) {
	var err error
	s := newSorter(input, fromBytes, toBytes, lessFunc, config)
	s.tempWriter, err = tempfile.New(s.config.TempFilesDir)
	if err != nil {
		s.mergeErrChan <- err
		close(s.mergeErrChan)
		close(s.mergeChunkChan)
	}
	return s, s.mergeChunkChan, s.mergeErrChan
}

// MockGeneric is the same as Generic() but is backed by memory instead of a temporary file on disk.
// n is the size to initialize the backing bytes buffer to.
func MockGeneric[E any](input chan E, fromBytes FromBytesGeneric[E], toBytes ToBytesGeneric[E], lessFunc CompareLessFuncGeneric[E], config *Config, n int) (*GenericSorter[E], chan E, chan error) {
	s := newSorter(input, fromBytes, toBytes, lessFunc, config)
	s.tempWriter = tempfile.Mock(n)
	return s, s.mergeChunkChan, s.mergeErrChan
}

// Sort sorts the Sorter's input chan and returns a new sorted chan, and error Chan
// Sort is a chunking operation that runs multiple workers asynchronously
// this blocks while sorting chunks and unblocks when merging
// NOTE: the context passed to Sort must outlive Sort() returning.
// Merge uses the same context and runs in a goroutine after Sort returns().
// for example, if calling sort in an errGroup, you must pass the group's parent context into sort.
func (s *GenericSorter[E]) Sort(ctx context.Context) {
	var buildSortErrGroup, saveErrGroup *errgroup.Group
	buildSortErrGroup, s.buildSortCtx = errgroup.WithContext(ctx)
	saveErrGroup, s.saveCtx = errgroup.WithContext(ctx)

	//start creating chunks
	buildSortErrGroup.Go(s.buildChunks)

	// sort chunks
	for i := 0; i < s.config.NumWorkers; i++ {
		buildSortErrGroup.Go(s.sortChunks)
	}

	// save chunks
	saveErrGroup.Go(s.saveChunks)

	err := buildSortErrGroup.Wait()
	if err != nil {
		s.mergeErrChan <- err
		close(s.mergeErrChan)
		close(s.mergeChunkChan)
		return
	}

	// need to close saveChunkChan
	close(s.saveChunkChan)
	err = saveErrGroup.Wait()
	if err != nil {
		s.mergeErrChan <- err
		close(s.mergeErrChan)
		close(s.mergeChunkChan)
		return
	}

	// read chunks and merge
	// if this errors, it is returned in the errorChan
	go s.mergeNChunks(ctx)
}

// buildChunks reads data from the input chan to builds chunks and pushes them to chunkChan
func (s *GenericSorter[E]) buildChunks() error {
	defer close(s.chunkChan) // if this is not called on error, causes a deadlock

	for {
		c := s.getChunk()
		for i := 0; i < s.config.ChunkSize; i++ {
			select {
			case rec, ok := <-s.input:
				if !ok {
					break
				}
				c.data = append(c.data, rec)
			case <-s.buildSortCtx.Done():
				s.putChunk(c) // Return unused chunk to pool
				return s.buildSortCtx.Err()
			}
		}
		if len(c.data) == 0 {
			// the chunk is empty, return it to pool
			s.putChunk(c)
			break
		}

		// chunk is now full
		s.chunkChan <- c
	}

	return nil
}

// sortChunks is a worker for sorting the data stored in a chunk prior to save
func (s *GenericSorter[E]) sortChunks() error {
	for {
		select {
		case b, more := <-s.chunkChan:
			if more {
				// Create channels to communicate completion and errors
				sortDone := make(chan error, 1)

				// Run sort in a separate goroutine
				go func() {
					defer func() {
						// Recover from panics in comparison function
						if r := recover(); r != nil {
							sortDone <- NewComparisonError(r, "sortChunks")
						} else {
							sortDone <- nil // Success
						}
					}()
					sort.Sort(b) // TODO migrate to slices sortFunc?
				}()

				// Wait for either sort completion or context cancellation
				select {
				case sortErr := <-sortDone:
					if sortErr != nil {
						// Sort failed due to panic
						s.putChunk(b) // Return chunk to pool
						return sortErr
					}
					// Sort completed successfully, proceed to save
					select {
					case s.saveChunkChan <- b:
					case <-s.buildSortCtx.Done():
						return s.buildSortCtx.Err()
					}
				case <-s.buildSortCtx.Done():
					// Context cancelled while sorting - abandon this chunk
					return s.buildSortCtx.Err()
				}
			} else {
				return nil
			}
		case <-s.buildSortCtx.Done():
			return s.buildSortCtx.Err()
		}
	}
}

// saveChunks is a worker for saving sorted data to disk
func (s *GenericSorter[E]) saveChunks() (err error) {
	scratchPtr := s.pools.scratchPool.Get().(*[]byte)
	scratch := *scratchPtr
	defer func() {
		s.pools.scratchPool.Put(scratchPtr)
		// Recover from panics in ToBytes() calls
		if r := recover(); r != nil {
			err = NewSerializationError(r, "saveChunks")
		}
	}()

	for {
		select {
		case b, more := <-s.saveChunkChan:
			if more {
				for _, d := range b.data {
					// binary encoding for size
					raw := s.toBytes(d)
					n := binary.PutUvarint(scratch, uint64(len(raw)))
					_, err = s.tempWriter.Write(scratch[:n])
					if err != nil {
						s.putChunk(b) // Return chunk to pool on error
						return NewDiskError(err, "write size header", "")
					}
					// add data
					_, err = s.tempWriter.Write(raw)
					if err != nil {
						s.putChunk(b) // Return chunk to pool on error
						return NewDiskError(err, "write data", "")
					}
				}
				_, err = s.tempWriter.Next()
				if err != nil {
					s.putChunk(b) // Return chunk to pool on error
					return NewDiskError(err, "next chunk", "")
				}
				// Successfully processed chunk, return to pool
				s.putChunk(b)
			} else {
				s.tempReader, err = s.tempWriter.Save()
				if err != nil {
					return NewDiskError(err, "save temp file", "")
				}
				return nil
			}
		case <-s.saveCtx.Done():
			// delete the temp file from disk
			_ = s.tempWriter.Close() // ignore error on cleanup
			return s.saveCtx.Err()
		}
	}
}

// mergeNChunks runs asynchronously in the background feeding data to getNext
// sends errors to s.mergeErrorChan. Uses parallel merging for better performance.
func (s *GenericSorter[E]) mergeNChunks(ctx context.Context) {
	defer close(s.mergeChunkChan)
	defer func() {
		err := s.tempReader.Close()
		if err != nil {
			s.mergeErrChan <- err
		}
	}()
	defer close(s.mergeErrChan)

	numChunks := s.tempReader.Size()
	if numChunks == 0 {
		return
	}

	// For small number of chunks, use single-threaded merge
	if numChunks <= s.config.NumMergeWorkers {
		s.mergeNChunksSingleThreaded(ctx)
		return
	}

	// Use parallel merging for many chunks
	s.mergeNChunksParallel(ctx)
}

// mergeNChunksSingleThreaded is the original single-threaded implementation
func (s *GenericSorter[E]) mergeNChunksSingleThreaded(ctx context.Context) {
	pq := queue.NewPriorityQueue(func(a, b *mergeFile[E]) bool {
		return s.lessFunc(a.nextRec, b.nextRec)
	})

	for i := 0; i < s.tempReader.Size(); i++ {
		merge := new(mergeFile[E])
		merge.fromBytes = s.fromBytes
		merge.reader = s.tempReader.Read(i)
		_, ok, err := merge.getNext() // start the merge by preloading the values
		if err == io.EOF || !ok {
			continue
		}
		if err != nil {
			s.mergeErrChan <- err
			return
		}
		pq.Push(merge)
	}

	for pq.Len() > 0 {
		merge := pq.Peek()
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
		// check for err in context just in case
		select {
		case s.mergeChunkChan <- rec:
		case <-ctx.Done():
			s.mergeErrChan <- ctx.Err()
			return
		}
	}
}

// mergeNChunksParallel implements parallel k-way merging
func (s *GenericSorter[E]) mergeNChunksParallel(ctx context.Context) {
	numChunks := s.tempReader.Size()
	numWorkers := s.config.NumMergeWorkers

	// Create intermediate channels for each worker
	intermediateChanSize := s.config.SortedChanBuffSize
	intermediateChans := make([]chan E, numWorkers)
	for i := range intermediateChans {
		intermediateChans[i] = make(chan E, intermediateChanSize)
	}

	errChan := make(chan error, numWorkers)

	// Divide chunks among workers and start them
	chunksPerWorker := (numChunks + numWorkers - 1) / numWorkers
	workersStarted := 0

	for i := 0; i < numWorkers; i++ {
		startChunk := i * chunksPerWorker
		endChunk := (i + 1) * chunksPerWorker
		if endChunk > numChunks {
			endChunk = numChunks
		}
		if startChunk >= numChunks {
			break
		}
		workersStarted++

		go s.mergeWorker(ctx, startChunk, endChunk, intermediateChans[i], errChan)
	}

	// Final merge of intermediate results using priority queue
	s.finalMergeStreaming(ctx, intermediateChans[:workersStarted], errChan)
}

// mergeWorker merges a subset of chunks and sends results to output channel
func (s *GenericSorter[E]) mergeWorker(ctx context.Context, startChunk, endChunk int, output chan<- E, errChan chan<- error) {
	defer close(output)

	pq := queue.NewPriorityQueue(func(a, b *mergeFile[E]) bool {
		return s.lessFunc(a.nextRec, b.nextRec)
	})

	// Initialize merge files for this worker's chunk range
	for i := startChunk; i < endChunk; i++ {
		merge := new(mergeFile[E])
		merge.fromBytes = s.fromBytes
		merge.reader = s.tempReader.Read(i)
		_, ok, err := merge.getNext()
		if err == io.EOF || !ok {
			continue
		}
		if err != nil {
			errChan <- err
			return
		}
		pq.Push(merge)
	}

	// Merge this worker's chunks
	for pq.Len() > 0 {
		merge := pq.Peek()
		rec, more, err := merge.getNext()
		if err != nil {
			errChan <- err
			return
		}
		if more {
			pq.PeekUpdate()
		} else {
			pq.Pop()
		}

		select {
		case output <- rec:
		case <-ctx.Done():
			errChan <- ctx.Err()
			return
		}
	}

	errChan <- nil // Signal successful completion
}

// finalMergeStreaming performs streaming merge of intermediate results
func (s *GenericSorter[E]) finalMergeStreaming(ctx context.Context, intermediateChans []chan E, errChan <-chan error) {
	// Create merge sources for each intermediate channel
	mergeSources := make([]*channelMergeSource[E], len(intermediateChans))
	pq := queue.NewPriorityQueue(func(a, b *channelMergeSource[E]) bool {
		return s.lessFunc(a.nextRec, b.nextRec)
	})

	// Initialize sources
	for i, ch := range intermediateChans {
		source := &channelMergeSource[E]{ch: ch}
		if source.getNext() {
			mergeSources[i] = source
			pq.Push(source)
		}
	}

	// Wait for workers to complete and collect any errors
	go func() {
		errCount := 0
		for errCount < len(intermediateChans) {
			select {
			case err := <-errChan:
				errCount++
				if err != nil {
					s.mergeErrChan <- err
					return
				}
			case <-ctx.Done():
				s.mergeErrChan <- ctx.Err()
				return
			}
		}
	}()

	// Perform final streaming merge
	for pq.Len() > 0 {
		source := pq.Peek()

		select {
		case s.mergeChunkChan <- source.nextRec:
		case <-ctx.Done():
			s.mergeErrChan <- ctx.Err()
			return
		}

		if source.getNext() {
			pq.PeekUpdate()
		} else {
			pq.Pop()
		}
	}
}

// channelMergeSource represents a source of sorted data from a channel
type channelMergeSource[E any] struct {
	ch      <-chan E
	nextRec E
	hasNext bool
}

func (c *channelMergeSource[E]) getNext() bool {
	if rec, ok := <-c.ch; ok {
		c.nextRec = rec
		c.hasNext = true
		return true
	}
	c.hasNext = false
	return false
}

// mergeFile represents each sorted chunk on disk and its next value
type mergeFile[E any] struct {
	nextRec   E
	fromBytes FromBytesGeneric[E]
	reader    *bufio.Reader
}

// getNext returns the next value from the sorted chunk on disk
// the first call will return nil while the struct is initialized
func (m *mergeFile[E]) getNext() (E, bool, error) {
	var newRecBytes []byte
	old := m.nextRec

	n, err := binary.ReadUvarint(m.reader)
	if err == nil {
		newRecBytes = make([]byte, int(n))
		_, err = io.ReadFull(m.reader, newRecBytes)
	}
	if err != nil {
		if err == io.EOF {
			return old, false, nil
		}
		return old, false, err
	}

	// Recover from panics in fromBytes() calls
	defer func() {
		if r := recover(); r != nil {
			err = NewDeserializationError(r, len(newRecBytes), "getNext")
		}
	}()

	m.nextRec = m.fromBytes(newRecBytes)
	return old, true, err
}
