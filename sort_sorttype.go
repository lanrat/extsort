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

type chunk struct {
	data []SortType
	less CompareLessFunc
}


// getChunk retrieves a chunk from the pool and initializes it
func (s *SortTypeSorter) getChunk() *chunk {
	c := s.pools.chunkPool.Get().(*chunk)
	
	// Get a slice pointer from the pool
	slicePtr := s.pools.slicePool.Get().(*[]SortType)
	*slicePtr = (*slicePtr)[:0] // Reset length but keep capacity
	
	c.data = *slicePtr
	c.less = s.lessFunc
	return c
}

// putChunk returns a chunk to the pool for reuse
func (s *SortTypeSorter) putChunk(c *chunk) {
	if c != nil && c.data != nil {
		// Return the slice to the pool
		data := c.data
		c.data = nil // Clear reference before putting
		s.pools.slicePool.Put(&data)
		
		// Return the chunk to the pool
		s.pools.chunkPool.Put(c)
	}
}

func (c *chunk) Len() int {
	return len(c.data)
}

func (c *chunk) Swap(i, j int) {
	c.data[i], c.data[j] = c.data[j], c.data[i]
}

func (c *chunk) Less(i, j int) bool {
	return c.less(c.data[i], c.data[j])
}

// memoryPools holds sync.Pool instances for memory reuse
type memoryPools struct {
	chunkPool     sync.Pool // *chunk objects
	slicePool     sync.Pool // []SortType slices
	byteSlicePool sync.Pool // []byte slices for serialization
	scratchPool   sync.Pool // scratch buffers for binary encoding
}

// SortTypeSorter stores an input chan and feeds Sort to return a sorted chan
type SortTypeSorter struct {
	config         Config
	buildSortCtx   context.Context
	saveCtx        context.Context
	mergeErrChan   chan error
	tempWriter     tempfile.TempWriter
	tempReader     tempfile.TempReader
	input          chan SortType
	chunkChan      chan *chunk
	saveChunkChan  chan *chunk
	mergeChunkChan chan SortType
	lessFunc       CompareLessFunc
	fromBytes      FromBytes
	pools          *memoryPools
}

func newSorter(i chan SortType, fromBytes FromBytes, lessFunc CompareLessFunc, config *Config) *SortTypeSorter {
	s := new(SortTypeSorter)
	s.input = i
	s.lessFunc = lessFunc
	s.fromBytes = fromBytes
	s.config = *mergeConfig(config)
	s.chunkChan = make(chan *chunk, s.config.ChanBuffSize)
	s.saveChunkChan = make(chan *chunk, s.config.ChanBuffSize)
	s.mergeChunkChan = make(chan SortType, s.config.SortedChanBuffSize)
	s.mergeErrChan = make(chan error, 1)
	s.pools = s.initMemoryPools()
	return s
}

// initMemoryPools initializes sync.Pool instances for memory reuse
func (s *SortTypeSorter) initMemoryPools() *memoryPools {
	pools := &memoryPools{}
	
	// Pool for chunk objects
	pools.chunkPool = sync.Pool{
		New: func() interface{} {
			return &chunk{
				less: s.lessFunc,
			}
		},
	}
	
	// Pool for SortType slices - store pointers to slices
	pools.slicePool = sync.Pool{
		New: func() interface{} {
			slice := make([]SortType, 0, s.config.ChunkSize)
			return &slice
		},
	}
	
	// Pool for byte slices (for serialization) - store pointers to slices
	pools.byteSlicePool = sync.Pool{
		New: func() interface{} {
			slice := make([]byte, 0, 1024) // Start with 1KB capacity
			return &slice
		},
	}
	
	// Pool for scratch buffers (for binary encoding) - store pointers to slices
	pools.scratchPool = sync.Pool{
		New: func() interface{} {
			slice := make([]byte, binary.MaxVarintLen64)
			return &slice
		},
	}
	
	return pools
}

// New returns a new Sorter instance that can be used to sort the input chan
// fromBytes is needed to unmarshal SortTypes from []byte on disk
// lessfunc is the comparator used for SortType
// config ca be nil to use the defaults, or only set the non-default values desired
// if errors or interrupted, may leave temp files behind in config.TempFilesDir
// the returned channels contain the data returned from calling Sort()
func New(i chan SortType, fromBytes FromBytes, lessFunc CompareLessFunc, config *Config) (*SortTypeSorter, chan SortType, chan error) {
	var err error
	s := newSorter(i, fromBytes, lessFunc, config)
	s.tempWriter, err = tempfile.New(s.config.TempFilesDir)
	if err != nil {
		s.mergeErrChan <- err
		close(s.mergeErrChan)
		close(s.mergeChunkChan)
	}
	return s, s.mergeChunkChan, s.mergeErrChan
}

// NewMock is the same as New() but is backed by memory instead of a temporary file on disk
// n is the size to initialize the backing bytes buffer too
func NewMock(i chan SortType, fromBytes FromBytes, lessFunc CompareLessFunc, config *Config, n int) (*SortTypeSorter, chan SortType, chan error) {
	s := newSorter(i, fromBytes, lessFunc, config)
	s.tempWriter = tempfile.Mock(n)
	return s, s.mergeChunkChan, s.mergeErrChan
}

// Sort sorts the Sorter's input chan and returns a new sorted chan, and error Chan
// Sort is a chunking operation that runs multiple workers asynchronously
// this blocks while sorting chunks and unblocks when merging
// NOTE: the context passed to Sort must outlive Sort() returning.
// Merge uses the same context and runs in a goroutine after Sort returns().
// for example, if calling sort in an errGroup, you must pass the group's parent context into sort.
func (s *SortTypeSorter) Sort(ctx context.Context) {
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
func (s *SortTypeSorter) buildChunks() error {
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
func (s *SortTypeSorter) sortChunks() error {
	for {
		select {
		case b, more := <-s.chunkChan:
			if more {
				// Create a channel to signal when sorting is complete
				sortDone := make(chan struct{})
				
				// Run sort in a separate goroutine
				go func() {
					defer close(sortDone)
					sort.Sort(b)
				}()
				
				// Wait for either sort completion or context cancellation
				select {
				case <-sortDone:
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
func (s *SortTypeSorter) saveChunks() error {
	var err error
	scratchPtr := s.pools.scratchPool.Get().(*[]byte)
	scratch := *scratchPtr
	defer func() {
		s.pools.scratchPool.Put(scratchPtr)
	}()
	
	for {
		select {
		case b, more := <-s.saveChunkChan:
			if more {
				for _, d := range b.data {
					// binary encoding for size
					raw := d.ToBytes()
					n := binary.PutUvarint(scratch, uint64(len(raw)))
					_, err = s.tempWriter.Write(scratch[:n])
					if err != nil {
						s.putChunk(b) // Return chunk to pool on error
						return err
					}
					// add data
					_, err = s.tempWriter.Write(raw)
					if err != nil {
						s.putChunk(b) // Return chunk to pool on error
						return err
					}
				}
				_, err = s.tempWriter.Next()
				if err != nil {
					s.putChunk(b) // Return chunk to pool on error
					return err
				}
				// Successfully processed chunk, return to pool
				s.putChunk(b)
			} else {
				s.tempReader, err = s.tempWriter.Save()
				return err
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
func (s *SortTypeSorter) mergeNChunks(ctx context.Context) {
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
func (s *SortTypeSorter) mergeNChunksSingleThreaded(ctx context.Context) {
	pq := queue.NewPriorityQueue(func(a, b interface{}) bool {
		return s.lessFunc(a.(*mergeFile).nextRec, b.(*mergeFile).nextRec)
	})

	for i := 0; i < s.tempReader.Size(); i++ {
		merge := new(mergeFile)
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
		merge := pq.Peek().(*mergeFile)
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
func (s *SortTypeSorter) mergeNChunksParallel(ctx context.Context) {
	numChunks := s.tempReader.Size()
	numWorkers := s.config.NumMergeWorkers
	
	// Create intermediate channels for each worker
	intermediateChanSize := s.config.SortedChanBuffSize
	intermediateChans := make([]chan SortType, numWorkers)
	for i := range intermediateChans {
		intermediateChans[i] = make(chan SortType, intermediateChanSize)
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
func (s *SortTypeSorter) mergeWorker(ctx context.Context, startChunk, endChunk int, output chan<- SortType, errChan chan<- error) {
	defer close(output)
	
	pq := queue.NewPriorityQueue(func(a, b interface{}) bool {
		return s.lessFunc(a.(*mergeFile).nextRec, b.(*mergeFile).nextRec)
	})

	// Initialize merge files for this worker's chunk range
	for i := startChunk; i < endChunk; i++ {
		merge := new(mergeFile)
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
		merge := pq.Peek().(*mergeFile)
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
func (s *SortTypeSorter) finalMergeStreaming(ctx context.Context, intermediateChans []chan SortType, errChan <-chan error) {
	// Create merge sources for each intermediate channel
	mergeSources := make([]*channelMergeSource, len(intermediateChans))
	pq := queue.NewPriorityQueue(func(a, b interface{}) bool {
		return s.lessFunc(a.(*channelMergeSource).nextRec, b.(*channelMergeSource).nextRec)
	})
	
	// Initialize sources
	for i, ch := range intermediateChans {
		source := &channelMergeSource{ch: ch}
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
		source := pq.Peek().(*channelMergeSource)
		
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
type channelMergeSource struct {
	ch      <-chan SortType
	nextRec SortType
	hasNext bool
}

func (c *channelMergeSource) getNext() bool {
	if rec, ok := <-c.ch; ok {
		c.nextRec = rec
		c.hasNext = true
		return true
	}
	c.hasNext = false
	return false
}

// mergeFile represents each sorted chunk on disk and its next value
type mergeFile struct {
	nextRec   SortType
	fromBytes FromBytes
	reader    *bufio.Reader
}

// getNext returns the next value from the sorted chunk on disk
// the first call will return nil while the struct is initialized
func (m *mergeFile) getNext() (SortType, bool, error) {
	var newRecBytes []byte
	old := m.nextRec

	n, err := binary.ReadUvarint(m.reader)
	if err == nil {
		newRecBytes = make([]byte, int(n))
		_, err = io.ReadFull(m.reader, newRecBytes)
	}
	if err != nil {
		if err == io.EOF {
			m.nextRec = nil
			return old, false, nil
		}
		return nil, false, err
	}

	m.nextRec = m.fromBytes(newRecBytes)
	return old, true, nil
}
