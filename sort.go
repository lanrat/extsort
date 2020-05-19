// Package extsort implements an unstable external sort for all the records in a chan or iterator
package extsort

import (
	"bufio"
	"context"
	"encoding/binary"
	"io"
	"io/ioutil"
	"os"
	"sort"
	"sync"

	"github.com/lanrat/extsort/queue"

	"golang.org/x/sync/errgroup"
)

type chunk struct {
	data []SortType
	less CompareLessFunc
}

func newChunk(size int, lessFunc CompareLessFunc) *chunk {
	c := new(chunk)
	c.less = lessFunc
	c.data = make([]SortType, 0, size)
	return c
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

// Sorter stores an input chan and feeds Sort to return a sorted chan
type Sorter struct {
	config             Config
	ctx                context.Context
	input              chan SortType
	chunkChan          chan *chunk
	mergeFileList      []string
	mergeFileListMutex sync.Mutex
	lessFunc           CompareLessFunc
	mergeChunkChan     chan SortType
	mergeErrChan       chan error
	fromBytes          FromBytes
}

// NewContext returns a new Sorter instance that can be used to sort the input chan
// fromBytes is needed to unmarshal SortTypes from []byte on disk
// lessfunc is the comparator used for SortType
// config ca be nil to use the defaults, or only set the non-default values desired
// if errors or interupted, may leave temp files behind in config.TempFilesDir
func NewContext(ctx context.Context, i chan SortType, fromBytes FromBytes, lessFunc CompareLessFunc, config *Config) *Sorter {
	s := new(Sorter)
	s.input = i
	s.ctx = ctx
	s.lessFunc = lessFunc
	s.fromBytes = fromBytes
	s.config = *mergeConfig(config)
	s.chunkChan = make(chan *chunk, s.config.ChanBuffSize)
	s.mergeFileList = make([]string, 0, 1)
	s.mergeChunkChan = make(chan SortType, s.config.SortedChanBuffSize)
	s.mergeErrChan = make(chan error, 1)
	return s
}

// New is the same as NewContext without a context
func New(i chan SortType, fromBytes FromBytes, lessFunc CompareLessFunc, config *Config) *Sorter {
	return NewContext(context.Background(), i, fromBytes, lessFunc, config)
}

// Sort sorts the Sorter's input chan and returns a new sorted chan, and error Chan
// Sort is a chunking operation that runs multiple workers asynchronously
func (s *Sorter) Sort() (chan SortType, chan error) {
	var errGroup *errgroup.Group
	errGroup, s.ctx = errgroup.WithContext(s.ctx)

	//start saving chunks
	errGroup.Go(s.buildChunks)

	for i := 0; i < s.config.NumWorkers; i++ {
		errGroup.Go(s.sortChunksToDisk)
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

// buildChunks reads data from the input chan to builds chunks and pushes them to chunkChan
func (s *Sorter) buildChunks() error {
	var err error
	defer close(s.chunkChan) // if this is not called on error, causes a deadlock

	for err != io.EOF {
		c := newChunk(s.config.ChunkSize, s.lessFunc)
		for i := 0; i < s.config.ChunkSize; i++ {
			select {
			case rec, ok := <-s.input:
				if !ok {
					break
				}
				c.data = append(c.data, rec)
			case <-s.ctx.Done():
				return s.ctx.Err()
			}
		}
		if len(c.data) == 0 {
			// the chunk is empty
			break
		}

		// chunk is now full
		s.chunkChan <- c
	}

	return nil
}

// sortChunks is a worker for sorting the data stored in a chunk prior to save
func (s *Sorter) sortChunksToDisk() error {
	scratch := make([]byte, binary.MaxVarintLen64)
	for {
		select {
		case b, more := <-s.chunkChan:
			if more {
				// sort
				sort.Sort(b)
				// save
				// create temp file
				f, err := ioutil.TempFile(s.config.TempFilesDir, mergeFilenamePrefix)
				if err != nil {
					return err
				}
				fName := f.Name()

				bufWriter := bufio.NewWriterSize(f, fileSortBufferSize)
				for _, d := range b.data {
					// binary encoding
					raw := d.ToBytes()
					n := binary.PutUvarint(scratch, uint64(len(raw)))
					_, err = bufWriter.Write(scratch[:n])
					if err != nil {
						return err
					}
					_, err = bufWriter.Write(d.ToBytes())
					if err != nil {
						return err
					}
				}
				err = bufWriter.Flush()
				if err != nil {
					return err
				}
				err = f.Close()
				if err != nil {
					return err
				}

				s.mergeFileListMutex.Lock()
				s.mergeFileList = append(s.mergeFileList, fName)
				s.mergeFileListMutex.Unlock()
			} else {
				return nil
			}
		case <-s.ctx.Done():
			return s.ctx.Err()
		}
	}
}

// mergefile represents each sorted chunk on disk and its next value
type mergeFile struct {
	nextRec   SortType
	file      *os.File
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
			if m.file != nil {
				err = m.file.Close()
				if err != nil {
					return nil, false, err
				}
				err = os.Remove(m.file.Name())
				if err != nil {
					return nil, false, err
				}
				m.file = nil
			}
			return old, false, nil
		}
		return nil, false, err
	}

	m.nextRec = m.fromBytes(newRecBytes)
	return old, true, nil
}

// mergeNChunks runs asynchronously in the background feeding data to getNext
// sends errors to s.mergeErrorChan
func (s *Sorter) mergeNChunks() {
	//populate queue with data from mergeFile list
	var err error
	defer close(s.mergeChunkChan)
	defer close(s.mergeErrChan)
	pq := queue.NewPriorityQueue(func(a, b interface{}) bool {
		return s.lessFunc(a.(*mergeFile).nextRec, b.(*mergeFile).nextRec)
	})
	for _, filename := range s.mergeFileList {
		// check the the file exists
		if _, err = os.Stat(filename); err != nil {
			s.mergeErrChan <- err
			return
		}

		merge := new(mergeFile)
		merge.fromBytes = s.fromBytes
		merge.file, err = os.Open(filename)
		if err != nil {
			s.mergeErrChan <- err
			return
		}
		merge.reader = bufio.NewReaderSize(merge.file, fileSortBufferSize)
		_, _, err := merge.getNext() // start the merge by preloading the values
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
		s.mergeChunkChan <- rec
	}
}
