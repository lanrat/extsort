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

type stringChunk struct {
	data []string
}

func newStringChunk(size int) *stringChunk {
	c := new(stringChunk)
	c.data = make([]string, 0, size)
	return c
}

func (c *stringChunk) Len() int {
	return len(c.data)
}

func (c *stringChunk) Swap(i, j int) {
	c.data[i], c.data[j] = c.data[j], c.data[i]
}

func (c *stringChunk) Less(i, j int) bool {
	return c.data[i] < c.data[j]
}

// StringSorter stores an input chan and feeds Sort to return a sorted chan
type StringSorter struct {
	config             Config
	ctx                context.Context
	input              chan string
	chunkChan          chan *stringChunk
	mergeFileList      []string
	mergeFileListMutex sync.Mutex
	mergeChunkChan     chan string
	mergeErrChan       chan error
}

// StringsContext returns a new Sorter instance that can be used to sort the input chan
func StringsContext(ctx context.Context, i chan string, config *Config) *StringSorter {
	s := new(StringSorter)
	s.input = i
	s.ctx = ctx
	s.config = *mergeConfig(config)
	s.chunkChan = make(chan *stringChunk, s.config.ChanBuffSize)
	s.mergeFileList = make([]string, 0, 1)
	s.mergeChunkChan = make(chan string, s.config.SortedChanBuffSize)
	s.mergeErrChan = make(chan error, 1)
	return s
}

// Strings is the same as NewContext without a context
func Strings(i chan string, config *Config) *StringSorter {
	return StringsContext(context.Background(), i, config)
}

// Sort sorts the Sorter's input chan and returns a new sorted chan, and error Chan
// Sort is a chunking operation that runs multiple workers asynchronously
func (s *StringSorter) Sort() (chan string, chan error) {
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
func (s *StringSorter) buildChunks() error {
	defer close(s.chunkChan) // if this is not called on error, causes a deadlock

	for {
		c := newStringChunk(s.config.ChunkSize)
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
func (s *StringSorter) sortChunksToDisk() error {
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
					n := binary.PutUvarint(scratch, uint64(len(d)))
					_, err = bufWriter.Write(scratch[:n])
					if err != nil {
						return err
					}
					_, err = bufWriter.WriteString(d)
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

// mergeNChunks runs asynchronously in the background feeding data to getNext
// sends errors to s.mergeErrorChan
func (s *StringSorter) mergeNChunks() {
	//populate queue with data from mergeFile list
	var err error
	defer close(s.mergeChunkChan)
	defer close(s.mergeErrChan)
	pq := queue.NewPriorityQueue(func(a, b interface{}) bool {
		return a.(*mergeStringFile).nextRec < b.(*mergeStringFile).nextRec
	})
	for _, filename := range s.mergeFileList {
		// check the the file exists
		if _, err = os.Stat(filename); err != nil {
			s.mergeErrChan <- err
			return
		}

		merge := new(mergeStringFile)
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
		merge := pq.Peek().(*mergeStringFile)
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
type mergeStringFile struct {
	nextRec string
	file    *os.File
	reader  *bufio.Reader
}

// getNext returns the next value from the sorted chunk on disk
// the first call will return nil while the struct is initialized
func (m *mergeStringFile) getNext() (string, bool, error) {
	var newRecBytes []byte
	old := m.nextRec

	n, err := binary.ReadUvarint(m.reader)
	if err == nil {
		newRecBytes = make([]byte, int(n))
		_, err = io.ReadFull(m.reader, newRecBytes)
	}
	if err != nil {
		if err == io.EOF {
			m.nextRec = ""
			if m.file != nil {
				err = m.file.Close()
				if err != nil {
					return "", false, err
				}
				err = os.Remove(m.file.Name())
				if err != nil {
					return "", false, err
				}
				m.file = nil
			}
			return old, false, nil
		}
		return "", false, err
	}

	m.nextRec = string(newRecBytes)
	return old, true, nil
}
