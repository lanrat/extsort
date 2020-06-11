// Package extsort implements an unstable external sort for all the records in a chan or iterator
package extsort

import (
	"bufio"
	"context"
	"encoding/binary"
	"io"
	"sort"

	"github.com/lanrat/extsort/queue"
	"github.com/lanrat/extsort/tempfile"

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
	config         Config
	buildSortCtx   context.Context
	saveCtx        context.Context
	mergeErrChan   chan error
	tempReader     *tempfile.TempFileReader
	input          chan string
	chunkChan      chan *stringChunk
	saveChunkChan  chan *stringChunk
	mergeChunkChan chan string
}

// Strings returns a new Sorter instance that can be used to sort the input chan
func Strings(i chan string, config *Config) *StringSorter {
	s := new(StringSorter)
	s.input = i
	s.config = *mergeConfig(config)
	s.chunkChan = make(chan *stringChunk, s.config.ChanBuffSize)
	s.saveChunkChan = make(chan *stringChunk, s.config.ChanBuffSize)
	s.mergeChunkChan = make(chan string, s.config.SortedChanBuffSize)
	s.mergeErrChan = make(chan error, 1)
	return s
}

// Sort sorts the Sorter's input chan and returns a new sorted chan, and error Chan
// Sort is a chunking operation that runs multiple workers asynchronously
// this blocks while sorting chunks and unblocks when merging
func (s *StringSorter) Sort(ctx context.Context) (chan string, chan error) {
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
		return s.mergeChunkChan, s.mergeErrChan
	}

	// need to close saveChunkChan
	close(s.saveChunkChan)
	err = saveErrGroup.Wait()
	if err != nil {
		s.mergeErrChan <- err
		close(s.mergeErrChan)
		close(s.mergeChunkChan)
		return s.mergeChunkChan, s.mergeErrChan
	}

	// read chunks and merge
	// if this errors, it is returned in the errorChan
	go s.mergeNChunks(ctx)

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
			case <-s.buildSortCtx.Done():
				return s.buildSortCtx.Err()
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
func (s *StringSorter) sortChunks() error {
	for {
		select {
		case b, more := <-s.chunkChan:
			if more {
				// sort
				sort.Sort(b)
				// save
				s.saveChunkChan <- b
			} else {
				return nil
			}
		case <-s.buildSortCtx.Done():
			return s.buildSortCtx.Err()
		}
	}
}

// saveChunks is a worker for saveing sorted data to disk
func (s *StringSorter) saveChunks() error {
	tempWriter, err := tempfile.New(s.config.TempFilesDir)
	if err != nil {
		return err
	}
	scratch := make([]byte, binary.MaxVarintLen64)
	for {
		select {
		case b, more := <-s.saveChunkChan:
			if more {
				for _, d := range b.data {
					// binary encoding for size
					n := binary.PutUvarint(scratch, uint64(len(d)))
					_, err = tempWriter.Write(scratch[:n])
					if err != nil {
						return err
					}
					// add data
					_, err = tempWriter.WriteString(d)
					if err != nil {
						return err
					}
				}
				_, err = tempWriter.Next()
				if err != nil {
					return err
				}
			} else {
				s.tempReader, err = tempWriter.Save()
				return err
			}
		case <-s.saveCtx.Done():
			// delete the temp file from disk (error unchecked)
			tempWriter.Close()
			return s.saveCtx.Err()
		}
	}
}

// mergeNChunks runs asynchronously in the background feeding data to getNext
// sends errors to s.mergeErrorChan
func (s *StringSorter) mergeNChunks(ctx context.Context) {
	//populate queue with data from mergeFile list
	defer close(s.mergeChunkChan)
	// close temp file when done
	defer func() {
		err := s.tempReader.Close()
		if err != nil {
			s.mergeErrChan <- err
		}
	}()
	defer close(s.mergeErrChan)
	pq := queue.NewPriorityQueue(func(a, b interface{}) bool {
		return a.(*mergeStringFile).nextRec < b.(*mergeStringFile).nextRec
	})

	for i := 0; i < s.tempReader.Size(); i++ {
		merge := new(mergeStringFile)
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
		// check for err in context just in case
		select {
		case s.mergeChunkChan <- rec:
		case <-ctx.Done():
			s.mergeErrChan <- ctx.Err()
			return
		}
	}
}

// mergefile represents each sorted chunk on disk and its next value
type mergeStringFile struct {
	nextRec string
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
			return old, false, nil
		}
		return "", false, err
	}

	m.nextRec = string(newRecBytes)
	return old, true, nil
}
