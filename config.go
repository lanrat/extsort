package extsort

import (
	"fmt"
	"os"
)

// Config holds configuration settings for extsort
type Config struct {
	ChunkSize           int    // amount of records to store in each chunk which will be written to disk
	NumSortWorkers      int    // maximum number of workers to use to sort chunks before saving to disk
	NumSaveWorkers      int    // maximum number of workers to use to save chunks to disk
	ChanBuffSize        int    // buffer size for merging chunks
	SortedChanBuffSize  int    // buffer size for passing records to output
	FileSortBufferSize  int    // file IO buffer size for each file
	TempFilesDir        string // empty for use OS default ex: /tmp
	MergeFilenamePrefix string // filename prefix for files put in temp directory
}

// DefaultConfig returns the default configuration options sued if none provided
func DefaultConfig() *Config {
	return &Config{
		ChunkSize:           int(2e7 / 4), // ~1GB/4 files,
		NumSortWorkers:      4,
		NumSaveWorkers:      3,
		ChanBuffSize:        1,
		SortedChanBuffSize:  10,
		FileSortBufferSize:  1000000 * 4, // 4MB
		MergeFilenamePrefix: fmt.Sprintf("extsort_%d_", os.Getpid()),
		TempFilesDir:        "",
	}
}

// mergeConfig takes a provided config and replaces any values not set with the defaults
func mergeConfig(c *Config) *Config {
	d := DefaultConfig()
	if c == nil {
		return d
	}
	if c.ChunkSize <= 1 {
		c.ChunkSize = d.ChunkSize
	}
	if c.NumSortWorkers <= 1 {
		c.NumSortWorkers = d.NumSortWorkers
	}
	if c.NumSaveWorkers <= 1 {
		c.NumSaveWorkers = d.NumSaveWorkers
	}
	if c.ChanBuffSize < 0 {
		c.ChanBuffSize = d.ChanBuffSize
	}
	if c.SortedChanBuffSize < 0 {
		c.SortedChanBuffSize = d.SortedChanBuffSize
	}
	if c.FileSortBufferSize < 0 {
		c.FileSortBufferSize = d.FileSortBufferSize
	}
	if c.MergeFilenamePrefix == "" {
		c.MergeFilenamePrefix = d.MergeFilenamePrefix
	}
	// skipping TempFilesDir as it is the empty string
	return c
}
