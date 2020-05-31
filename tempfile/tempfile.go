// Package tempfile implements a virtual temp files that can be written to (in series)
// and then read back (series/parallel) and then removed from the filesystem when done
// if multiple "tempfiles" are needed on the application layer, they are mapped to
// sections of the same real file on the filesystem
package tempfile

import (
	"bufio"
	"fmt"
	"io"
	"io/ioutil"
	"os"
)

var (
	// file IO buffer size for each file
	fileBufferSize = 1 << 16 // 64k
	// filename prefix for files put in temp directory
	mergeFilenamePrefix = fmt.Sprintf("extsort_%d_", os.Getpid())
)

type TempFileWriter struct {
	file      *os.File
	bufWriter *bufio.Writer
	sections  []int64
}

type TempFileReader struct {
	file     *os.File
	sections []int64
	readers  []*bufio.Reader
}

func New(dir string) (*TempFileWriter, error) {
	var w TempFileWriter
	var err error
	w.file, err = ioutil.TempFile(dir, mergeFilenamePrefix)
	if err != nil {
		return nil, err
	}
	w.bufWriter = bufio.NewWriterSize(w.file, fileBufferSize)
	w.sections = make([]int64, 0, 10)

	return &w, nil
}

func (w *TempFileWriter) Size() int {
	// we add one because we only write to the sections when we are done
	return len(w.sections) + 1
}

func (w *TempFileWriter) Name() string {
	return w.file.Name()
}

// Close stops the tempfile from accepting new data,
// closes the file, and removes the temp file from disk
// works like an abort, unrecoverable
func (w *TempFileWriter) Close() error {
	err := w.file.Close()
	if err != nil {
		return err
	}
	w.sections = nil
	w.bufWriter = nil
	return os.Remove(w.file.Name())
}

func (w *TempFileWriter) Write(p []byte) (nn int, err error) {
	return w.bufWriter.Write(p)
}

func (w *TempFileWriter) WriteString(s string) (int, error) {
	return w.bufWriter.WriteString(s)
}

func (w *TempFileWriter) Next() (int64, error) {
	// save offsets
	err := w.bufWriter.Flush()
	if err != nil {
		return 0, err
	}
	pos, err := w.file.Seek(0, os.SEEK_CUR)
	if err != nil {
		return 0, err
	}
	w.sections = append(w.sections, pos)

	return pos, nil
}

func (w *TempFileWriter) Save() (*TempFileReader, error) {
	_, err := w.Next()
	if err != nil {
		return nil, err
	}
	err = w.file.Sync()
	if err != nil {
		return nil, err
	}
	err = w.file.Close()
	if err != nil {
		return nil, err
	}
	return newTempReader(w.file.Name(), w.sections)
}

func newTempReader(filename string, sections []int64) (*TempFileReader, error) {
	// create TempReader
	var err error
	var r TempFileReader
	r.file, err = os.Open(filename)
	if err != nil {
		return nil, err
	}
	r.sections = sections
	r.readers = make([]*bufio.Reader, len(r.sections))

	offset := int64(0)
	for i, end := range r.sections {
		section := io.NewSectionReader(r.file, offset, end-offset)
		offset = end
		r.readers[i] = bufio.NewReaderSize(section, fileBufferSize)
	}

	return &r, nil
}

func (r *TempFileReader) Close() error {
	r.readers = nil
	err := r.file.Close()
	if err != nil {
		return err
	}
	return os.Remove(r.file.Name())
}

func (r *TempFileReader) Size() int {
	return len(r.readers)
}

func (r *TempFileReader) Read(i int) *bufio.Reader {
	if i < 0 || i >= len(r.readers) {
		panic("tempfile: read request out of range")
	}
	return r.readers[i]
}

func (r *TempFileReader) Name() string {
	return r.file.Name()
}
