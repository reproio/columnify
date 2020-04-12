package parquet

import (
	"fmt"
	"io"
	"os"

	"github.com/xitongsys/parquet-go/source"
)

// stdioFile is an implementation of ParquetFile, just writing data to stdout.
type stdioFile struct {
	in  io.ReadCloser
	out io.WriteCloser
}

func NewStdioFile() *stdioFile {
	return &stdioFile{
		in:  os.Stdin,
		out: os.Stdout,
	}
}

func (f *stdioFile) Read(p []byte) (n int, err error) {
	return f.in.Read(p)
}

func (f *stdioFile) Write(p []byte) (n int, err error) {
	return f.out.Write(p)
}

func (f *stdioFile) Seek(offset int64, whence int) (int64, error) {
	return 0, fmt.Errorf("unsupported yet: %w", ErrUnsupportedMethod)
}

func (f *stdioFile) Close() error {
	if err := f.in.Close(); err != nil {
		return err
	}

	if err := f.out.Close(); err != nil {
		return err
	}

	return nil
}

func (f *stdioFile) Open(name string) (source.ParquetFile, error) {
	return nil, fmt.Errorf("never implemented: %w", ErrUnsupportedMethod)
}

func (f *stdioFile) Create(name string) (source.ParquetFile, error) {
	return nil, fmt.Errorf("never implemented: %w", ErrUnsupportedMethod)
}
