package parquet

import (
	"fmt"
	"os"

	"github.com/xitongsys/parquet-go/source"
)

// stdioFile is an implementation of ParquetFile, just writing data to stdout.
type stdioFile struct {
}

func NewStdioFile() *stdioFile {
	return &stdioFile{}
}

func (f *stdioFile) Read(p []byte) (n int, err error) {
	return os.Stdin.Read(p)
}

func (f *stdioFile) Write(p []byte) (n int, err error) {
	return os.Stdout.Write(p)
}

func (f *stdioFile) Seek(offset int64, whence int) (int64, error) {
	return 0, fmt.Errorf("unsupported yet")
}

func (f *stdioFile) Close() error {
	return nil
}

func (f *stdioFile) Open(name string) (source.ParquetFile, error) {
	return nil, fmt.Errorf("never implemented")
}

func (f *stdioFile) Create(name string) (source.ParquetFile, error) {
	return nil, fmt.Errorf("never implemented")
}
