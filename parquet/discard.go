package parquet

import (
	"fmt"

	"github.com/xitongsys/parquet-go/source"
)

// discard is an implementation of ParquetFile, just discard written data.
type discard struct{}

func NewDiscard() *discard {
	return &discard{}
}

func (f *discard) Read(p []byte) (n int, err error) {
	return -1, fmt.Errorf("never implemented: %w", ErrUnsupportedMethod)
}

func (f *discard) Write(p []byte) (n int, err error) {
	return len(p), nil
}

func (f *discard) Seek(offset int64, whence int) (int64, error) {
	return -1, fmt.Errorf("never implemented: %w", ErrUnsupportedMethod)
}

func (f *discard) Close() error {
	return nil
}

func (f *discard) Open(name string) (source.ParquetFile, error) {
	return nil, fmt.Errorf("never implemented: %w", ErrUnsupportedMethod)
}

func (f *discard) Create(name string) (source.ParquetFile, error) {
	return nil, fmt.Errorf("never implemented: %w", ErrUnsupportedMethod)
}
