package columnifier

import "io"

// Columnifier is the interface that converts input file to columnar format file.
type Columnifier interface {
	io.WriteCloser

	WriteFromFiles(paths []string) (int, error)
}

// NewColumnifier creates a new Columnifier.
func NewColumnifier(st string, sf string, rt string, o string) (Columnifier, error) {
	return NewParquetColumnifier(st, sf, rt, o)
}
