package columnifier

import "io"

// Columnifier is the interface that converts input file to columnar format file.
type Columnifier interface {
	WriteFromReader(reader io.Reader) (int, error)
	WriteFromFiles(paths []string) (int, error)
	Close() error
}

// NewColumnifier creates a new Columnifier.
func NewColumnifier(st string, sf string, rt string, o string, config Config) (Columnifier, error) {
	return NewParquetColumnifier(st, sf, rt, o, config)
}
