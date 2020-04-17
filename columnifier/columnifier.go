package columnifier

import "io"

type Columnifier interface {
	io.WriteCloser

	WriteFromFiles(paths []string) (int, error)
}

func NewColumnifier(st string, sf string, rt string, o string) (Columnifier, error) {
	return NewParquetColumnifier(st, sf, rt, o)
}
