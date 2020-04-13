package columnifier

import "io"

const (
	schemaTypeAvro     = "avro"
	schemaTypeBigquery = "bigquery"

	recordTypeCsv   = "csv"
	recordTypeJsonl = "jsonl"
	recordTypeLtsv  = "ltsv"
	recordTypeTsv   = "tsv"
)

type Columnifier interface {
	io.WriteCloser

	WriteFromFiles(paths []string) (int, error)
}

func NewColumnifier(st string, sf string, rt string, o string) (Columnifier, error) {
	return NewParquetColumnifier(st, sf, rt, o)
}
