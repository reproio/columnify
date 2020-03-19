package columnifier

const (
	schemaTypeAvro     = "avro"
	schemaTypeBigquery = "bigquery"

	recordTypeCsv   = "csv"
	recordTypeJsonl = "jsonl"
	recordTypeLtsv  = "ltsv"
	recordTypeTsv   = "tsv"
)

type Columnifier interface {
	Write(data []byte) error
	WriteFromFiles(paths []string) error
	Flush() error
}

func NewColumnifier(st string, sf string, rt string, o string) (Columnifier, error) {
	return NewParquetColumnifier(st, sf, rt, o)
}
