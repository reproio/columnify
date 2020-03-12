package columnifier

const (
	schemaTypeAvro = "avro"
	schemaTypeJson = "json"

	dataTypeCsv   = "csv"
	dataTypeJsonl = "jsonl"
	dataTypeLtsv  = "ltsv"
)

type Columnifier interface {
	Write(data []byte) error
	WriteFromFiles(paths []string) error
	Flush() error
}

func NewColumnifier(st string, sf string, dt string, o string) (Columnifier, error) {
	return NewParquetColumnifier(st, sf, dt, o)
}
