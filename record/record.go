package record

import (
	"fmt"

	"github.com/reproio/columnify/schema"
)

const (
	RecordTypeAvro    = "avro"
	RecordTypeCsv     = "csv"
	RecordTypeJsonl   = "jsonl"
	RecordTypeLtsv    = "ltsv"
	RecordTypeMsgpack = "msgpack"
	RecordTypeTsv     = "tsv"
)

func FormatToArrow(data []byte, s *schema.IntermediateSchema, recordType string) (*WrappedRecord, error) {
	switch recordType {
	case RecordTypeAvro:
		return FormatAvroToArrow(s, data)

	case RecordTypeCsv:
		return FormatCsvToArrow(s, data, CsvDelimiter)

	case RecordTypeJsonl:
		return FormatJsonlToArrow(s, data)

	case RecordTypeLtsv:
		return FormatLtsvToArrow(s, data)

	case RecordTypeMsgpack:
		return FormatMsgpackToArrow(s, data)

	case RecordTypeTsv:
		return FormatCsvToArrow(s, data, TsvDelimiter)

	default:
		return nil, fmt.Errorf("unsupported data type: %s", recordType)
	}
}

func FormatToMap(data []byte, s *schema.IntermediateSchema, recordType string) ([]map[string]interface{}, error) {
	switch recordType {
	case RecordTypeAvro:
		return FormatAvroToMap(data)

	case RecordTypeCsv:
		return FormatCsvToMap(s, data, CsvDelimiter)

	case RecordTypeJsonl:
		return FormatJsonlToMap(data)

	case RecordTypeLtsv:
		return FormatLtsvToMap(data)

	case RecordTypeMsgpack:
		return FormatMsgpackToMap(data)

	case RecordTypeTsv:
		return FormatCsvToMap(s, data, TsvDelimiter)

	default:
		return nil, fmt.Errorf("unsupported data type: %s", recordType)
	}
}
