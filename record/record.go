package record

import (
	"errors"
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

var (
	ErrUnsupportedRecord   = errors.New("unsupported record")
	ErrUnconvertibleRecord = errors.New("input record is unable to convert")
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
		return nil, fmt.Errorf("unsupported record type %s; %w", recordType, ErrUnsupportedRecord)
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
		return nil, fmt.Errorf("unsupported record type %s; %w", recordType, ErrUnsupportedRecord)
	}
}
