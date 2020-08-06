package record

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"

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

// innerDecoder decodes data from given Reader to the intermediate representation.
type innerDecoder interface {
	// Decode reads input data via Reader and extract it to the argument.
	// If there is no data left to be read, Read returns nil, io.EOF.
	Decode(r *map[string]interface{}) error
}

// jsonDecoder decodes data with innerDecoder and returns JSON string value.
type jsonDecoder struct {
	inner innerDecoder
}

func NewJsonDecoder(r io.Reader, s *schema.IntermediateSchema, recordType string) (*jsonDecoder, error) {
	var inner innerDecoder
	var err error

	switch recordType {
	case RecordTypeAvro:
		inner, err = newAvroInnerDecoder(r)

	case RecordTypeCsv:
		inner, err = newCsvInnerDecoder(r, s, CsvDelimiter)

	case RecordTypeJsonl:
		inner = newJsonlInnerDecoder(r)

	case RecordTypeLtsv:
		inner = newLtsvInnerDecoder(r)

	case RecordTypeMsgpack:
		inner = newMsgpackInnerDecoder(r)

	case RecordTypeTsv:
		inner, err = newCsvInnerDecoder(r, s, TsvDelimiter)

	default:
		return nil, fmt.Errorf("unsupported record type %s: %w", recordType, ErrUnsupportedRecord)
	}

	return &jsonDecoder{
		inner: inner,
	}, err
}

func (d *jsonDecoder) Decode(v *string) error {
	var vv map[string]interface{}

	err := d.inner.Decode(&vv)
	if err != nil {
		return err
	}

	data, err := json.Marshal(vv)
	if err != nil {
		return err
	}
	*v = string(data)

	return nil
}

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
		return nil, fmt.Errorf("unsupported record type %s: %w", recordType, ErrUnsupportedRecord)
	}
}

// FormatToMap converts input data to map based data with given schema.
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
		return nil, fmt.Errorf("unsupported record type %s: %w", recordType, ErrUnsupportedRecord)
	}
}
