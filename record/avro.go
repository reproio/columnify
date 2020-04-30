package record

import (
	"bytes"
	"fmt"

	"github.com/reproio/columnify/schema"

	"github.com/linkedin/goavro/v2"
)

func FormatAvroToMap(data []byte) ([]map[string]interface{}, error) {
	r, err := goavro.NewOCFReader(bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("avro parse error %v: %w", err, ErrUnconvertibleRecord)
	}

	maps := make([]map[string]interface{}, 0)
	for r.Scan() {
		v, err := r.Read()
		if err != nil {
			return nil, fmt.Errorf("avro parse error %v: %w", err, ErrUnconvertibleRecord)
		}
		m, mapOk := v.(map[string]interface{})
		if !mapOk {
			return nil, fmt.Errorf("invalid value %v: %w", v, ErrUnconvertibleRecord)
		}
		maps = append(maps, m)
	}

	return maps, nil
}

func FormatAvroToArrow(s *schema.IntermediateSchema, data []byte) (*WrappedRecord, error) {
	maps, err := FormatAvroToMap(data)
	if err != nil {
		return nil, err
	}

	return formatMapToArrowRecord(s.ArrowSchema, maps)
}
