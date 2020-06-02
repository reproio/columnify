package record

import (
	"bytes"
	"fmt"

	"github.com/reproio/columnify/schema"

	"github.com/linkedin/goavro/v2"
)

// flattenAvroUnion flattens nested map type has only 1 element.
func flattenAvroUnion(in map[string]interface{}) map[string]interface{} {
	out := make(map[string]interface{})

	for k, v := range in {
		if m, ok := v.(map[string]interface{}); ok {
			// Flatten because Avro-JSON representation has redundant nested map type.
			// see also https://github.com/linkedin/goavro#translating-from-go-to-avro-data
			if len(m) == 1 {
				for _, vv := range m {
					out[k] = vv
					break
				}
			} else {
				out[k] = flattenAvroUnion(m)
			}
		} else {
			out[k] = v
		}
	}

	return out
}

func FormatAvroToMap(data []byte) ([]map[string]interface{}, error) {
	r, err := goavro.NewOCFReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}

	maps := make([]map[string]interface{}, 0)
	for r.Scan() {
		v, err := r.Read()
		if err != nil {
			return nil, err
		}
		m, mapOk := v.(map[string]interface{})
		if !mapOk {
			return nil, fmt.Errorf("invalid value %v: %w", v, ErrUnconvertibleRecord)
		}
		flatten := flattenAvroUnion(m)
		maps = append(maps, flatten)
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
