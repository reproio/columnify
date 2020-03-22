package record

import (
	"encoding/json"
	"fmt"
	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/memory"
	"strings"
)

func FormatJsonlToMap(data []byte) ([]map[string]interface{}, error) {
	lines := strings.Split(string(data), "\n")

	records := make([]map[string]interface{}, 0)
	for _, l := range lines {
		var e map[string]interface{}
		if err := json.Unmarshal([]byte(l), &e); err != nil {
			return nil, err
		}

		records = append(records, e)
	}

	return records, nil
}

func FormatJsonlToArrow(s *arrow.Schema, data []byte) (*WrappedRecord, error) {
	lines := strings.Split(string(data), "\n")

	pool := memory.NewGoAllocator()
	b := array.NewRecordBuilder(pool, s)

	for _, line := range lines {
		m := make(map[string]interface{})
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			return nil, err
		}

		for i, f := range s.Fields() {
			v, ok := m[f.Name]
			if !ok {
				return nil, fmt.Errorf("unexpected input: %v", v)
			}

			switch f.Type.ID() {
			case arrow.BOOL:
				if vv, ok := v.(bool); ok {
					b.Field(i).(*array.BooleanBuilder).Append(vv)
				} else {
					return nil, fmt.Errorf("unexpected input: %v", v)
				}
			case arrow.UINT32:
				if vv, ok := v.(float64); ok {
					b.Field(i).(*array.Uint32Builder).Append(uint32(vv))
				} else {
					return nil, fmt.Errorf("unexpected input: %v", v)
				}
			case arrow.UINT64:
				if vv, ok := v.(float64); ok {
					b.Field(i).(*array.Uint64Builder).Append(uint64(vv))
				} else {
					return nil, fmt.Errorf("unexpected input: %v", v)
				}
			case arrow.FLOAT32:
				if vv, ok := v.(float64); ok {
					b.Field(i).(*array.Float32Builder).Append(float32(vv))
				} else {
					return nil, fmt.Errorf("unexpected input: %v", v)
				}
			case arrow.FLOAT64:
				if vv, ok := v.(float64); ok {
					b.Field(i).(*array.Float64Builder).Append(vv)
				} else {
					return nil, fmt.Errorf("unexpected input: %v", v)
				}
			case arrow.STRING:
				if vv, ok := v.(string); ok {
					b.Field(i).(*array.StringBuilder).Append(vv)
				} else {
					return nil, fmt.Errorf("unexpected input: %v", v)
				}
			case arrow.BINARY:
				if vv, ok := v.(string); ok {
					b.Field(i).(*array.BinaryBuilder).Append([]byte(vv))
				} else {
					return nil, fmt.Errorf("unexpected input: %v", v)
				}

			// TODO STRUCT

			// TODO LIST

			default:
				return nil, fmt.Errorf("unconvertable type: %v", f.Type.ID())
			}
		}
	}

	return NewWrappedRecord(b), nil
}
