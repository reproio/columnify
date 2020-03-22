package record

import (
	"fmt"
	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/memory"
)

type WrappedRecord struct {
	Record array.Record
}

func NewWrappedRecord(b *array.RecordBuilder) *WrappedRecord {
	return &WrappedRecord{
		Record: b.NewRecord(),
	}
}

func formatMapToArrow(s *arrow.Schema, maps []map[string]interface{}) (*WrappedRecord, error) {
	pool := memory.NewGoAllocator()
	b := array.NewRecordBuilder(pool, s)

	for _, m := range maps {
		for i, f := range s.Fields() {
			v, ok := m[f.Name]
			if !ok {
				return nil, fmt.Errorf("unexpected input: %v for %v", m, f.Name)
			}

			switch f.Type.ID() {
			case arrow.BOOL:
				if vv, ok := v.(bool); ok {
					b.Field(i).(*array.BooleanBuilder).Append(vv)
				} else {
					return nil, fmt.Errorf("unexpected input: %v as bool value", v)
				}

			case arrow.UINT32:
				if vv, ok := v.(float64); ok {
					b.Field(i).(*array.Uint32Builder).Append(uint32(vv))
				} else {
					return nil, fmt.Errorf("unexpected input: %v as uint32 value", v)
				}

			case arrow.UINT64:
				if vv, ok := v.(float64); ok {
					b.Field(i).(*array.Uint64Builder).Append(uint64(vv))
				} else {
					return nil, fmt.Errorf("unexpected input: %v as uint64 value", v)
				}

			case arrow.FLOAT32:
				if vv, ok := v.(float64); ok {
					b.Field(i).(*array.Float32Builder).Append(float32(vv))
				} else {
					return nil, fmt.Errorf("unexpected input: %v as float32 value", v)
				}

			case arrow.FLOAT64:
				if vv, ok := v.(float64); ok {
					b.Field(i).(*array.Float64Builder).Append(vv)
				} else {
					return nil, fmt.Errorf("unexpected input: %v as float64 value", v)
				}

			case arrow.STRING:
				if vv, ok := v.(string); ok {
					b.Field(i).(*array.StringBuilder).Append(vv)
				} else {
					return nil, fmt.Errorf("unexpected input: %v as string value", v)
				}

			case arrow.BINARY:
				if vv, ok := v.(string); ok {
					b.Field(i).(*array.BinaryBuilder).Append([]byte(vv))
				} else {
					return nil, fmt.Errorf("unexpected input: %v as binary value", v)
				}

			case arrow.STRUCT:
				return nil, fmt.Errorf("unimplemented yet") // FIXME

			case arrow.LIST:
				return nil, fmt.Errorf("unimplemented yet") // FIXME

			default:
				return nil, fmt.Errorf("unconvertable type: %v", f.Type.ID())
			}
		}
	}

	return NewWrappedRecord(b), nil
}
