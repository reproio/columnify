package record

import (
	"fmt"
	"strconv"
	"time"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
)

type WrappedRecord struct {
	Record array.Record
}

func NewWrappedRecord(b *array.RecordBuilder) *WrappedRecord {
	return &WrappedRecord{
		Record: b.NewRecord(),
	}
}

func formatMapToArrowRecord(b *array.RecordBuilder, m map[string]interface{}) (*array.RecordBuilder, error) {
	for i, f := range b.Schema().Fields() {
		if v, ok := m[f.Name]; ok {
			if _, err := formatMapToArrowField(b.Field(i), f.Type, f.Nullable, v); err != nil {
				return nil, err
			}
		} else {
			b.Field(i).AppendNull()
		}
	}

	return b, nil
}

func formatMapToArrowStruct(b *array.StructBuilder, s *arrow.StructType, m map[string]interface{}) (*array.StructBuilder, error) {
	for i, f := range s.Fields() {
		if v, ok := m[f.Name]; ok {
			if _, err := formatMapToArrowField(b.FieldBuilder(i), f.Type, f.Nullable, v); err != nil {
				return nil, err
			}
		} else {
			b.FieldBuilder(i).AppendNull()
		}

	}

	return b, nil
}

func formatMapToArrowList(b *array.ListBuilder, l *arrow.ListType, list []interface{}) (*array.ListBuilder, error) {
	for _, e := range list {
		// NOTE list type always accepts null values?
		if _, err := formatMapToArrowField(b.ValueBuilder(), l.Elem(), true, e); err != nil {
			return nil, err
		}
	}

	return b, nil
}

func formatMapToArrowField(b array.Builder, t arrow.DataType, nullable bool, v interface{}) (array.Builder, error) {
	if v == nil && nullable {
		b.AppendNull()
		return b, nil
	}

	switch t.ID() {
	case arrow.BOOL:
		vb, builderOk := b.(*array.BooleanBuilder)
		vv, valueOk := v.(bool)
		if builderOk && valueOk {
			vb.Append(vv)
		} else {
			return nil, fmt.Errorf("unexpected input %v as bool: %w", v, ErrUnconvertibleRecord)
		}

	case arrow.INT32:
		vb, builderOk := b.(*array.Int32Builder)
		if !builderOk {
			return nil, fmt.Errorf("builder %v is wrong: %w", v, ErrUnconvertibleRecord)
		}
		switch vv := v.(type) {
		case int:
			vb.Append(int32(vv))
		case int8:
			vb.Append(int32(vv))
		case int16:
			vb.Append(int32(vv))
		case int32:
			vb.Append(int32(vv))
		case int64:
			vb.Append(int32(vv))
		case uint:
			vb.Append(int32(vv))
		case uint8:
			vb.Append(int32(vv))
		case uint16:
			vb.Append(int32(vv))
		case uint32:
			vb.Append(int32(vv))
		case uint64:
			vb.Append(int32(vv))
		case float64:
			vb.Append(int32(vv))
		default:
			return nil, fmt.Errorf("unexpected input %v as int32: %w", v, ErrUnconvertibleRecord)
		}

	case arrow.INT64:
		vb, builderOk := b.(*array.Int64Builder)
		if !builderOk {
			return nil, fmt.Errorf("builder %v is wrong: %w", v, ErrUnconvertibleRecord)
		}
		switch vv := v.(type) {
		case int:
			vb.Append(int64(vv))
		case int8:
			vb.Append(int64(vv))
		case int16:
			vb.Append(int64(vv))
		case int32:
			vb.Append(int64(vv))
		case int64:
			vb.Append(int64(vv))
		case uint:
			vb.Append(int64(vv))
		case uint8:
			vb.Append(int64(vv))
		case uint16:
			vb.Append(int64(vv))
		case uint32:
			vb.Append(int64(vv))
		case uint64:
			vb.Append(int64(vv))
		case float64:
			vb.Append(int64(vv))
		default:
			return nil, fmt.Errorf("unexpected input %v as int64: %w", v, ErrUnconvertibleRecord)
		}

	case arrow.UINT32:
		vb, builderOk := b.(*array.Uint32Builder)
		if !builderOk {
			return nil, fmt.Errorf("builder %v is wrong: %w", v, ErrUnconvertibleRecord)
		}
		switch vv := v.(type) {
		case int:
			vb.Append(uint32(vv))
		case int8:
			vb.Append(uint32(vv))
		case int16:
			vb.Append(uint32(vv))
		case int32:
			vb.Append(uint32(vv))
		case int64:
			vb.Append(uint32(vv))
		case uint:
			vb.Append(uint32(vv))
		case uint8:
			vb.Append(uint32(vv))
		case uint16:
			vb.Append(uint32(vv))
		case uint32:
			vb.Append(uint32(vv))
		case uint64:
			vb.Append(uint32(vv))
		case float64:
			vb.Append(uint32(vv))
		default:
			return nil, fmt.Errorf("unexpected input %v as uint32: %w", v, ErrUnconvertibleRecord)
		}

	case arrow.UINT64:
		vb, builderOk := b.(*array.Uint64Builder)
		if !builderOk {
			return nil, fmt.Errorf("builder %v is wrong: %w", v, ErrUnconvertibleRecord)
		}
		switch vv := v.(type) {
		case int8:
			vb.Append(uint64(vv))
		case int16:
			vb.Append(uint64(vv))
		case int32:
			vb.Append(uint64(vv))
		case int64:
			vb.Append(uint64(vv))
		case uint8:
			vb.Append(uint64(vv))
		case uint16:
			vb.Append(uint64(vv))
		case uint32:
			vb.Append(uint64(vv))
		case uint64:
			vb.Append(uint64(vv))
		case float64:
			vb.Append(uint64(vv))
		default:
			return nil, fmt.Errorf("unexpected input %v as uint64: %w", v, ErrUnconvertibleRecord)
		}

	case arrow.FLOAT32:
		vb, builderOk := b.(*array.Float32Builder)
		if !builderOk {
			return nil, fmt.Errorf("builder %v is wrong: %w", v, ErrUnconvertibleRecord)
		}
		switch vv := v.(type) {
		case float32:
			vb.Append(vv)
		case float64:
			vb.Append(float32(vv))
		default:
			return nil, fmt.Errorf("unexpected input %v as float32: %w", v, ErrUnconvertibleRecord)
		}

	case arrow.FLOAT64:
		vb, builderOk := b.(*array.Float64Builder)
		if !builderOk {
			return nil, fmt.Errorf("builder %v is wrong: %w", b, ErrUnconvertibleRecord)
		}
		switch vv := v.(type) {
		case float32:
			f64, err := strconv.ParseFloat(fmt.Sprint(vv), 64)
			if err != nil {
				return nil, fmt.Errorf("invalid input %v: %w", vv, ErrUnconvertibleRecord)
			}
			vb.Append(f64)
		case float64:
			vb.Append(vv)
		default:
			return nil, fmt.Errorf("unexpected input %v as float64: %w", v, ErrUnconvertibleRecord)
		}

	case arrow.STRING:
		vb, builderOk := b.(*array.StringBuilder)
		vv, valueOk := v.(string)
		if builderOk && valueOk {
			vb.Append(vv)
		} else {
			return nil, fmt.Errorf("unexpected input %v as string: %w", v, ErrUnconvertibleRecord)
		}

	case arrow.BINARY:
		vb, builderOk := b.(*array.BinaryBuilder)
		if !builderOk {
			return nil, fmt.Errorf("builder %v is wrong: %w", v, ErrUnconvertibleRecord)
		}
		switch vv := v.(type) {
		case string:
			vb.Append([]byte(vv))
		case []byte:
			vb.Append(vv)
		default:
			return nil, fmt.Errorf("unexpected input %v as binary: %w", v, ErrUnconvertibleRecord)
		}

	case arrow.DATE32:
		vb, builderOk := b.(*array.Date32Builder)
		if !builderOk {
			return nil, fmt.Errorf("builder %v is wrong: %w", b, ErrUnconvertibleRecord)
		}
		switch vv := v.(type) {
		case int:
			vb.Append(arrow.Date32(vv))
		case int8:
			vb.Append(arrow.Date32(vv))
		case int16:
			vb.Append(arrow.Date32(vv))
		case int32:
			vb.Append(arrow.Date32(vv))
		case int64:
			vb.Append(arrow.Date32(vv))
		case uint:
			vb.Append(arrow.Date32(vv))
		case uint8:
			vb.Append(arrow.Date32(vv))
		case uint16:
			vb.Append(arrow.Date32(vv))
		case uint32:
			vb.Append(arrow.Date32(vv))
		case uint64:
			vb.Append(arrow.Date32(vv))
		case float64:
			vb.Append(arrow.Date32(vv))
		case time.Time:
			_, _, d := vv.Date()
			vb.Append(arrow.Date32(d - 1))
		default:
			return nil, fmt.Errorf("unexpected input %v as Date32: %w", v, ErrUnconvertibleRecord)
		}

	case arrow.DATE64:
		vb, builderOk := b.(*array.Date64Builder)
		if !builderOk {
			return nil, fmt.Errorf("builder %v is wrong: %w", b, ErrUnconvertibleRecord)
		}
		switch vv := v.(type) {
		case int:
			vb.Append(arrow.Date64(vv))
		case int8:
			vb.Append(arrow.Date64(vv))
		case int16:
			vb.Append(arrow.Date64(vv))
		case int32:
			vb.Append(arrow.Date64(vv))
		case int64:
			vb.Append(arrow.Date64(vv))
		case uint:
			vb.Append(arrow.Date64(vv))
		case uint8:
			vb.Append(arrow.Date64(vv))
		case uint16:
			vb.Append(arrow.Date64(vv))
		case uint32:
			vb.Append(arrow.Date64(vv))
		case uint64:
			vb.Append(arrow.Date64(vv))
		case float64:
			vb.Append(arrow.Date64(vv))
		case time.Time:
			_, _, d := vv.Date()
			vb.Append(arrow.Date64(d - 1))
		default:
			return nil, fmt.Errorf("unexpected input %v as Date64: %w", v, ErrUnconvertibleRecord)
		}

	case arrow.TIME32:
		vb, builderOk := b.(*array.Time32Builder)
		if !builderOk {
			return nil, fmt.Errorf("builder %v is wrong: %w", b, ErrUnconvertibleRecord)
		}
		switch vv := v.(type) {
		case int:
			vb.Append(arrow.Time32(vv))
		case int8:
			vb.Append(arrow.Time32(vv))
		case int16:
			vb.Append(arrow.Time32(vv))
		case int32:
			vb.Append(arrow.Time32(vv))
		case int64:
			vb.Append(arrow.Time32(vv))
		case uint:
			vb.Append(arrow.Time32(vv))
		case uint8:
			vb.Append(arrow.Time32(vv))
		case uint16:
			vb.Append(arrow.Time32(vv))
		case uint32:
			vb.Append(arrow.Time32(vv))
		case uint64:
			vb.Append(arrow.Time32(vv))
		case float64:
			vb.Append(arrow.Time32(vv))
		case time.Duration:
			vb.Append(arrow.Time32(vv.Milliseconds()))
		default:
			return nil, fmt.Errorf("unexpected input %v as Time32: %w", v, ErrUnconvertibleRecord)
		}

	case arrow.TIME64:
		vb, builderOk := b.(*array.Time64Builder)
		if !builderOk {
			return nil, fmt.Errorf("builder %v is wrong: %w", b, ErrUnconvertibleRecord)
		}
		switch vv := v.(type) {
		case int:
			vb.Append(arrow.Time64(vv))
		case int8:
			vb.Append(arrow.Time64(vv))
		case int16:
			vb.Append(arrow.Time64(vv))
		case int32:
			vb.Append(arrow.Time64(vv))
		case int64:
			vb.Append(arrow.Time64(vv))
		case uint:
			vb.Append(arrow.Time64(vv))
		case uint8:
			vb.Append(arrow.Time64(vv))
		case uint16:
			vb.Append(arrow.Time64(vv))
		case uint32:
			vb.Append(arrow.Time64(vv))
		case uint64:
			vb.Append(arrow.Time64(vv))
		case float64:
			vb.Append(arrow.Time64(vv))
		case time.Duration:
			vb.Append(arrow.Time64(vv.Microseconds()))
		default:
			return nil, fmt.Errorf("unexpected input %v as Time64: %w", v, ErrUnconvertibleRecord)
		}

	case arrow.TIMESTAMP:
		vb, builderOk := b.(*array.TimestampBuilder)
		if !builderOk {
			return nil, fmt.Errorf("builder %v is wrong: %w", b, ErrUnconvertibleRecord)
		}
		switch vv := v.(type) {
		case int:
			vb.Append(arrow.Timestamp(vv))
		case int8:
			vb.Append(arrow.Timestamp(vv))
		case int16:
			vb.Append(arrow.Timestamp(vv))
		case int32:
			vb.Append(arrow.Timestamp(vv))
		case int64:
			vb.Append(arrow.Timestamp(vv))
		case uint:
			vb.Append(arrow.Timestamp(vv))
		case uint8:
			vb.Append(arrow.Timestamp(vv))
		case uint16:
			vb.Append(arrow.Timestamp(vv))
		case uint32:
			vb.Append(arrow.Timestamp(vv))
		case uint64:
			vb.Append(arrow.Timestamp(vv))
		case float64:
			vb.Append(arrow.Timestamp(vv))
		case time.Time:
			tt, ok := t.(*arrow.TimestampType)
			if !ok {
				return nil, fmt.Errorf("unexpected type %v as Timestamp: %w", t, ErrUnconvertibleRecord)
			}
			switch tt.Unit {
			case arrow.Millisecond:
				vb.Append(arrow.Timestamp(vv.UnixNano() / 1000000))
			case arrow.Microsecond:
				vb.Append(arrow.Timestamp(vv.UnixNano() / 1000))
			default:
				return nil, fmt.Errorf("unexpected input %v as Timestamp: %w", v, ErrUnconvertibleRecord)
			}
		default:
			return nil, fmt.Errorf("unexpected input %v as Timestamp: %w", v, ErrUnconvertibleRecord)
		}

	case arrow.STRUCT:
		vb, builderOk := b.(*array.StructBuilder)
		st, structOk := t.(*arrow.StructType)
		if builderOk && structOk {
			if v != nil {
				vb.Append(true)
				vv, valueOk := v.(map[string]interface{})
				if !valueOk {
					return nil, fmt.Errorf("unexpected input %v as struct: %w", v, ErrUnconvertibleRecord)
				} else if _, err := formatMapToArrowStruct(vb, st, vv); err != nil {
					return nil, err
				}
			} else {
				vb.Append(false)
			}
		} else {
			return nil, fmt.Errorf("unexpected input %v as struct: %w", v, ErrUnconvertibleRecord)
		}

	case arrow.LIST:
		vb, builderOk := b.(*array.ListBuilder)
		lt, listOk := t.(*arrow.ListType)
		if builderOk && listOk {
			if v != nil {
				vb.Append(true)
				vv, valueOk := v.([]interface{})
				if !valueOk {
					return nil, fmt.Errorf("unexpected input %v as list: %w", v, ErrUnconvertibleRecord)
				}
				if _, err := formatMapToArrowList(vb, lt, vv); err != nil {
					return nil, err
				}
			} else {
				vb.Append(false)
			}
		} else {
			return nil, fmt.Errorf("unexpected input %v as list: %w", v, ErrUnconvertibleRecord)
		}

	default:
		return nil, fmt.Errorf("unconvertable type %v: %w", t.ID(), ErrUnconvertibleRecord)
	}

	return b, nil
}
