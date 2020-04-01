package schema

import (
	"encoding/json"
	"fmt"

	"github.com/apache/arrow/go/arrow"
	"github.com/reproio/columnify/avro"
)

var (
	avroPrimitivesToArrow = map[avro.PrimitiveType]arrow.DataType{
		avro.AvroPrimitiveType_Boolean: arrow.FixedWidthTypes.Boolean,
		avro.AvroPrimitiveType_Int:     arrow.PrimitiveTypes.Uint32,
		avro.AvroPrimitiveType_Long:    arrow.PrimitiveTypes.Uint64,
		avro.AvroPrimitiveType_Float:   arrow.PrimitiveTypes.Float32,
		avro.AvroPrimitiveType_Double:  arrow.PrimitiveTypes.Float64,
		avro.AvroPrimitiveType_String:  arrow.BinaryTypes.String,
		avro.AvroPrimitiveType_Bytes:   arrow.BinaryTypes.Binary,
		// AvroPrimitiveTypeNull doesn't have direct mapping rule
	}

	avroLogicalTypeToArrow = map[string]arrow.DataType{
		avro.AvroLogicalType_Date:            arrow.FixedWidthTypes.Date64,
		avro.AvroLogicalType_Duration:        arrow.FixedWidthTypes.Duration_ms,
		avro.AvroLogicalType_TimeMillis:      arrow.FixedWidthTypes.Time32ms,
		avro.AvroLogicalType_TimeMicros:      arrow.FixedWidthTypes.Time64us,
		avro.AvroLogicalType_TimestampMillis: arrow.FixedWidthTypes.Timestamp_ms,
		avro.AvroLogicalType_TimestampMicros: arrow.FixedWidthTypes.Timestamp_us,
		// avro.AvroLogicalType_Decimal doesn't have direct mapping rule
	}
)

func NewSchemaFromAvroSchema(schemaContent []byte) (*IntermediateSchema, error) {
	var rt avro.RecordType
	if err := json.Unmarshal(schemaContent, &rt); err != nil {
		return nil, err
	}

	fields := make([]arrow.Field, 0)
	for _, f := range rt.Fields {
		af, err := avroFieldToArrowField(f)
		if err != nil {
			return nil, err
		}
		fields = append(fields, *af)
	}

	return NewIntermediateSchema(arrow.NewSchema(fields, nil), rt.Name), nil
}

func avroFieldToArrowField(f avro.RecordField) (*arrow.Field, error) {
	t, nullable, err := avroTypeToArrowType(f.Type)
	if err != nil {
		return nil, err
	}

	return &arrow.Field{
		Name:     f.Name,
		Type:     t,
		Nullable: nullable,
	}, nil
}

func avroTypeToArrowType(t avro.AvroType) (arrow.DataType, bool, error) {
	if t.PrimitiveType != nil {
		if t, ok := avroPrimitivesToArrow[*t.PrimitiveType]; !ok {
			return nil, false, fmt.Errorf("unsupported primitive type %v; %w", t, ErrUnconvertibleSchema)
		} else {
			return t, false, nil
		}
	}

	if t.RecordType != nil {
		fields := make([]arrow.Field, 0, len(t.RecordType.Fields))
		for _, f := range t.RecordType.Fields {
			af, err := avroFieldToArrowField(f)
			if err != nil {
				return nil, false, err
			}
			fields = append(fields, *af)
		}
		return arrow.StructOf(fields...), false, nil
	}

	if t.EnumsType != nil {
		return arrow.BinaryTypes.String, false, nil
	}

	if t.ArrayType != nil {
		itemType, _, err := avroTypeToArrowType(t.ArrayType.Items)
		if err != nil {
			return nil, false, err
		}
		return arrow.ListOf(itemType), false, nil
	}

	if t.MapsType != nil {
		// TODO support map type
		// NOTE arrow go module has not supported map typpe yet ???
		return nil, false, fmt.Errorf("map type conversion is unsupported; %w", ErrUnconvertibleSchema)
	}

	if t.UnionType != nil {
		if nt := isNullableField(t.UnionType); nt != nil {
			if nested, _, err := avroTypeToArrowType(*nt); err == nil {
				return nested, true, nil
			}
		}
	}

	if t.FixedType != nil {
		return arrow.BinaryTypes.Binary, false, nil
	}

	if t.LogicalType != nil {
		if t, ok := avroLogicalTypeToArrow[t.LogicalType.LogicalType]; !ok {
			return nil, false, fmt.Errorf("unsupported logical type %v; %w", t, ErrUnconvertibleSchema)
		} else {
			return t, false, nil
		}
	}

	// TODO defined types

	return nil, false, fmt.Errorf("unsupported type %v; %w", t, ErrUnconvertibleSchema)
}

func isNullableField(ut *avro.UnionType) *avro.AvroType {
	if len(*ut) == 2 && *(*ut)[0].PrimitiveType == *avro.ToPrimitiveType(avro.AvroPrimitiveType_Null) {
		// According to Avro spec, the "null" is usually listed first
		return &(*ut)[1]
	}

	return nil
}
