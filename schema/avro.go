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
		avro.AvroPrimitiveType_Int:     arrow.PrimitiveTypes.Int32,
		avro.AvroPrimitiveType_Long:    arrow.PrimitiveTypes.Int64,
		avro.AvroPrimitiveType_Float:   arrow.PrimitiveTypes.Float32,
		avro.AvroPrimitiveType_Double:  arrow.PrimitiveTypes.Float64,
		avro.AvroPrimitiveType_String:  arrow.BinaryTypes.String,
		avro.AvroPrimitiveType_Bytes:   arrow.BinaryTypes.Binary,
		// AvroPrimitiveTypeNull doesn't have direct mapping rule
	}

	avroLogicalTypeToArrow = map[string]arrow.DataType{
		avro.AvroLogicalType_Date:            arrow.FixedWidthTypes.Date32,
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
		return nil, fmt.Errorf("schema is wrong %v: %w", err, ErrInvalidSchema)
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
	tpe, nullable := extractAvroTypeWithNullability(f.Type)

	t, err := avroTypeToArrowType(tpe)
	if err != nil {
		return nil, err
	}

	return &arrow.Field{
		Name:     f.Name,
		Type:     t,
		Nullable: nullable,
	}, nil
}

func avroTypeToArrowType(t avro.AvroType) (arrow.DataType, error) {
	if t.PrimitiveType != nil {
		if t, ok := avroPrimitivesToArrow[*t.PrimitiveType]; !ok {
			return nil, fmt.Errorf("unsupported primitive type %v: %w", t, ErrUnconvertibleSchema)
		} else {
			return t, nil
		}
	}

	if t.RecordType != nil {
		fields := make([]arrow.Field, 0, len(t.RecordType.Fields))
		for _, f := range t.RecordType.Fields {
			af, err := avroFieldToArrowField(f)
			if err != nil {
				return nil, err
			}
			fields = append(fields, *af)
		}
		return arrow.StructOf(fields...), nil
	}

	if t.EnumsType != nil {
		return arrow.BinaryTypes.String, nil
	}

	if t.ArrayType != nil {
		itemType, err := avroTypeToArrowType(t.ArrayType.Items)
		if err != nil {
			return nil, err
		}
		return arrow.ListOf(itemType), nil
	}

	if t.MapsType != nil {
		// TODO support map type
		// NOTE arrow go module has not supported map typpe yet ???
		return nil, fmt.Errorf("map type conversion is unsupported: %w", ErrUnconvertibleSchema)
	}

	// TODO support union type except ["null", "type"] nullable pattern

	if t.FixedType != nil {
		return arrow.BinaryTypes.Binary, nil
	}

	if t.LogicalType != nil {
		if t, ok := avroLogicalTypeToArrow[t.LogicalType.LogicalType]; !ok {
			return nil, fmt.Errorf("unsupported logical type %v: %w", t, ErrUnconvertibleSchema)
		} else {
			return t, nil
		}
	}

	// TODO defined types

	return nil, fmt.Errorf("unsupported type %v: %w", t, ErrUnconvertibleSchema)
}

// extractAvroTypeWithNullability extracts union type or others to avro type with nullable flag.
func extractAvroTypeWithNullability(t avro.AvroType) (avro.AvroType, bool) {
	if t.UnionType != nil {
		// According to Avro spec, the "null" is usually listed first
		if len(*t.UnionType) == 2 && *(*t.UnionType)[0].PrimitiveType == *avro.ToPrimitiveType(avro.AvroPrimitiveType_Null) {
			return (*t.UnionType)[1], true
		}
	}

	return t, false
}
