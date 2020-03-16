package schema

import (
	"encoding/json"
	"fmt"

	"github.com/repro/columnify/avro"

	"github.com/apache/arrow/go/arrow"
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
			return nil, false, fmt.Errorf("invalid schema conversion at %v", t)
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

	if t.UnionType != nil {
		if t := isNullableField(t); t != nil {
			if nested, _, err := avroTypeToArrowType(*t); err == nil {
				return nested, true, nil
			}
		}
	}

	// TODO support more types

	return nil, false, fmt.Errorf("invalid schema")
}

func isNullableField(t avro.AvroType) *avro.AvroType {
	ut := t.UnionType
	if len(*ut) == 2 && (*ut)[0].PrimitiveType == avro.ToPrimitiveType(avro.AvroPrimitiveType_Null) {
		// According to Avro spec, the "null" is usually listed first
		return &(*ut)[1]
	}

	return nil
}
