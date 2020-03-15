package schema

import (
	"encoding/json"
	"fmt"

	"github.com/repro/columnify/avro"

	"github.com/apache/arrow/go/arrow"
)

var (
	primitivesToArrow = map[avro.PrimitiveType]arrow.DataType{
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

func NewArrowSchemaFromAvroSchema(schemaContent []byte) (*IntermediateSchema, error) {
	var rt avro.RecordType
	if err := json.Unmarshal(schemaContent, &rt); err != nil {
		return nil, err
	}

	fields := make([]arrow.Field, 0)
	for _, f := range rt.Fields {
		t, nullable, err := toArrowType(f.Type)
		if err != nil {
			return nil, err
		}

		f := arrow.Field{
			Name:     f.Name,
			Type:     t,
			Nullable: nullable,
		}

		fields = append(fields, f)
	}

	return NewIntermediateSchema(arrow.NewSchema(fields, nil), rt.Name), nil
}

func toArrowType(t avro.AvroType) (arrow.DataType, bool, error) {
	if t.PrimitiveType != nil {
		if t, ok := primitivesToArrow[*t.PrimitiveType]; !ok {
			return nil, false, fmt.Errorf("invalid schema conversion")
		} else {
			return t, false, nil
		}
	}

	if t.UnionType != nil {
		if t := isNullableField(t); t != nil {
			if nested, _, err := toArrowType(*t); err == nil {
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
