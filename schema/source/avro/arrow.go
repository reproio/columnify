package avro

import (
	"encoding/json"
	"fmt"

	"github.com/apache/arrow/go/arrow"
	"github.com/repro/columnify/schema/intermediate"
)

var (
	primitivesToArrow = map[primitiveType]arrow.DataType{
		AvroPrimitiveType_Boolean: arrow.FixedWidthTypes.Boolean,
		AvroPrimitiveType_Int:     arrow.PrimitiveTypes.Uint32,
		AvroPrimitiveType_Long:    arrow.PrimitiveTypes.Uint64,
		AvroPrimitiveType_Float:   arrow.PrimitiveTypes.Float32,
		AvroPrimitiveType_Double:  arrow.PrimitiveTypes.Float64,
		AvroPrimitiveType_String:  arrow.BinaryTypes.String,
		AvroPrimitiveType_Bytes:   arrow.BinaryTypes.Binary,
		// AvroPrimitiveTypeNull doesn't have direct mapping rule
	}
)

var ErroInvalidSchemaConversion = fmt.Errorf("invalid schema conversion")

func NewArrowSchemaFromAvroSchema(schemaContent []byte) (*intermediate.IntermediateSchema, error) {
	var rt recordType
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

	return intermediate.NewIntermediateSchema(arrow.NewSchema(fields, nil), rt.Name), nil
}

func toArrowType(t avroType) (arrow.DataType, bool, error) {
	if t.primitiveType != nil {
		if t, ok := primitivesToArrow[*t.primitiveType]; !ok {
			return nil, false, ErroInvalidSchemaConversion
		} else {
			return t, false, nil
		}
	}

	if t.unionType != nil {
		if t := isNullableField(t); t != nil {
			if nested, _, err := toArrowType(*t); err == nil {
				return nested, true, nil
			}
		}
	}

	// TODO support more types

	return nil, false, ErrInvalidAvroSchema
}

func isNullableField(t avroType) *avroType {
	ut := t.unionType
	if len(*ut) == 2 && (*ut)[0].primitiveType == toPrimitiveType(AvroPrimitiveType_Null) {
		// According to Avro spec, the "null" is usually listed first
		return &(*ut)[1]
	}

	return nil
}
