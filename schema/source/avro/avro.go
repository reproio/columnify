package avro

import (
	"encoding/json"
	"fmt"
)

const (
	AvroPrimitiveType_Null    = "null"
	AvroPrimitiveType_Boolean = "boolean"
	AvroPrimitiveType_Int     = "int"
	AvroPrimitiveType_Long    = "long"
	AvroPrimitiveType_Float   = "float"
	AvroPrimitiveType_Double  = "double"
	AvroPrimitiveType_Bytes   = "bytes"
	AvroPrimitiveType_String  = "string"

	AvroComplexType_Record = "record"
	AvroComplexType_Enums  = "enum"
	AvroComplexType_Array  = "array"
	AvroComplexType_Maps   = "map"
	AvroComplexType_Fixed  = "fixed"

	AvroLogicalType_Decimal         = "decimal"
	AvroLogicalType_Date            = "date"
	AvroLogicalType_TimeMillis      = "time-millis"
	AvroLogicalType_TimeMicros      = "time-micros"
	AvroLogicalType_TimestampMillis = "timestamp-millis"
	AvroLogicalType_TimestampMicros = "timestamp-micros"
	AvroLogicalType_Duration        = "duration"
)

var (
	ErrInvalidAvroSchema = fmt.Errorf("invalid avro schema")

	avroPrimitiveTypes = []string{
		AvroPrimitiveType_Null,
		AvroPrimitiveType_Boolean,
		AvroPrimitiveType_Int,
		AvroPrimitiveType_Long,
		AvroPrimitiveType_Float,
		AvroPrimitiveType_Double,
		AvroPrimitiveType_Bytes,
		AvroPrimitiveType_String,
	}

	avroValidTypesForLogicalType = map[string][]string{
		AvroLogicalType_Decimal:         {AvroPrimitiveType_Bytes, AvroComplexType_Fixed},
		AvroLogicalType_Date:            {AvroPrimitiveType_Int},
		AvroLogicalType_TimeMillis:      {AvroPrimitiveType_Int},
		AvroLogicalType_TimeMicros:      {AvroPrimitiveType_Long},
		AvroLogicalType_TimestampMillis: {AvroPrimitiveType_Long},
		AvroLogicalType_TimestampMicros: {AvroPrimitiveType_Long},
		AvroLogicalType_Duration:        {AvroComplexType_Fixed},
	}
)

type primitiveType string

type recordType struct {
	Type      string        `json:"type"` // MUST be "record"
	Name      string        `json:"name"`
	Namespace string        `json:"namespace"`
	Doc       string        `json:"doc"`
	Aliases   []string      `json:"aliases"`
	Fields    []recordField `json:"fields"`
}

type recordField struct {
	Name    string   `json:"name"`
	Doc     string   `json:"doc"`
	Type    avroType `json:"type"`
	Default string   `json:"default"`
	Order   string   `json:"order"`
	Aliases []string `json:"aliases"`
}

type enumsType struct {
	Type      string   `json:"type"` // MUST be "enum"
	Name      string   `json:"name"`
	Namespace string   `json:"namespace"`
	Aliases   []string `json:"aliases"`
	Doc       string   `json:"doc"`
	Symbols   []string `json:"symbols"`
}

type arrayType struct {
	Type  string   `json:"type"` // MUST be "array"
	Items avroType `json:"items"`
}

type mapsType struct {
	Type   string   `json:"type"` // MUST be "map"
	Values avroType `json:"values"`
}

type unionType []avroType

type fixedType struct {
	Type      string   `json:"type"` // MUST be "fixed"
	Name      string   `json:"name"`
	Namespace string   `json:"namespace"`
	Aliases   []string `json:"aliases"`
	Size      int64    `json:"size"`
}

type logicalType struct {
	Type        string `json:"type"` // MUST follow spec
	LogicalType string `json:"logicalType"`

	// Decimal logical type specific
	Scale     int64 `json:"scale"`
	Precision int64 `json:"precision"`
}

type definedType string // a type name already defined before

type avroType struct {
	primitiveType *primitiveType
	recordType    *recordType
	enumsType     *enumsType
	arrayType     *arrayType
	mapsType      *mapsType
	unionType     *unionType
	fixedType     *fixedType
	logicalType   *logicalType
	definedType   *definedType
}

func (t *avroType) UnmarshalJSON(b []byte) error {
	var pt primitiveType
	if err := json.Unmarshal(b, &pt); err == nil {
		if IsValidPrimitiveType(pt) {
			t.primitiveType = &pt
			return nil
		}
	}

	var rt recordType
	if err := json.Unmarshal(b, &rt); err == nil {
		if rt.Type != AvroComplexType_Record {
			return ErrInvalidAvroSchema
		}
		t.recordType = &rt
		return nil
	}

	var et enumsType
	if err := json.Unmarshal(b, &et); err == nil {
		if et.Type == AvroComplexType_Enums {
			t.enumsType = &et
			return nil
		}
	}

	var at arrayType
	if err := json.Unmarshal(b, &at); err == nil {
		if at.Type == AvroComplexType_Array {
			t.arrayType = &at
			return nil
		}
	}

	var mt mapsType
	if err := json.Unmarshal(b, &mt); err == nil {
		if mt.Type == AvroComplexType_Maps {
			t.mapsType = &mt
			return nil
		}
	}

	var ut unionType
	if err := json.Unmarshal(b, &ut); err == nil {
		t.unionType = &ut
		return nil
	}

	var ft fixedType
	if err := json.Unmarshal(b, &ft); err == nil {
		if ft.Type == AvroComplexType_Fixed {
			t.fixedType = &ft
			return nil
		}
	}

	var lt logicalType
	if err := json.Unmarshal(b, &lt); err == nil {
		if IsValidLogicalType(lt) {
			t.logicalType = &lt
			return nil
		}
	}

	var dt definedType
	if err := json.Unmarshal(b, &dt); err == nil {
		// NOTE no validation to ensure the type name was defined
		// TODO check the type name is already defined
		t.definedType = &dt
		return nil
	}

	return ErrInvalidAvroSchema
}

func IsValidPrimitiveType(t primitiveType) bool {
	for _, pt := range avroPrimitiveTypes {
		if t == primitiveType(pt) {
			return true
		}
	}

	return false
}

func IsValidLogicalType(t logicalType) bool {
	if types, ok := avroValidTypesForLogicalType[string(t.LogicalType)]; ok {
		for _, tpe := range types {
			if t.Type == tpe {
				return true
			}
		}
	}

	return false
}
