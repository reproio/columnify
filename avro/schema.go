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

type PrimitiveType string

type RecordType struct {
	Type      string        `json:"type"` // MUST be "record"
	Name      string        `json:"name"`
	Namespace string        `json:"namespace"`
	Doc       string        `json:"doc"`
	Aliases   []string      `json:"aliases"`
	Fields    []RecordField `json:"fields"`
}

type RecordField struct {
	Name    string   `json:"name"`
	Doc     string   `json:"doc"`
	Type    AvroType `json:"type"`
	Default string   `json:"default"`
	Order   string   `json:"order"`
	Aliases []string `json:"aliases"`
}

type EnumsType struct {
	Type      string   `json:"type"` // MUST be "enum"
	Name      string   `json:"name"`
	Namespace string   `json:"namespace"`
	Aliases   []string `json:"aliases"`
	Doc       string   `json:"doc"`
	Symbols   []string `json:"symbols"`
}

type ArrayType struct {
	Type  string   `json:"type"` // MUST be "array"
	Items AvroType `json:"items"`
}

type MapsType struct {
	Type   string   `json:"type"` // MUST be "map"
	Values AvroType `json:"values"`
}

type UnionType []AvroType

type FixedType struct {
	Type      string   `json:"type"` // MUST be "fixed"
	Name      string   `json:"name"`
	Namespace string   `json:"namespace"`
	Aliases   []string `json:"aliases"`
	Size      int64    `json:"size"`
}

type LogicalType struct {
	Type        string `json:"type"` // MUST follow spec
	LogicalType string `json:"logicalType"`

	// Decimal logical type specific
	Scale     int64 `json:"scale"`
	Precision int64 `json:"precision"`
}

type DefinedType string // a type name already defined before

type AvroType struct {
	PrimitiveType *PrimitiveType
	RecordType    *RecordType
	EnumsType     *EnumsType
	ArrayType     *ArrayType
	MapsType      *MapsType
	UnionType     *UnionType
	FixedType     *FixedType
	LogicalType   *LogicalType
	DefinedType   *DefinedType
}

func (t *AvroType) UnmarshalJSON(b []byte) error {
	var pt PrimitiveType
	if err := json.Unmarshal(b, &pt); err == nil {
		if IsValidPrimitiveType(pt) {
			t.PrimitiveType = &pt
			return nil
		}
	}

	var rt RecordType
	if err := json.Unmarshal(b, &rt); err == nil {
		if rt.Type == AvroComplexType_Record {
			t.RecordType = &rt
			return nil
		}
	}

	var et EnumsType
	if err := json.Unmarshal(b, &et); err == nil {
		if et.Type == AvroComplexType_Enums {
			t.EnumsType = &et
			return nil
		}
	}

	var at ArrayType
	if err := json.Unmarshal(b, &at); err == nil {
		if at.Type == AvroComplexType_Array {
			t.ArrayType = &at
			return nil
		}
	}

	var mt MapsType
	if err := json.Unmarshal(b, &mt); err == nil {
		if mt.Type == AvroComplexType_Maps {
			t.MapsType = &mt
			return nil
		}
	}

	var ut UnionType
	if err := json.Unmarshal(b, &ut); err == nil {
		t.UnionType = &ut
		return nil
	}

	var ft FixedType
	if err := json.Unmarshal(b, &ft); err == nil {
		if ft.Type == AvroComplexType_Fixed {
			t.FixedType = &ft
			return nil
		}
	}

	var lt LogicalType
	if err := json.Unmarshal(b, &lt); err == nil {
		if IsValidLogicalType(lt) {
			t.LogicalType = &lt
			return nil
		}
	}

	var dt DefinedType
	if err := json.Unmarshal(b, &dt); err == nil {
		// NOTE no validation to ensure the type name was defined
		// TODO check the type name is already defined
		t.DefinedType = &dt
		return nil
	}

	return fmt.Errorf("invalid avro schema because of unexpected data: %v", b)
}

func IsValidPrimitiveType(t PrimitiveType) bool {
	for _, pt := range avroPrimitiveTypes {
		if t == PrimitiveType(pt) {
			return true
		}
	}

	return false
}

func IsValidLogicalType(t LogicalType) bool {
	if types, ok := avroValidTypesForLogicalType[string(t.LogicalType)]; ok {
		for _, tpe := range types {
			if t.Type == tpe {
				return true
			}
		}
	}

	return false
}

func ToPrimitiveType(v string) *PrimitiveType {
	vv := PrimitiveType(v)
	return &vv
}

func ToDefinedType(v string) *DefinedType {
	vv := DefinedType(v)
	return &vv
}
