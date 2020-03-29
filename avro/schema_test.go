package avro

import (
	"encoding/json"
	"testing"
)

func areSameRecordType(l, r RecordType) bool {
	ll, err := json.Marshal(l)
	if err != nil {
		return false
	}

	rr, err := json.Marshal(r)
	if err != nil {
		return false
	}

	return string(ll) == string(rr)
}

func TestUnmarshal(t *testing.T) {
	cases := []struct {
		schema   string
		expected RecordType
		err      error
	}{
		// Only primitives
		{
			schema: `
{
  "type": "record",
  "name": "Primitives",
  "fields" : [
    {"name": "boolean", "type": "boolean"},
    {"name": "int",     "type": "int"},
    {"name": "long",    "type": "long"},
    {"name": "float",   "type": "float"},
    {"name": "double",  "type": "double"},
    {"name": "bytes",   "type": "bytes"},
    {"name": "string",  "type": "string"}
  ]
}
`,
			expected: RecordType{
				Type: AvroComplexType_Record,
				Name: "Primitives",
				Fields: []RecordField{
					{
						Name: "boolean",
						Type: AvroType{
							PrimitiveType: ToPrimitiveType("boolean"),
						},
					},
					{
						Name: "int",
						Type: AvroType{
							PrimitiveType: ToPrimitiveType("int"),
						},
					},
					{
						Name: "long",
						Type: AvroType{
							PrimitiveType: ToPrimitiveType("long"),
						},
					},
					{
						Name: "float",
						Type: AvroType{
							PrimitiveType: ToPrimitiveType("float"),
						},
					},
					{
						Name: "double",
						Type: AvroType{
							PrimitiveType: ToPrimitiveType("double"),
						},
					},
					{
						Name: "bytes",
						Type: AvroType{
							PrimitiveType: ToPrimitiveType("bytes"),
						},
					},
					{
						Name: "string",
						Type: AvroType{
							PrimitiveType: ToPrimitiveType("string"),
						},
					},
				},
			},
			err: nil,
		},

		// Nested record
		{
			schema: `
{
  "type": "record",
  "name": "Nested",
  "fields" : [
    {"name": "boolean", "type": "boolean"},
    {"name": "int",     "type": "int"},
    {"name": "long",    "type": "long"},
    {"name": "float",   "type": "float"},
    {"name": "double",  "type": "double"},
    {"name": "bytes",   "type": "bytes"},
    {"name": "string",  "type": "string"},
    {"name": "record",  "type": {
      "type": "record",
      "name": "Level1",
      "fields" : [
        {"name": "boolean", "type": "boolean"},
        {"name": "int",     "type": "int"},
        {"name": "long",    "type": "long"},
        {"name": "float",   "type": "float"},
        {"name": "double",  "type": "double"},
        {"name": "bytes",   "type": "bytes"},
        {"name": "string",  "type": "string"}
      ]}
    }
  ]
}
`,
			expected: RecordType{
				Type: AvroComplexType_Record,
				Name: "Nested",
				Fields: []RecordField{
					{
						Name: "boolean",
						Type: AvroType{
							PrimitiveType: ToPrimitiveType("boolean"),
						},
					},
					{
						Name: "int",
						Type: AvroType{
							PrimitiveType: ToPrimitiveType("int"),
						},
					},
					{
						Name: "long",
						Type: AvroType{
							PrimitiveType: ToPrimitiveType("long"),
						},
					},
					{
						Name: "float",
						Type: AvroType{
							PrimitiveType: ToPrimitiveType("float"),
						},
					},
					{
						Name: "double",
						Type: AvroType{
							PrimitiveType: ToPrimitiveType("double"),
						},
					},
					{
						Name: "bytes",
						Type: AvroType{
							PrimitiveType: ToPrimitiveType("bytes"),
						},
					},
					{
						Name: "string",
						Type: AvroType{
							PrimitiveType: ToPrimitiveType("string"),
						},
					},
					{
						Name: "record",
						Type: AvroType{
							RecordType: &RecordType{
								Type: AvroComplexType_Record,
								Name: "Level1",
								Fields: []RecordField{
									{
										Name: "boolean",
										Type: AvroType{
											PrimitiveType: ToPrimitiveType("boolean"),
										},
									},
									{
										Name: "int",
										Type: AvroType{
											PrimitiveType: ToPrimitiveType("int"),
										},
									},
									{
										Name: "long",
										Type: AvroType{
											PrimitiveType: ToPrimitiveType("long"),
										},
									},
									{
										Name: "float",
										Type: AvroType{
											PrimitiveType: ToPrimitiveType("float"),
										},
									},
									{
										Name: "double",
										Type: AvroType{
											PrimitiveType: ToPrimitiveType("double"),
										},
									},
									{
										Name: "bytes",
										Type: AvroType{
											PrimitiveType: ToPrimitiveType("bytes"),
										},
									},
									{
										Name: "string",
										Type: AvroType{
											PrimitiveType: ToPrimitiveType("string"),
										},
									},
								},
							},
						},
					},
				},
			},
			err: nil,
		},

		// Enum
		{
			schema: `
{
  "type": "record",
  "name": "Enum",
  "fields" : [
    {
      "name": "enum",
      "type": {
        "name": "enum",
        "type": "enum",
        "namespace": "enum",
        "aliases": ["alias"],
        "symbols": ["ZERO", "ONE", "TWO"]
      }
    }
  ]
}
`,
			expected: RecordType{
				Type: AvroComplexType_Record,
				Name: "Enum",
				Fields: []RecordField{
					{
						Name: "enum",
						Type: AvroType{
							EnumsType: &EnumsType{
								Name:      "enum",
								Namespace: "enum",
								Type:      AvroComplexType_Enums,
								Aliases:   []string{"alias"},
								Symbols:   []string{"ZERO", "ONE", "TWO"},
							},
						},
					},
				},
			},
			err: nil,
		},

		// Array
		{
			schema: `
{
  "type": "record",
  "name": "Array",
  "fields" : [
    {"name": "boolean", "type": "boolean"},
    {"name": "int",     "type": "int"},
    {"name": "long",    "type": "long"},
    {"name": "float",   "type": "float"},
    {"name": "double",  "type": "double"},
    {"name": "bytes",   "type": "bytes"},
    {"name": "string",  "type": "string"},
    {"name": "array",   "type": {
      "type": "array",
      "items": {
        "type": "record",
        "name": "Level1",
        "fields" : [
          {"name": "boolean", "type": "boolean"},
          {"name": "int",     "type": "int"},
          {"name": "long",    "type": "long"},
          {"name": "float",   "type": "float"},
          {"name": "double",  "type": "double"},
          {"name": "bytes",   "type": "bytes"},
          {"name": "string",  "type": "string"}
        ]
      }}
    }
  ]
}
`,
			expected: RecordType{
				Type: AvroComplexType_Record,
				Name: "Array",
				Fields: []RecordField{
					{
						Name: "boolean",
						Type: AvroType{
							PrimitiveType: ToPrimitiveType("boolean"),
						},
					},
					{
						Name: "int",
						Type: AvroType{
							PrimitiveType: ToPrimitiveType("int"),
						},
					},
					{
						Name: "long",
						Type: AvroType{
							PrimitiveType: ToPrimitiveType("long"),
						},
					},
					{
						Name: "float",
						Type: AvroType{
							PrimitiveType: ToPrimitiveType("float"),
						},
					},
					{
						Name: "double",
						Type: AvroType{
							PrimitiveType: ToPrimitiveType("double"),
						},
					},
					{
						Name: "bytes",
						Type: AvroType{
							PrimitiveType: ToPrimitiveType("bytes"),
						},
					},
					{
						Name: "string",
						Type: AvroType{
							PrimitiveType: ToPrimitiveType("string"),
						},
					},
					{
						Name: "array",
						Type: AvroType{
							ArrayType: &ArrayType{
								Type: AvroComplexType_Array,
								Items: AvroType{
									RecordType: &RecordType{
										Type: AvroComplexType_Record,
										Name: "Level1",
										Fields: []RecordField{
											{
												Name: "boolean",
												Type: AvroType{
													PrimitiveType: ToPrimitiveType("boolean"),
												},
											},
											{
												Name: "int",
												Type: AvroType{
													PrimitiveType: ToPrimitiveType("int"),
												},
											},
											{
												Name: "long",
												Type: AvroType{
													PrimitiveType: ToPrimitiveType("long"),
												},
											},
											{
												Name: "float",
												Type: AvroType{
													PrimitiveType: ToPrimitiveType("float"),
												},
											},
											{
												Name: "double",
												Type: AvroType{
													PrimitiveType: ToPrimitiveType("double"),
												},
											},
											{
												Name: "bytes",
												Type: AvroType{
													PrimitiveType: ToPrimitiveType("bytes"),
												},
											},
											{
												Name: "string",
												Type: AvroType{
													PrimitiveType: ToPrimitiveType("string"),
												},
											},
										},
									},
								},
							},
						},
					},
				},
			},
			err: nil,
		},

		// Map
		{
			schema: `
{
  "type": "record",
  "name": "Map",
  "fields" : [
    {
      "name": "map",
      "type": {
        "type": "map",
        "values": "long"
      }
    }
  ]
}
`,
			expected: RecordType{
				Type: AvroComplexType_Record,
				Name: "Map",
				Fields: []RecordField{
					{
						Name: "map",
						Type: AvroType{
							MapsType: &MapsType{
								Type: AvroComplexType_Maps,
								Values: AvroType{
									PrimitiveType: ToPrimitiveType("long"),
								},
							},
						},
					},
				},
			},
			err: nil,
		},

		// Union
		{
			schema: `
{
  "type": "record",
  "name": "Union",
  "fields" : [
    {
      "name": "union",
      "type": ["null", "string"]
    }
  ]
}
`,
			expected: RecordType{
				Type: AvroComplexType_Record,
				Name: "Union",
				Fields: []RecordField{
					{
						Name: "union",
						Type: AvroType{
							UnionType: &UnionType{
								{
									PrimitiveType: ToPrimitiveType("null"),
								},
								{
									PrimitiveType: ToPrimitiveType("string"),
								},
							},
						},
					},
				},
			},
			err: nil,
		},

		// Fixed
		{
			schema: `
{
  "type": "record",
  "name": "Fixed",
  "fields" : [
    {
      "name": "fixed",
      "type": {
        "type": "fixed",
        "name": "fixed",
        "namespace": "fixed",
        "aliases": ["alias"],
        "size": 16
      }
    }
  ]
}
`,
			expected: RecordType{
				Type: AvroComplexType_Record,
				Name: "Fixed",
				Fields: []RecordField{
					{
						Name: "fixed",
						Type: AvroType{
							FixedType: &FixedType{
								Type:      AvroComplexType_Fixed,
								Name:      "fixed",
								Namespace: "fixed",
								Aliases:   []string{"alias"},
								Size:      16,
							},
						},
					},
				},
			},
			err: nil,
		},

		// Logical Types
		{
			schema: `
{
  "type": "record",
  "name": "LogicalTypes",
  "fields" : [
    {
      "name": "decimal",
      "type": {
        "type": "bytes",
        "logicalType": "decimal",
        "precision": 4,
        "scale": 2
      }
    },
    {
      "name": "date",
      "type": {
        "type": "int",
        "logicalType": "date"
      }
    },
    {
      "name": "time-millis",
      "type": {
        "type": "int",
        "logicalType": "time-millis"
      }
    },
    {
      "name": "time-micros",
      "type": {
        "type": "long",
        "logicalType": "time-micros"
      }
    },
    {
      "name": "timestamp-millis",
      "type": {
        "type": "long",
        "logicalType": "timestamp-millis"
      }
    },
    {
      "name": "timestamp-micros",
      "type": {
        "type": "long",
        "logicalType": "timestamp-micros"
      }
    }
  ]
}
`,
			expected: RecordType{
				Type: AvroComplexType_Record,
				Name: "LogicalTypes",
				Fields: []RecordField{
					{
						Name: "decimal",
						Type: AvroType{
							LogicalType: &LogicalType{
								Type:        AvroPrimitiveType_Bytes,
								LogicalType: AvroLogicalType_Decimal,
								Precision:   4,
								Scale:       2,
							},
						},
					},
					{
						Name: "date",
						Type: AvroType{
							LogicalType: &LogicalType{
								Type:        AvroPrimitiveType_Int,
								LogicalType: AvroLogicalType_Date,
							},
						},
					},
					{
						Name: "time-millis",
						Type: AvroType{
							LogicalType: &LogicalType{
								Type:        AvroPrimitiveType_Int,
								LogicalType: AvroLogicalType_TimeMillis,
							},
						},
					},
					{
						Name: "time-micros",
						Type: AvroType{
							LogicalType: &LogicalType{
								Type:        AvroPrimitiveType_Long,
								LogicalType: AvroLogicalType_TimeMicros,
							},
						},
					},
					{
						Name: "timestamp-millis",
						Type: AvroType{
							LogicalType: &LogicalType{
								Type:        AvroPrimitiveType_Long,
								LogicalType: AvroLogicalType_TimestampMillis,
							},
						},
					},
					{
						Name: "timestamp-micros",
						Type: AvroType{
							LogicalType: &LogicalType{
								Type:        AvroPrimitiveType_Long,
								LogicalType: AvroLogicalType_TimestampMicros,
							},
						},
					},
					// TODO duration
				},
			},
			err: nil,
		},
	}

	for _, c := range cases {
		var actual RecordType
		err := json.Unmarshal([]byte(c.schema), &actual)

		if err != c.err {
			t.Errorf("expected: %v, but actual: %v\n", c.err, err)
		}

		if !areSameRecordType(actual, c.expected) {
			t.Errorf("expected: %v, but actual: %v\n", c.expected, actual)
		}
	}
}
