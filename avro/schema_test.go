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
