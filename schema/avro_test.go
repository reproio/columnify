package schema

import (
	"errors"
	"reflect"
	"testing"

	"github.com/reproio/columnify/avro"

	"github.com/apache/arrow/go/arrow"
)

func TestNewArrowSchemaFromAvroSchema(t *testing.T) {
	cases := []struct {
		avroSchema string
		expected   *arrow.Schema
		err        error
	}{
		// Only primitives
		{
			avroSchema: `
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
			expected: arrow.NewSchema(
				[]arrow.Field{
					{
						Name:     "boolean",
						Type:     arrow.FixedWidthTypes.Boolean,
						Nullable: false,
					},
					{
						Name:     "int",
						Type:     arrow.PrimitiveTypes.Uint32,
						Nullable: false,
					},
					{
						Name:     "long",
						Type:     arrow.PrimitiveTypes.Uint64,
						Nullable: false,
					},
					{
						Name:     "float",
						Type:     arrow.PrimitiveTypes.Float32,
						Nullable: false,
					},
					{
						Name:     "double",
						Type:     arrow.PrimitiveTypes.Float64,
						Nullable: false,
					},
					{
						Name:     "bytes",
						Type:     arrow.BinaryTypes.Binary,
						Nullable: false,
					},
					{
						Name:     "string",
						Type:     arrow.BinaryTypes.String,
						Nullable: false,
					},
				}, nil,
			),
			err: nil,
		},

		// Nested record
		{
			avroSchema: `
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
			expected: arrow.NewSchema(
				[]arrow.Field{
					{
						Name:     "boolean",
						Type:     arrow.FixedWidthTypes.Boolean,
						Nullable: false,
					},
					{
						Name:     "int",
						Type:     arrow.PrimitiveTypes.Uint32,
						Nullable: false,
					},
					{
						Name:     "long",
						Type:     arrow.PrimitiveTypes.Uint64,
						Nullable: false,
					},
					{
						Name:     "float",
						Type:     arrow.PrimitiveTypes.Float32,
						Nullable: false,
					},
					{
						Name:     "double",
						Type:     arrow.PrimitiveTypes.Float64,
						Nullable: false,
					},
					{
						Name:     "bytes",
						Type:     arrow.BinaryTypes.Binary,
						Nullable: false,
					},
					{
						Name:     "string",
						Type:     arrow.BinaryTypes.String,
						Nullable: false,
					},
					{
						Name: "record",
						Type: arrow.StructOf(
							[]arrow.Field{
								{
									Name:     "boolean",
									Type:     arrow.FixedWidthTypes.Boolean,
									Nullable: false,
								},
								{
									Name:     "int",
									Type:     arrow.PrimitiveTypes.Uint32,
									Nullable: false,
								},
								{
									Name:     "long",
									Type:     arrow.PrimitiveTypes.Uint64,
									Nullable: false,
								},
								{
									Name:     "float",
									Type:     arrow.PrimitiveTypes.Float32,
									Nullable: false,
								},
								{
									Name:     "double",
									Type:     arrow.PrimitiveTypes.Float64,
									Nullable: false,
								},
								{
									Name:     "bytes",
									Type:     arrow.BinaryTypes.Binary,
									Nullable: false,
								},
								{
									Name:     "string",
									Type:     arrow.BinaryTypes.String,
									Nullable: false,
								},
							}...,
						),
						Nullable: false,
					},
				}, nil,
			),
			err: nil,
		},

		// Enum
		{
			avroSchema: `
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
			expected: arrow.NewSchema(
				[]arrow.Field{
					{
						Name:     "enum",
						Type:     arrow.BinaryTypes.String,
						Nullable: false,
					},
				}, nil,
			),
			err: nil,
		},

		// Array
		{
			avroSchema: `
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
			expected: arrow.NewSchema(
				[]arrow.Field{
					{
						Name:     "boolean",
						Type:     arrow.FixedWidthTypes.Boolean,
						Nullable: false,
					},
					{
						Name:     "int",
						Type:     arrow.PrimitiveTypes.Uint32,
						Nullable: false,
					},
					{
						Name:     "long",
						Type:     arrow.PrimitiveTypes.Uint64,
						Nullable: false,
					},
					{
						Name:     "float",
						Type:     arrow.PrimitiveTypes.Float32,
						Nullable: false,
					},
					{
						Name:     "double",
						Type:     arrow.PrimitiveTypes.Float64,
						Nullable: false,
					},
					{
						Name:     "bytes",
						Type:     arrow.BinaryTypes.Binary,
						Nullable: false,
					},
					{
						Name:     "string",
						Type:     arrow.BinaryTypes.String,
						Nullable: false,
					},
					{
						Name: "array",
						Type: arrow.ListOf(
							arrow.StructOf(
								[]arrow.Field{
									{
										Name:     "boolean",
										Type:     arrow.FixedWidthTypes.Boolean,
										Nullable: false,
									},
									{
										Name:     "int",
										Type:     arrow.PrimitiveTypes.Uint32,
										Nullable: false,
									},
									{
										Name:     "long",
										Type:     arrow.PrimitiveTypes.Uint64,
										Nullable: false,
									},
									{
										Name:     "float",
										Type:     arrow.PrimitiveTypes.Float32,
										Nullable: false,
									},
									{
										Name:     "double",
										Type:     arrow.PrimitiveTypes.Float64,
										Nullable: false,
									},
									{
										Name:     "bytes",
										Type:     arrow.BinaryTypes.Binary,
										Nullable: false,
									},
									{
										Name:     "string",
										Type:     arrow.BinaryTypes.String,
										Nullable: false,
									},
								}...,
							)),
						Nullable: false,
					},
				}, nil,
			),
			err: nil,
		},

		// Union
		{
			avroSchema: `
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
			expected: arrow.NewSchema(
				[]arrow.Field{
					{
						Name:     "union",
						Type:     arrow.BinaryTypes.String,
						Nullable: true,
					},
				}, nil,
			),
			err: nil,
		},

		// Fixed
		{
			avroSchema: `
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
			expected: arrow.NewSchema(
				[]arrow.Field{
					{
						Name:     "fixed",
						Type:     arrow.BinaryTypes.Binary,
						Nullable: false,
					},
				}, nil,
			),
			err: nil,
		},

		// Logical Types
		{
			avroSchema: `
{
  "type": "record",
  "name": "LogicalTypes",
  "fields" : [
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
			expected: arrow.NewSchema(
				[]arrow.Field{
					{
						Name:     "date",
						Type:     arrow.FixedWidthTypes.Date32,
						Nullable: false,
					},
					{
						Name:     "time-millis",
						Type:     arrow.FixedWidthTypes.Time32ms,
						Nullable: false,
					},
					{
						Name:     "time-micros",
						Type:     arrow.FixedWidthTypes.Time64us,
						Nullable: false,
					},
					{
						Name:     "timestamp-millis",
						Type:     arrow.FixedWidthTypes.Timestamp_ms,
						Nullable: false,
					},
					{
						Name:     "timestamp-micros",
						Type:     arrow.FixedWidthTypes.Timestamp_us,
						Nullable: false,
					},
				}, nil,
			),
			err: nil,
		},

		// null primitive type
		{
			avroSchema: `
{
  "type": "record",
  "name": "Null",
  "fields" : [
    {"name": "null", "type": "null"}
  ]
}
`,
			expected: &arrow.Schema{},
			err:      ErrUnconvertibleSchema,
		},

		// map complex type
		{
			avroSchema: `
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
			expected: &arrow.Schema{},
			err:      ErrUnconvertibleSchema,
		},

		// decimal logical type
		{
			avroSchema: `
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
    }
  ]
}
`,
			expected: &arrow.Schema{},
			err:      ErrUnconvertibleSchema,
		},
	}

	for _, c := range cases {
		actual, err := NewSchemaFromAvroSchema([]byte(c.avroSchema))

		if !errors.Is(err, c.err) {
			t.Errorf("expected: %v, but actual: %v\n", c.err, err)
		}

		if err == nil && actual.ArrowSchema.String() != c.expected.String() {
			t.Errorf("expected: %v, but actual: %v\n", c.expected, actual)
		}
	}
}

func TestExtractAvroTypeWithNullability(t *testing.T) {
	cases := []struct {
		t                avro.AvroType
		expectedType     avro.AvroType
		expectedNullable bool
	}{
		// required
		{
			t: avro.AvroType{
				PrimitiveType: avro.ToPrimitiveType(avro.AvroPrimitiveType_String),
			},
			expectedType: avro.AvroType{
				PrimitiveType: avro.ToPrimitiveType(avro.AvroPrimitiveType_String),
			},
			expectedNullable: false,
		},

		// nullable
		{
			t: avro.AvroType{
				UnionType: &avro.UnionType{
					avro.AvroType{
						PrimitiveType: avro.ToPrimitiveType(avro.AvroPrimitiveType_Null),
					},
					avro.AvroType{
						PrimitiveType: avro.ToPrimitiveType(avro.AvroPrimitiveType_String),
					},
				},
			},
			expectedType: avro.AvroType{
				PrimitiveType: avro.ToPrimitiveType(avro.AvroPrimitiveType_String),
			},
			expectedNullable: true,
		},
	}

	for _, c := range cases {
		tpe, nullable := extractAvroTypeWithNullability(c.t)

		if !reflect.DeepEqual(c.expectedType, tpe) {
			t.Errorf("expected: %v, but actual: %v", c.expectedType, tpe)
		}

		if c.expectedNullable != nullable {
			t.Errorf("expected: %v, but actual: %v", c.expectedNullable, nullable)
		}
	}
}
