package schema

import (
	"testing"

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
	}

	for _, c := range cases {
		actual, err := NewSchemaFromAvroSchema([]byte(c.avroSchema))

		if err != c.err {
			t.Errorf("expected: %v, but actual: %v\n", c.err, err)
		}

		if actual.ArrowSchema.String() != c.expected.String() {
			t.Errorf("expected: %v, but actual: %v\n", c.expected, actual)
		}
	}
}
