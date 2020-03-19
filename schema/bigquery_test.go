package schema

import (
	"github.com/apache/arrow/go/arrow"
	"testing"
)

func TestNewArrowSchemaFromBigquerySchema(t *testing.T) {
	cases := []struct {
		bqSchema string
		expected *arrow.Schema
		err      error
	}{
		// Only primitives
		{
			bqSchema: `
[
  {
    "name": "boolean",
    "type": "BOOLEAN",
    "mode": "REQUIRED"
  },
  {
    "name": "int",
    "type": "INTEGER",
    "mode": "REQUIRED"
  },
  {
    "name": "long",
    "type": "INTEGER",
    "mode": "REQUIRED"
  },
  {
    "name": "float",
    "type": "FLOAT",
    "mode": "REQUIRED"
  },
  {
    "name": "double",
    "type": "FLOAT",
    "mode": "REQUIRED"
  },
  {
    "name": "bytes",
    "type": "BYTES",
    "mode": "REQUIRED"
  },
  {
    "name": "string",
    "type": "STRING",
    "mode": "REQUIRED"
  }
]
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
						Type:     arrow.PrimitiveTypes.Uint64,
						Nullable: false,
					},
					{
						Name:     "long",
						Type:     arrow.PrimitiveTypes.Uint64,
						Nullable: false,
					},
					{
						Name:     "float",
						Type:     arrow.PrimitiveTypes.Float64,
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
			bqSchema: `
[
  {
    "name": "boolean",
    "type": "BOOLEAN",
    "mode": "REQUIRED"
  },
  {
    "name": "int",
    "type": "INTEGER",
    "mode": "REQUIRED"
  },
  {
    "name": "long",
    "type": "INTEGER",
    "mode": "REQUIRED"
  },
  {
    "name": "float",
    "type": "FLOAT",
    "mode": "REQUIRED"
  },
  {
    "name": "double",
    "type": "FLOAT",
    "mode": "REQUIRED"
  },
  {
    "name": "bytes",
    "type": "BYTES",
    "mode": "REQUIRED"
  },
  {
    "name": "string",
    "type": "STRING",
    "mode": "REQUIRED"
  },
  {
    "name":   "record",
    "type":   "RECORD",
    "mode":   "REQUIRED",
    "fields": [
      {
        "name": "boolean",
        "type": "BOOLEAN",
        "mode": "REQUIRED"
      },
      {
        "name": "int",
        "type": "INTEGER",
        "mode": "REQUIRED"
      },
      {
        "name": "long",
        "type": "INTEGER",
        "mode": "REQUIRED"
      },
      {
        "name": "float",
        "type": "FLOAT",
        "mode": "REQUIRED"
      },
      {
        "name": "double",
        "type": "FLOAT",
        "mode": "REQUIRED"
      },
      {
        "name": "bytes",
        "type": "BYTES",
        "mode": "REQUIRED"
      },
      {
        "name": "string",
        "type": "STRING",
        "mode": "REQUIRED"
      }
    ]
  }
]
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
						Type:     arrow.PrimitiveTypes.Uint64,
						Nullable: false,
					},
					{
						Name:     "long",
						Type:     arrow.PrimitiveTypes.Uint64,
						Nullable: false,
					},
					{
						Name:     "float",
						Type:     arrow.PrimitiveTypes.Float64,
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
									Type:     arrow.PrimitiveTypes.Uint64,
									Nullable: false,
								},
								{
									Name:     "long",
									Type:     arrow.PrimitiveTypes.Uint64,
									Nullable: false,
								},
								{
									Name:     "float",
									Type:     arrow.PrimitiveTypes.Float64,
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
			bqSchema: `
[
  {
    "name": "boolean",
    "type": "BOOLEAN",
    "mode": "REQUIRED"
  },
  {
    "name": "int",
    "type": "INTEGER",
    "mode": "REQUIRED"
  },
  {
    "name": "long",
    "type": "INTEGER",
    "mode": "REQUIRED"
  },
  {
    "name": "float",
    "type": "FLOAT",
    "mode": "REQUIRED"
  },
  {
    "name": "double",
    "type": "FLOAT",
    "mode": "REQUIRED"
  },
  {
    "name": "bytes",
    "type": "BYTES",
    "mode": "REQUIRED"
  },
  {
    "name": "string",
    "type": "STRING",
    "mode": "REQUIRED"
  },
  {
    "name":   "array",
    "type":   "RECORD",
    "mode":   "REPEATED",
    "fields": [
      {
        "name": "boolean",
        "type": "BOOLEAN",
        "mode": "REQUIRED"
      },
      {
        "name": "int",
        "type": "INTEGER",
        "mode": "REQUIRED"
      },
      {
        "name": "long",
        "type": "INTEGER",
        "mode": "REQUIRED"
      },
      {
        "name": "float",
        "type": "FLOAT",
        "mode": "REQUIRED"
      },
      {
        "name": "double",
        "type": "FLOAT",
        "mode": "REQUIRED"
      },
      {
        "name": "bytes",
        "type": "BYTES",
        "mode": "REQUIRED"
      },
      {
        "name": "string",
        "type": "STRING",
        "mode": "REQUIRED"
      }
    ]
  }
]
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
						Type:     arrow.PrimitiveTypes.Uint64,
						Nullable: false,
					},
					{
						Name:     "long",
						Type:     arrow.PrimitiveTypes.Uint64,
						Nullable: false,
					},
					{
						Name:     "float",
						Type:     arrow.PrimitiveTypes.Float64,
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
										Type:     arrow.PrimitiveTypes.Uint64,
										Nullable: false,
									},
									{
										Name:     "long",
										Type:     arrow.PrimitiveTypes.Uint64,
										Nullable: false,
									},
									{
										Name:     "float",
										Type:     arrow.PrimitiveTypes.Float64,
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
						),
						Nullable: true,
					},
				}, nil,
			),
			err: nil,
		},
	}

	for _, c := range cases {
		actual, err := NewSchemaFromBigQuerySchema([]byte(c.bqSchema))

		if err != c.err {
			t.Errorf("expected: %v, but actual: %v\n", c.err, err)
		}

		if actual.ArrowSchema.String() != c.expected.String() {
			t.Errorf("expected: %v, but actual: %v\n", c.expected, actual)
		}
	}
}
