package record

import (
	"reflect"
	"testing"

	"github.com/apache/arrow/go/arrow"
	"github.com/reproio/columnify/schema"
)

func TestFormatCsvToMap(t *testing.T) {
	cases := []struct {
		schema    *schema.IntermediateSchema
		input     []byte
		delimiter delimiter
		expected  []map[string]interface{}
		err       error
	}{
		// csv; Primitives
		{
			schema: schema.NewIntermediateSchema(
				arrow.NewSchema(
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
					}, nil),
				"primitives"),
			input: []byte(`false,1,1,1.1,1.1,"foo","foo"
true,2,2,2.2,2.2,"bar","bar"`),
			delimiter: CsvDelimiter,
			expected: []map[string]interface{}{
				{
					"boolean": "false",
					"bytes":   string([]byte("foo")),
					"double":  "1.1", // TODO cast to actual types
					"float":   "1.1",
					"int":     "1",
					"long":    "1",
					"string":  "foo",
				},
				{
					"boolean": "true",
					"bytes":   string([]byte("bar")),
					"double":  "2.2",
					"float":   "2.2",
					"int":     "2",
					"long":    "2",
					"string":  "bar",
				},
			},
			err: nil,
		},

		// tsv; Primitives
		{
			schema: schema.NewIntermediateSchema(
				arrow.NewSchema(
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
					}, nil),
				"primitives"),
			input: []byte(`false	1	1	1.1	1.1	foo	foo
true	2	2	2.2	2.2	bar	bar`),
			delimiter: TsvDelimiter,
			expected: []map[string]interface{}{
				{
					"boolean": "false",
					"bytes":   string([]byte("foo")),
					"double":  "1.1", // TODO cast to actual types
					"float":   "1.1",
					"int":     "1",
					"long":    "1",
					"string":  "foo",
				},
				{
					"boolean": "true",
					"bytes":   string([]byte("bar")),
					"double":  "2.2",
					"float":   "2.2",
					"int":     "2",
					"long":    "2",
					"string":  "bar",
				},
			},
			err: nil,
		},
	}

	for _, c := range cases {
		actual, err := FormatCsvToMap(c.schema, c.input, c.delimiter)

		if err != c.err {
			t.Errorf("expected: %v, but actual: %v\n", c.err, err)
		}

		if !reflect.DeepEqual(actual, c.expected) {
			t.Errorf("expected: %v, but actual: %v\n", c.expected, actual)
		}
	}
}
