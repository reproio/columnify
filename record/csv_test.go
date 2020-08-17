package record

import (
	"bytes"
	"io"
	"reflect"
	"testing"

	"github.com/apache/arrow/go/arrow"
	"github.com/reproio/columnify/schema"
)

func TestCsvInnerDecoder_Decode(t *testing.T) {
	cases := []struct {
		schema    *schema.IntermediateSchema
		input     []byte
		delimiter delimiter
		expected  []map[string]interface{}
		isErr     bool
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
					"boolean": false,
					"bytes":   string([]byte("foo")),
					"double":  float64(1.1),
					"float":   float64(1.1),
					"int":     int64(1),
					"long":    int64(1),
					"string":  "foo",
				},
				{
					"boolean": true,
					"bytes":   string([]byte("bar")),
					"double":  float64(2.2),
					"float":   float64(2.2),
					"int":     int64(2),
					"long":    int64(2),
					"string":  "bar",
				},
			},
			isErr: false,
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
					"boolean": false,
					"bytes":   string([]byte("foo")),
					"double":  float64(1.1),
					"float":   float64(1.1),
					"int":     int64(1),
					"long":    int64(1),
					"string":  "foo",
				},
				{
					"boolean": true,
					"bytes":   string([]byte("bar")),
					"double":  float64(2.2),
					"float":   float64(2.2),
					"int":     int64(2),
					"long":    int64(2),
					"string":  "bar",
				},
			},
			isErr: false,
		},
	}

	for _, c := range cases {
		buf := bytes.NewReader(c.input)
		d, err := newCsvInnerDecoder(buf, c.schema, c.delimiter)
		if err != nil {
			t.Fatal(err)
		}

		actual := make([]map[string]interface{}, 0)
		for {
			var v map[string]interface{}
			err = d.Decode(&v)
			if err != nil {
				break
			}
			actual = append(actual, v)
		}

		if (err != nil && err != io.EOF) != c.isErr {
			t.Errorf("expected: %v, but actual: %v\n", c.isErr, err)
			continue
		}

		if !reflect.DeepEqual(actual, c.expected) {
			t.Errorf("expected: %v, but actual: %v\n", c.expected, actual)
		}
	}
}
