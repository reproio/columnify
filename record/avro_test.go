package record

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/memory"
	"github.com/reproio/columnify/schema"

	"github.com/linkedin/goavro/v2"
)

func TestFlattenAvroUnion(t *testing.T) {
	input := map[string]interface{}{
		"primitive": 42,
		"nested-single": map[string]interface{}{
			"string": "test",
		},
		"nested-multiple": map[string]interface{}{
			"int":    42,
			"string": "test",
		},
	}
	expected := map[string]interface{}{
		"primitive":     42,
		"nested-single": "test",
		"nested-multiple": map[string]interface{}{
			"int":    42,
			"string": "test",
		},
	}

	actual := flattenAvroUnion(input)

	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("expected: %v, but actual: %v\n", expected, actual)
	}
}

func TestFormatAvroToMap(t *testing.T) {
	cases := []struct {
		input    []byte
		expected []map[string]interface{}
		isErr    bool
	}{
		{
			input: func() []byte {
				w := &bytes.Buffer{}

				r, err := goavro.NewOCFWriter(goavro.OCFConfig{
					W: w,
					Schema: `
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
				})
				if err != nil {
					t.Fatal(err)
				}

				err = r.Append([]map[string]interface{}{
					{
						"boolean": false,
						"bytes":   string([]byte("foo")),
						"double":  1.1,
						"float":   1.1,
						"int":     1,
						"long":    1,
						"string":  "foo",
					},
					{
						"boolean": true,
						"bytes":   string([]byte("bar")),
						"double":  2.2,
						"float":   2.2,
						"int":     2,
						"long":    2,
						"string":  "bar",
					},
				})
				if err != nil {
					t.Fatal(err)
				}

				return w.Bytes()
			}(),
			expected: []map[string]interface{}{
				{
					"boolean": false,
					"bytes":   []byte("foo"),
					"double":  float64(1.1),
					"float":   float32(1.1),
					"int":     int32(1),
					"long":    int64(1),
					"string":  "foo",
				},
				{
					"boolean": true,
					"bytes":   []byte("bar"),
					"double":  float64(2.2),
					"float":   float32(2.2),
					"int":     int32(2),
					"long":    int64(2),
					"string":  "bar",
				},
			},
			isErr: false,
		},

		// Not avro
		{
			input:    []byte("not-valid-avro"),
			expected: nil,
			isErr:    true,
		},
	}

	for _, c := range cases {
		actual, err := FormatAvroToMap(c.input)

		if err != nil != c.isErr {
			t.Errorf("expected: %v, but actual: %v\n", c.isErr, err)
		}

		if !reflect.DeepEqual(actual, c.expected) {
			t.Errorf("expected: %v, but actual: %v\n", c.expected, actual)
		}
	}
}

func TestFormatAvroToArrow(t *testing.T) {
	cases := []struct {
		input    []byte
		schema   *schema.IntermediateSchema
		expected func(s *schema.IntermediateSchema) *WrappedRecord
		isErr    bool
	}{
		{
			input: func() []byte {
				w := &bytes.Buffer{}

				r, err := goavro.NewOCFWriter(goavro.OCFConfig{
					W: w,
					Schema: `
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
				})
				if err != nil {
					t.Fatal(err)
				}

				err = r.Append([]map[string]interface{}{
					{
						"boolean": false,
						"bytes":   string([]byte("foo")),
						"double":  1.1,
						"float":   1.1,
						"int":     1,
						"long":    1,
						"string":  "foo",
					},
					{
						"boolean": true,
						"bytes":   string([]byte("bar")),
						"double":  2.2,
						"float":   2.2,
						"int":     2,
						"long":    2,
						"string":  "bar",
					},
				})
				if err != nil {
					t.Fatal(err)
				}

				return w.Bytes()
			}(),
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
			expected: func(s *schema.IntermediateSchema) *WrappedRecord {
				pool := memory.NewGoAllocator()
				b := array.NewRecordBuilder(pool, s.ArrowSchema)
				defer b.Release()

				b.Field(0).(*array.BooleanBuilder).AppendValues([]bool{false, true}, []bool{true, true})
				b.Field(1).(*array.Uint32Builder).AppendValues([]uint32{1, 2}, []bool{true, true})
				b.Field(2).(*array.Uint64Builder).AppendValues([]uint64{1, 2}, []bool{true, true})
				b.Field(3).(*array.Float32Builder).AppendValues([]float32{1.1, 2.2}, []bool{true, true})
				b.Field(4).(*array.Float64Builder).AppendValues([]float64{1.1, 2.2}, []bool{true, true})
				b.Field(5).(*array.BinaryBuilder).AppendValues([][]byte{[]byte("foo"), []byte("bar")}, []bool{true, true})
				b.Field(6).(*array.StringBuilder).AppendValues([]string{"foo", "bar"}, []bool{true, true})

				return NewWrappedRecord(b)
			},
			isErr: false,
		},

		// Not avro
		{
			input: []byte("not-valid-avro"),
			schema: schema.NewIntermediateSchema(
				arrow.NewSchema([]arrow.Field{}, nil),
				""),
			expected: func(s *schema.IntermediateSchema) *WrappedRecord {
				return nil
			},
			isErr: true,
		},
	}

	for _, c := range cases {
		actual, err := FormatAvroToArrow(c.schema, c.input)

		if err != nil != c.isErr {
			t.Errorf("expected: %v, but actual: %v\n", c.isErr, err)
		}

		expectedRecord := c.expected(c.schema)
		if !reflect.DeepEqual(actual, expectedRecord) {
			t.Errorf("expected: %v, but actual: %v\n", expectedRecord, actual)
		}
	}
}
