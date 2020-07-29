package record

import (
	"reflect"
	"testing"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/memory"
	"github.com/reproio/columnify/schema"
)

func TestFormatLtsvToMap(t *testing.T) {
	cases := []struct {
		input    []byte
		expected []map[string]interface{}
		isErr    bool
	}{
		// Primitives
		{
			input: []byte(`boolean:false	int:1	long:1	float:1.1	double:1.1	bytes:foo	string:foo
boolean:true	int:2	long:2	float:2.2	double:2.2	bytes:bar	string:bar`),
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

		// Not LTSV
		{
			input:    []byte("not-valid-ltsv"),
			expected: nil,
			isErr:    true,
		},
	}

	for _, c := range cases {
		actual, err := FormatLtsvToMap(c.input)

		if err != nil != c.isErr {
			t.Errorf("expected: %v, but actual: %v\n", c.isErr, err)
		}

		if !reflect.DeepEqual(actual, c.expected) {
			t.Errorf("expected: %v, but actual: %v\n", c.expected, actual)
		}
	}
}

func TestFormatLtsvToArrow(t *testing.T) {
	cases := []struct {
		input    []byte
		schema   *schema.IntermediateSchema
		expected func(s *schema.IntermediateSchema) *WrappedRecord
		isErr    bool
	}{
		// Primitives
		{
			input: []byte(`boolean:false	int:1	long:1	float:1.1	double:1.1	bytes:foo	string:foo
boolean:true	int:2	long:2	float:2.2	double:2.2	bytes:bar	string:bar`),
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

		// Not LTSV
		{
			input: []byte("not-valid-ltsv"),
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
		actual, err := FormatLtsvToArrow(c.schema, c.input)

		if err != nil != c.isErr {
			t.Errorf("expected: %v, but actual: %v\n", c.isErr, err)
		}

		expectedRecord := c.expected(c.schema)
		if !reflect.DeepEqual(actual, expectedRecord) {
			t.Errorf("expected: %v, but actual: %v\n", expectedRecord, actual)
		}
	}
}
