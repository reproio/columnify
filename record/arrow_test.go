package record

import (
	"reflect"
	"testing"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/memory"
	"github.com/reproio/columnify/schema"
)

func TestNewArrowSchemaFromAvroSchema(t *testing.T) {
	cases := []struct {
		input    []map[string]interface{}
		schema   *schema.IntermediateSchema
		expected func(s *schema.IntermediateSchema) *WrappedRecord
		err      error
	}{
		// Primitives
		{
			input: []map[string]interface{}{
				{
					"boolean": false,
					"bytes":   string([]byte("foo")),
					"double":  float64(1.1),
					"float":   float64(1.1),
					"int":     float64(1),
					"long":    float64(1),
					"string":  "foo",
				},
				{
					"boolean": true,
					"bytes":   string([]byte("bar")),
					"double":  float64(2.2),
					"float":   float64(2.2),
					"int":     float64(2),
					"long":    float64(2),
					"string":  "bar",
				},
			},
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

				b.Field(0).(*array.BooleanBuilder).AppendValues([]bool{false, true}, []bool{true, true})
				b.Field(1).(*array.Uint32Builder).AppendValues([]uint32{1, 2}, []bool{true, true})
				b.Field(2).(*array.Uint64Builder).AppendValues([]uint64{1, 2}, []bool{true, true})
				b.Field(3).(*array.Float32Builder).AppendValues([]float32{1.1, 2.2}, []bool{true, true})
				b.Field(4).(*array.Float64Builder).AppendValues([]float64{1.1, 2.2}, []bool{true, true})
				b.Field(5).(*array.BinaryBuilder).AppendValues([][]byte{[]byte("foo"), []byte("bar")}, []bool{true, true})
				b.Field(6).(*array.StringBuilder).AppendValues([]string{"foo", "bar"}, []bool{true, true})

				return NewWrappedRecord(b)
			},
			err: nil,
		},

		// Nested
		{
			input: []map[string]interface{}{
				{
					"boolean": false,
					"bytes":   string([]byte("foo")),
					"double":  float64(1.1),
					"float":   float64(1.1),
					"int":     float64(1),
					"long":    float64(1),
					"string":  "foo",
					"record": map[string]interface{}{
						"boolean": false,
						"bytes":   string([]byte("foo")),
						"double":  float64(1.1),
						"float":   float64(1.1),
						"int":     float64(1),
						"long":    float64(1),
						"string":  "foo",
					},
				},
				{
					"boolean": true,
					"bytes":   string([]byte("bar")),
					"double":  float64(2.2),
					"float":   float64(2.2),
					"int":     float64(2),
					"long":    float64(2),
					"string":  "bar",
					"record": map[string]interface{}{
						"boolean": true,
						"bytes":   string([]byte("bar")),
						"double":  float64(2.2),
						"float":   float64(2.2),
						"int":     float64(2),
						"long":    float64(2),
						"string":  "bar",
					},
				},
			},
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
					},
					nil),
				"nested"),
			expected: func(s *schema.IntermediateSchema) *WrappedRecord {
				pool := memory.NewGoAllocator()
				b := array.NewRecordBuilder(pool, s.ArrowSchema)

				b.Field(0).(*array.BooleanBuilder).AppendValues([]bool{false, true}, []bool{true, true})
				b.Field(1).(*array.Uint32Builder).AppendValues([]uint32{1, 2}, []bool{true, true})
				b.Field(2).(*array.Uint64Builder).AppendValues([]uint64{1, 2}, []bool{true, true})
				b.Field(3).(*array.Float32Builder).AppendValues([]float32{1.1, 2.2}, []bool{true, true})
				b.Field(4).(*array.Float64Builder).AppendValues([]float64{1.1, 2.2}, []bool{true, true})
				b.Field(5).(*array.BinaryBuilder).AppendValues([][]byte{[]byte("foo"), []byte("bar")}, []bool{true, true})
				b.Field(6).(*array.StringBuilder).AppendValues([]string{"foo", "bar"}, []bool{true, true})
				sb := b.Field(7).(*array.StructBuilder)
				sb.AppendValues([]bool{true, true})
				sb.FieldBuilder(0).(*array.BooleanBuilder).AppendValues([]bool{false, true}, []bool{true, true})
				sb.FieldBuilder(1).(*array.Uint32Builder).AppendValues([]uint32{1, 2}, []bool{true, true})
				sb.FieldBuilder(2).(*array.Uint64Builder).AppendValues([]uint64{1, 2}, []bool{true, true})
				sb.FieldBuilder(3).(*array.Float32Builder).AppendValues([]float32{1.1, 2.2}, []bool{true, true})
				sb.FieldBuilder(4).(*array.Float64Builder).AppendValues([]float64{1.1, 2.2}, []bool{true, true})
				sb.FieldBuilder(5).(*array.BinaryBuilder).AppendValues([][]byte{[]byte("foo"), []byte("bar")}, []bool{true, true})
				sb.FieldBuilder(6).(*array.StringBuilder).AppendValues([]string{"foo", "bar"}, []bool{true, true})

				return NewWrappedRecord(b)
			},
			err: nil,
		},

		// Array
		{
			input: []map[string]interface{}{
				{
					"boolean": false,
					"bytes":   string([]byte("foo")),
					"double":  float64(1.1),
					"float":   float64(1.1),
					"int":     float64(1),
					"long":    float64(1),
					"string":  "foo",
					"array": []interface{}{
						map[string]interface{}{
							"boolean": false,
							"bytes":   string([]byte("foo")),
							"double":  float64(1.1),
							"float":   float64(1.1),
							"int":     float64(1),
							"long":    float64(1),
							"string":  "foo",
						},
						map[string]interface{}{
							"boolean": true,
							"bytes":   string([]byte("bar")),
							"double":  float64(2.2),
							"float":   float64(2.2),
							"int":     float64(2),
							"long":    float64(2),
							"string":  "bar",
						},
					},
				},
				{
					"boolean": true,
					"bytes":   string([]byte("bar")),
					"double":  float64(2.2),
					"float":   float64(2.2),
					"int":     float64(2),
					"long":    float64(2),
					"string":  "bar",
					"array": []interface{}{
						map[string]interface{}{
							"boolean": false,
							"bytes":   string([]byte("foo")),
							"double":  float64(1.1),
							"float":   float64(1.1),
							"int":     float64(1),
							"long":    float64(1),
							"string":  "foo",
						},
						map[string]interface{}{
							"boolean": true,
							"bytes":   string([]byte("bar")),
							"double":  float64(2.2),
							"float":   float64(2.2),
							"int":     float64(2),
							"long":    float64(2),
							"string":  "bar",
						},
					},
				},
			},
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
					}, nil),
				"arrays"),
			expected: func(s *schema.IntermediateSchema) *WrappedRecord {
				pool := memory.NewGoAllocator()
				b := array.NewRecordBuilder(pool, s.ArrowSchema)

				b.Field(0).(*array.BooleanBuilder).AppendValues([]bool{false, true}, []bool{true, true})
				b.Field(1).(*array.Uint32Builder).AppendValues([]uint32{1, 2}, []bool{true, true})
				b.Field(2).(*array.Uint64Builder).AppendValues([]uint64{1, 2}, []bool{true, true})
				b.Field(3).(*array.Float32Builder).AppendValues([]float32{1.1, 2.2}, []bool{true, true})
				b.Field(4).(*array.Float64Builder).AppendValues([]float64{1.1, 2.2}, []bool{true, true})
				b.Field(5).(*array.BinaryBuilder).AppendValues([][]byte{[]byte("foo"), []byte("bar")}, []bool{true, true})
				b.Field(6).(*array.StringBuilder).AppendValues([]string{"foo", "bar"}, []bool{true, true})
				lb := b.Field(7).(*array.ListBuilder)
				sb := lb.ValueBuilder().(*array.StructBuilder)
				lb.Append(true)
				sb.AppendValues([]bool{true, true})
				sb.FieldBuilder(0).(*array.BooleanBuilder).AppendValues([]bool{false, true}, []bool{true, true})
				sb.FieldBuilder(1).(*array.Uint32Builder).AppendValues([]uint32{1, 2}, []bool{true, true})
				sb.FieldBuilder(2).(*array.Uint64Builder).AppendValues([]uint64{1, 2}, []bool{true, true})
				sb.FieldBuilder(3).(*array.Float32Builder).AppendValues([]float32{1.1, 2.2}, []bool{true, true})
				sb.FieldBuilder(4).(*array.Float64Builder).AppendValues([]float64{1.1, 2.2}, []bool{true, true})
				sb.FieldBuilder(5).(*array.BinaryBuilder).AppendValues([][]byte{[]byte("foo"), []byte("bar")}, []bool{true, true})
				sb.FieldBuilder(6).(*array.StringBuilder).AppendValues([]string{"foo", "bar"}, []bool{true, true})
				lb.Append(true)
				sb.AppendValues([]bool{true, true})
				sb.FieldBuilder(0).(*array.BooleanBuilder).AppendValues([]bool{false, true}, []bool{true, true})
				sb.FieldBuilder(1).(*array.Uint32Builder).AppendValues([]uint32{1, 2}, []bool{true, true})
				sb.FieldBuilder(2).(*array.Uint64Builder).AppendValues([]uint64{1, 2}, []bool{true, true})
				sb.FieldBuilder(3).(*array.Float32Builder).AppendValues([]float32{1.1, 2.2}, []bool{true, true})
				sb.FieldBuilder(4).(*array.Float64Builder).AppendValues([]float64{1.1, 2.2}, []bool{true, true})
				sb.FieldBuilder(5).(*array.BinaryBuilder).AppendValues([][]byte{[]byte("foo"), []byte("bar")}, []bool{true, true})
				sb.FieldBuilder(6).(*array.StringBuilder).AppendValues([]string{"foo", "bar"}, []bool{true, true})

				return NewWrappedRecord(b)
			},
			err: nil,
		},
	}

	for _, c := range cases {
		expectedRecord := c.expected(c.schema)

		actual, err := formatMapToArrowRecord(c.schema.ArrowSchema, c.input)

		if err != c.err {
			t.Errorf("expected: %v, but actual: %v\n", c.err, err)
		}

		if !reflect.DeepEqual(actual, expectedRecord) {
			t.Errorf("values:  expected: %v, but actual: %v\n", expectedRecord, actual)
		}
	}
}
