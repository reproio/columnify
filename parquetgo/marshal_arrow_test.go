package parquetgo

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/reproio/columnify/record"
	"github.com/reproio/columnify/schema"
	"github.com/xitongsys/parquet-go/layout"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/memory"
)

func TestNewArrowSchemaFromAvroSchema(t *testing.T) {
	cases := []struct {
		input  func(s *schema.IntermediateSchema) []interface{}
		schema *schema.IntermediateSchema
		expect *map[string]*layout.Table
		err    error
	}{
		// Only primitives
		{
			input: func(s *schema.IntermediateSchema) []interface{} {
				pool := memory.NewGoAllocator()
				b := array.NewRecordBuilder(pool, s.ArrowSchema)

				b.Field(0).(*array.BooleanBuilder).AppendValues([]bool{false, true}, []bool{true, true})
				b.Field(1).(*array.Uint32Builder).AppendValues([]uint32{1, 2}, []bool{true, true})
				b.Field(2).(*array.Uint64Builder).AppendValues([]uint64{1, 2}, []bool{true, true})
				b.Field(3).(*array.Float32Builder).AppendValues([]float32{1.1, 2.2}, []bool{true, true})
				b.Field(4).(*array.Float64Builder).AppendValues([]float64{1.1, 2.2}, []bool{true, true})
				b.Field(5).(*array.BinaryBuilder).AppendValues([][]byte{[]byte("foo"), []byte("bar")}, []bool{true, true})
				b.Field(6).(*array.StringBuilder).AppendValues([]string{"foo", "bar"}, []bool{true, true})

				return []interface{}{record.NewWrappedRecord(b)}
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
			expect: &map[string]*layout.Table{
				"Primitives.Boolean": {
					Values:           []interface{}{false, true},
					DefinitionLevels: []int32{0, 0},
					RepetitionLevels: []int32{0, 0},
				},
				"Primitives.Int": {
					Values:           []interface{}{int32(1), int32(2)},
					DefinitionLevels: []int32{0, 0},
					RepetitionLevels: []int32{0, 0},
				},
				"Primitives.Long": {
					Values:           []interface{}{int64(1), int64(2)},
					DefinitionLevels: []int32{0, 0},
					RepetitionLevels: []int32{0, 0},
				},
				"Primitives.Float": {
					Values:           []interface{}{float32(1.1), float32(2.2)},
					DefinitionLevels: []int32{0, 0},
					RepetitionLevels: []int32{0, 0},
				},
				"Primitives.Double": {
					Values:           []interface{}{float64(1.1), float64(2.2)},
					DefinitionLevels: []int32{0, 0},
					RepetitionLevels: []int32{0, 0},
				},
				"Primitives.Bytes": {
					Values:           []interface{}{fmt.Sprintf("%v", []byte("foo")), fmt.Sprintf("%v", []byte("bar"))},
					DefinitionLevels: []int32{0, 0},
					RepetitionLevels: []int32{0, 0},
				},
				"Primitives.String": {
					Values:           []interface{}{"foo", "bar"},
					DefinitionLevels: []int32{0, 0},
					RepetitionLevels: []int32{0, 0},
				},
			},
			err: nil,
		},

		// Nested
		{
			input: func(s *schema.IntermediateSchema) []interface{} {
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

				return []interface{}{record.NewWrappedRecord(b)}
			},
			schema: schema.NewIntermediateSchema(
				arrow.NewSchema(
					[]arrow.Field{
						{
							Name: "boolean",
							Type: arrow.FixedWidthTypes.Boolean,
						},
						{
							Name: "int",
							Type: arrow.PrimitiveTypes.Uint32,
						},
						{
							Name: "long",
							Type: arrow.PrimitiveTypes.Uint64,
						},
						{
							Name: "float",
							Type: arrow.PrimitiveTypes.Float32,
						},
						{
							Name: "double",
							Type: arrow.PrimitiveTypes.Float64,
						},
						{
							Name: "bytes",
							Type: arrow.BinaryTypes.Binary,
						},
						{
							Name: "string",
							Type: arrow.BinaryTypes.String,
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
			expect: &map[string]*layout.Table{
				"Nested.Boolean": {
					Values:           []interface{}{false, true},
					DefinitionLevels: []int32{0, 0},
					RepetitionLevels: []int32{0, 0},
				},
				"Nested.Int": {
					Values:           []interface{}{int32(1), int32(2)},
					DefinitionLevels: []int32{0, 0},
					RepetitionLevels: []int32{0, 0},
				},
				"Nested.Long": {
					Values:           []interface{}{int64(1), int64(2)},
					DefinitionLevels: []int32{0, 0},
					RepetitionLevels: []int32{0, 0},
				},
				"Nested.Float": {
					Values:           []interface{}{float32(1.1), float32(2.2)},
					DefinitionLevels: []int32{0, 0},
					RepetitionLevels: []int32{0, 0},
				},
				"Nested.Double": {
					Values:           []interface{}{float64(1.1), float64(2.2)},
					DefinitionLevels: []int32{0, 0},
					RepetitionLevels: []int32{0, 0},
				},
				"Nested.Bytes": {
					Values:           []interface{}{fmt.Sprintf("%v", []byte("foo")), fmt.Sprintf("%v", []byte("bar"))},
					DefinitionLevels: []int32{0, 0},
					RepetitionLevels: []int32{0, 0},
				},
				"Nested.String": {
					Values:           []interface{}{"foo", "bar"},
					DefinitionLevels: []int32{0, 0},
					RepetitionLevels: []int32{0, 0},
				},
				"Nested.Record.Boolean": {
					Values:           []interface{}{false, true},
					DefinitionLevels: []int32{0, 0},
					RepetitionLevels: []int32{0, 0},
				},
				"Nested.Record.Int": {
					Values:           []interface{}{int32(1), int32(2)},
					DefinitionLevels: []int32{0, 0},
					RepetitionLevels: []int32{0, 0},
				},
				"Nested.Record.Long": {
					Values:           []interface{}{int64(1), int64(2)},
					DefinitionLevels: []int32{0, 0},
					RepetitionLevels: []int32{0, 0},
				},
				"Nested.Record.Float": {
					Values:           []interface{}{float32(1.1), float32(2.2)},
					DefinitionLevels: []int32{0, 0},
					RepetitionLevels: []int32{0, 0},
				},
				"Nested.Record.Double": {
					Values:           []interface{}{float64(1.1), float64(2.2)},
					DefinitionLevels: []int32{0, 0},
					RepetitionLevels: []int32{0, 0},
				},
				"Nested.Record.Bytes": {
					Values:           []interface{}{fmt.Sprintf("%v", []byte("foo")), fmt.Sprintf("%v", []byte("bar"))},
					DefinitionLevels: []int32{0, 0},
					RepetitionLevels: []int32{0, 0},
				},
				"Nested.Record.String": {
					Values:           []interface{}{"foo", "bar"},
					DefinitionLevels: []int32{0, 0},
					RepetitionLevels: []int32{0, 0},
				},
			},
			err: nil,
		},

		// Array
		{
			input: func(s *schema.IntermediateSchema) []interface{} {
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
				sb.Append(true)
				sb.FieldBuilder(0).(*array.BooleanBuilder).AppendValues([]bool{false}, []bool{true})
				sb.FieldBuilder(1).(*array.Uint32Builder).AppendValues([]uint32{1}, []bool{true})
				sb.FieldBuilder(2).(*array.Uint64Builder).AppendValues([]uint64{1}, []bool{true})
				sb.FieldBuilder(3).(*array.Float32Builder).AppendValues([]float32{1.1}, []bool{true})
				sb.FieldBuilder(4).(*array.Float64Builder).AppendValues([]float64{1.1}, []bool{true})
				sb.FieldBuilder(5).(*array.BinaryBuilder).AppendValues([][]byte{[]byte("foo")}, []bool{true})
				sb.FieldBuilder(6).(*array.StringBuilder).AppendValues([]string{"foo"}, []bool{true})
				lb.Append(true)
				sb.Append(true)
				sb.FieldBuilder(0).(*array.BooleanBuilder).AppendValues([]bool{true}, []bool{true})
				sb.FieldBuilder(1).(*array.Uint32Builder).AppendValues([]uint32{2}, []bool{true})
				sb.FieldBuilder(2).(*array.Uint64Builder).AppendValues([]uint64{2}, []bool{true})
				sb.FieldBuilder(3).(*array.Float32Builder).AppendValues([]float32{2.2}, []bool{true})
				sb.FieldBuilder(4).(*array.Float64Builder).AppendValues([]float64{2.2}, []bool{true})
				sb.FieldBuilder(5).(*array.BinaryBuilder).AppendValues([][]byte{[]byte("bar")}, []bool{true})
				sb.FieldBuilder(6).(*array.StringBuilder).AppendValues([]string{"bar"}, []bool{true})

				return []interface{}{record.NewWrappedRecord(b)}
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
			expect: &map[string]*layout.Table{
				"Arrays.Boolean": {
					Values:           []interface{}{false, true},
					DefinitionLevels: []int32{0, 0},
					RepetitionLevels: []int32{0, 0},
				},
				"Arrays.Int": {
					Values:           []interface{}{int32(1), int32(2)},
					DefinitionLevels: []int32{0, 0},
					RepetitionLevels: []int32{0, 0},
				},
				"Arrays.Long": {
					Values:           []interface{}{int64(1), int64(2)},
					DefinitionLevels: []int32{0, 0},
					RepetitionLevels: []int32{0, 0},
				},
				"Arrays.Float": {
					Values:           []interface{}{float32(1.1), float32(2.2)},
					DefinitionLevels: []int32{0, 0},
					RepetitionLevels: []int32{0, 0},
				},
				"Arrays.Double": {
					Values:           []interface{}{float64(1.1), float64(2.2)},
					DefinitionLevels: []int32{0, 0},
					RepetitionLevels: []int32{0, 0},
				},
				"Arrays.Bytes": {
					Values:           []interface{}{fmt.Sprintf("%v", []byte("foo")), fmt.Sprintf("%v", []byte("bar"))},
					DefinitionLevels: []int32{0, 0},
					RepetitionLevels: []int32{0, 0},
				},
				"Arrays.String": {
					Values:           []interface{}{"foo", "bar"},
					DefinitionLevels: []int32{0, 0},
					RepetitionLevels: []int32{0, 0},
				},
				"Arrays.Array.Boolean": {
					Values:           []interface{}{false, true},
					DefinitionLevels: []int32{1, 1},
					RepetitionLevels: []int32{0, 0},
				},
				"Arrays.Array.Int": {
					Values:           []interface{}{int32(1), int32(2)},
					DefinitionLevels: []int32{1, 1},
					RepetitionLevels: []int32{0, 0},
				},
				"Arrays.Array.Long": {
					Values:           []interface{}{int64(1), int64(2)},
					DefinitionLevels: []int32{1, 1},
					RepetitionLevels: []int32{0, 0},
				},
				"Arrays.Array.Float": {
					Values:           []interface{}{float32(1.1), float32(2.2)},
					DefinitionLevels: []int32{1, 1},
					RepetitionLevels: []int32{0, 0},
				},
				"Arrays.Array.Double": {
					Values:           []interface{}{float64(1.1), float64(2.2)},
					DefinitionLevels: []int32{1, 1},
					RepetitionLevels: []int32{0, 0},
				},
				"Arrays.Array.Bytes": {
					Values:           []interface{}{fmt.Sprintf("%v", []byte("foo")), fmt.Sprintf("%v", []byte("bar"))},
					DefinitionLevels: []int32{1, 1},
					RepetitionLevels: []int32{0, 0},
				},
				"Arrays.Array.String": {
					Values:           []interface{}{"foo", "bar"},
					DefinitionLevels: []int32{1, 1},
					RepetitionLevels: []int32{0, 0},
				},
			},
			err: nil,
		},
	}

	for _, c := range cases {
		sh, err := schema.NewSchemaHandlerFromArrow(*c.schema)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		tables, err := MarshalArrow(c.input(c.schema), 0, 1, sh)

		if err != c.err {
			t.Errorf("expected: %v, but actual: %v\n", c.err, err)
		}

		for k, v := range *c.expect {
			actual := (*tables)[k]

			if !reflect.DeepEqual(actual.Values, v.Values) {
				t.Errorf("expected: %v, but actual: %v\n", v.Values, actual.Values)
			}

			if !reflect.DeepEqual(actual.DefinitionLevels, v.DefinitionLevels) {
				t.Errorf("expected: %v, but actual: %v\n", v.DefinitionLevels, actual.DefinitionLevels)
			}

			if !reflect.DeepEqual(actual.RepetitionLevels, v.RepetitionLevels) {
				t.Errorf("expected: %v, but actual: %v\n", v.RepetitionLevels, actual.RepetitionLevels)
			}
		}
	}
}
