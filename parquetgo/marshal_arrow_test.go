package parquetgo

import (
	"fmt"
	"github.com/repro/columnify/schema"
	"github.com/xitongsys/parquet-go/layout"
	"reflect"
	"testing"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/memory"
)

func int32ToPtr(v int32) *int32 { return &v }

func TestNewArrowSchemaFromAvroSchema(t *testing.T) {
	cases := []struct {
		input  []interface{}
		schema *schema.IntermediateSchema
		expect *map[string]*layout.Table
		err    error
	}{
		{
			input: func() []interface{} {
				pool := memory.NewGoAllocator()
				b := array.NewRecordBuilder(pool, arrow.NewSchema(
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
					},
					nil),
				)

				b.Field(0).(*array.BooleanBuilder).AppendValues([]bool{false, true}, []bool{true, true})
				b.Field(1).(*array.Uint32Builder).AppendValues([]uint32{1, 2}, []bool{true, true})
				b.Field(2).(*array.Uint64Builder).AppendValues([]uint64{1, 2}, []bool{true, true})
				b.Field(3).(*array.Float32Builder).AppendValues([]float32{1.1, 2.2}, []bool{true, true})
				b.Field(4).(*array.Float64Builder).AppendValues([]float64{1.1, 2.2}, []bool{true, true})
				b.Field(5).(*array.BinaryBuilder).AppendValues([][]byte{[]byte("foo"), []byte("bar")}, []bool{true, true})
				b.Field(6).(*array.StringBuilder).AppendValues([]string{"foo", "bar"}, []bool{true, true})

				return []interface{}{NewWrappedRecord(b)}
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
	}

	for _, c := range cases {
		sh, err := schema.NewSchemaHandlerFromArrow(*c.schema)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		tables, err := MarshalArrow(c.input, 0, 1, sh)

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
