package parquetgo

import (
	"testing"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/memory"
	"github.com/xitongsys/parquet-go/common"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/schema"
)

func int32ToPtr(v int32) *int32 { return &v }

func TestNewArrowSchemaFromAvroSchema(t *testing.T) {
	cases := []struct {
		input []interface{}
		sh    *schema.SchemaHandler
		err   error
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

				return []interface{}{b.NewRecord()}
			}(),
			sh: &schema.SchemaHandler{
				SchemaElements: []*parquet.SchemaElement{
					{
						RepetitionType: parquet.FieldRepetitionTypePtr(parquet.FieldRepetitionType_REQUIRED),
						Name:           "primitives",
						NumChildren:    int32ToPtr(7),
					},
					{
						Type:           parquet.TypePtr(parquet.Type_BOOLEAN),
						RepetitionType: parquet.FieldRepetitionTypePtr(parquet.FieldRepetitionType_REQUIRED),
						Name:           "boolean",
					},
					{
						Type:           parquet.TypePtr(parquet.Type_INT32),
						RepetitionType: parquet.FieldRepetitionTypePtr(parquet.FieldRepetitionType_REQUIRED),
						Name:           "int",
					},
					{
						Type:           parquet.TypePtr(parquet.Type_INT64),
						RepetitionType: parquet.FieldRepetitionTypePtr(parquet.FieldRepetitionType_REQUIRED),
						Name:           "long",
					},
					{
						Type:           parquet.TypePtr(parquet.Type_FLOAT),
						RepetitionType: parquet.FieldRepetitionTypePtr(parquet.FieldRepetitionType_REQUIRED),
						Name:           "float",
					},
					{
						Type:           parquet.TypePtr(parquet.Type_DOUBLE),
						RepetitionType: parquet.FieldRepetitionTypePtr(parquet.FieldRepetitionType_REQUIRED),
						Name:           "double",
					},
					{
						Type:           parquet.TypePtr(parquet.Type_BYTE_ARRAY),
						RepetitionType: parquet.FieldRepetitionTypePtr(parquet.FieldRepetitionType_REQUIRED),
						Name:           "bytes",
					},
					{
						Type:           parquet.TypePtr(parquet.Type_BYTE_ARRAY),
						RepetitionType: parquet.FieldRepetitionTypePtr(parquet.FieldRepetitionType_REQUIRED),
						ConvertedType:  parquet.ConvertedTypePtr(parquet.ConvertedType_UTF8),
						Name:           "string",
					},
				},
				Infos: []*common.Tag{
					{
						InName: "root",
					},
					{
						InName: "primitives",
					},
					{
						InName: "boolean",
					},
					{
						InName: "int",
					},
					{
						InName: "long",
					},
					{
						InName: "float",
					},
					{
						InName: "double",
					},
					{
						InName: "bytes",
					},
					{
						InName: "string",
					},
				},
			},
		},
	}

	for _, c := range cases {
		table, err := MarshalArrow(c.input, -1, -1, c.sh)

		if err != c.err {
			t.Errorf("expected: %v, but actual: %v\n", c.err, err)
		}

		// FIXME
		if table != nil {
			t.Errorf("expected: %v, but actual: %v\n", nil, table)
		}
	}
}
