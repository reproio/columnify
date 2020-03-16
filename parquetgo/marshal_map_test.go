package parquetgo

import (
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/schema"
	"testing"
)

func TestMarshalMap(t *testing.T) {
	cases := []struct {
		input map[string]interface{}
		bgn   int
		end   int
		sh    *schema.SchemaHandler
		err   error
	}{
		{
			input: map[string]interface{}{
				"boolean": true,
				"bytes":   "bytes",
				"double":  44.22,
				"float":   4.2,
				"int":     42,
				"long":    420,
				"string":  "string",
			},
			bgn: 0,
			end: 1,
			sh: func() *schema.SchemaHandler {
				return schema.NewSchemaHandlerFromSchemaList([]*parquet.SchemaElement{
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
				})
			}(),
			err: nil,
		},
	}

	for _, c := range cases {
		// prepare schema handler
		src := []interface{}{c.input}

		_, err := MarshalMap(src, c.bgn, c.end, c.sh)

		if err != c.err {
			t.Errorf("expected: %v, but actual: %v\n", c.err, err)
		}
	}
}
