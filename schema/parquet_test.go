package schema

import (
	"reflect"
	"testing"

	"github.com/apache/arrow/go/arrow"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/schema"
)

func int32ToPtr(v int32) *int32 { return &v }

func TestNewSchemaHandlerFromArrow(t *testing.T) {
	cases := []struct {
		intermediate *IntermediateSchema
		expected     schema.SchemaHandler
		err          error
	}{
		// primitive types
		{
			intermediate: NewIntermediateSchema(
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
			expected: schema.SchemaHandler{
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
			},
			err: nil,
		},

		// Nested types
		{
			intermediate: NewIntermediateSchema(
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
					}, nil),
				"nested"),
			expected: schema.SchemaHandler{
				SchemaElements: []*parquet.SchemaElement{
					{
						RepetitionType: parquet.FieldRepetitionTypePtr(parquet.FieldRepetitionType_REQUIRED),
						Name:           "nested",
						NumChildren:    int32ToPtr(8),
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
					{
						RepetitionType: parquet.FieldRepetitionTypePtr(parquet.FieldRepetitionType_REQUIRED),
						Name:           "record",
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
			},
			err: nil,
		},

		// Array
		{
			intermediate: NewIntermediateSchema(
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
				"array"),
			expected: schema.SchemaHandler{
				SchemaElements: []*parquet.SchemaElement{
					{
						RepetitionType: parquet.FieldRepetitionTypePtr(parquet.FieldRepetitionType_REQUIRED),
						Name:           "array",
						NumChildren:    int32ToPtr(8),
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
					{
						RepetitionType: parquet.FieldRepetitionTypePtr(parquet.FieldRepetitionType_REPEATED),
						Name:           "array",
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
			},
			err: nil,
		},
	}

	for _, c := range cases {
		actual, err := NewSchemaHandlerFromArrow(*c.intermediate)

		if err != c.err {
			t.Errorf("expected: %v, but actual: %v\n", c.err, err)
		}

		// For now, compare only SchemaElements
		if !reflect.DeepEqual(actual.SchemaElements, c.expected.SchemaElements) {
			t.Errorf("expected: %v, but actual: %v\n", c.expected.SchemaElements, actual.SchemaElements)
		}
	}
}
