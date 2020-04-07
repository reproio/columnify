package schema

import (
	"fmt"

	"cloud.google.com/go/bigquery"
	"github.com/apache/arrow/go/arrow"
)

var (
	bqPrimitivesToArrow = map[bigquery.FieldType]arrow.DataType{
		bigquery.BooleanFieldType:   arrow.FixedWidthTypes.Boolean,
		bigquery.IntegerFieldType:   arrow.PrimitiveTypes.Uint64,
		bigquery.FloatFieldType:     arrow.PrimitiveTypes.Float64,
		bigquery.NumericFieldType:   arrow.PrimitiveTypes.Uint64,
		bigquery.StringFieldType:    arrow.BinaryTypes.String,
		bigquery.BytesFieldType:     arrow.BinaryTypes.Binary,
		bigquery.DateFieldType:      arrow.FixedWidthTypes.Date32,
		bigquery.TimeFieldType:      arrow.FixedWidthTypes.Time64us,
		bigquery.TimestampFieldType: arrow.FixedWidthTypes.Timestamp_us,
		// bigquery.DateTimeFieldType: Unsupported
	}
)

func NewSchemaFromBigQuerySchema(schemaContent []byte) (*IntermediateSchema, error) {
	s, err := bigquery.SchemaFromJSON(schemaContent)
	if err != nil {
		return nil, err
	}

	fields := make([]arrow.Field, 0)
	for _, f := range s {
		af, err := bqFieldToArrowField(f)
		if err != nil {
			return nil, err
		}
		fields = append(fields, *af)
	}

	return NewIntermediateSchema(arrow.NewSchema(fields, nil), ""), nil
}

func bqFieldToArrowField(f *bigquery.FieldSchema) (*arrow.Field, error) {
	if pt, ok := bqPrimitivesToArrow[f.Type]; ok {
		return &arrow.Field{
			Name:     f.Name,
			Type:     bqModeToList(f, pt),
			Nullable: bqModeToNullable(f),
		}, nil
	}

	if f.Type == bigquery.RecordFieldType {
		subFields := make([]arrow.Field, 0, len(f.Schema))
		for _, sub := range f.Schema {
			sf, err := bqFieldToArrowField(sub)
			if err != nil {
				return nil, err
			}

			subFields = append(subFields, *sf)
		}

		return &arrow.Field{
			Name:     f.Name,
			Type:     bqModeToList(f, arrow.StructOf(subFields...)),
			Nullable: bqModeToNullable(f),
		}, nil
	}

	return nil, fmt.Errorf("unsupported field: %v", f)
}

func bqModeToNullable(f *bigquery.FieldSchema) bool {
	return !f.Required
}

func bqModeToList(f *bigquery.FieldSchema, t arrow.DataType) arrow.DataType {
	if f.Repeated {
		return arrow.ListOf(t)
	} else {
		return t
	}
}
