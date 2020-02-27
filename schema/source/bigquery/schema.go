package bigquery

import (
	"fmt"

	"cloud.google.com/go/bigquery"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/schema"
)

var ErrUnknownFiledType = fmt.Errorf("unkwnon BigQuery field type")

func NewSchemaHandlerFromBigQuerySchema(schemaContent []byte) (*schema.SchemaHandler, error) {
	s, err := bigquery.SchemaFromJSON(schemaContent)
	if err != nil {
		return nil, err
	}

	elems := make([]*parquet.SchemaElement, len(s))

	for _, f := range s {
		e := parquet.NewSchemaElement()

		switch f.Type {
		case bigquery.BooleanFieldType:
			e.Type = parquet.TypePtr(parquet.Type_BOOLEAN)

		case bigquery.BytesFieldType:
			e.Type = parquet.TypePtr(parquet.Type_BYTE_ARRAY)

		// case bigquery.DateFieldType:
		// case bigquery.DateTimeFieldType:

		case bigquery.FloatFieldType:
			e.Type = parquet.TypePtr(parquet.Type_FLOAT)

		// case bigquery.GeographyFieldType:

		case bigquery.IntegerFieldType:
			e.Type = parquet.TypePtr(parquet.Type_INT64)

		case bigquery.NumericFieldType:
			e.Type = parquet.TypePtr(parquet.Type_INT64)

		// case bigquery.RecordFieldType:
		// case bigquery.TimeFieldType:
		// case bigquery.TimestampFieldType:

		case bigquery.StringFieldType:
			e.Type = parquet.TypePtr(parquet.Type_BYTE_ARRAY)
			e.ConvertedType = parquet.ConvertedTypePtr(parquet.ConvertedType_UTF8)

		default:
			return nil, ErrUnknownFiledType
		}

		e.RepetitionType = parquet.FieldRepetitionTypePtr(BqModeToParquetRepetationType(f))

		elems = append(elems, e)
	}

	sh := schema.NewSchemaHandlerFromSchemaList(elems)
	// TODO info's
	sh.CreateInExMap()

	return sh, nil
}

func BqModeToParquetRepetationType(f *bigquery.FieldSchema) parquet.FieldRepetitionType {
	if f.Repeated {
		return parquet.FieldRepetitionType_REQUIRED
	} else if f.Required {
		return parquet.FieldRepetitionType_REQUIRED
	} else {
		return parquet.FieldRepetitionType_OPTIONAL
	}
}
