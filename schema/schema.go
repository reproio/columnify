package schema

import (
	"errors"
	"fmt"
)

const (
	SchemaTypeAvro     = "avro"
	SchemaTypeBigquery = "bigquery"
)

var (
	ErrUnsupportedSchema   = errors.New("unsupported schema")
	ErrUnconvertibleSchema = errors.New("input schema is unable to convert")
)

func GetSchema(content []byte, schemaType string) (*IntermediateSchema, error) {
	switch schemaType {
	case SchemaTypeAvro:
		return NewSchemaFromAvroSchema(content)
	case SchemaTypeBigquery:
		return NewSchemaFromBigQuerySchema(content)
	default:
		return nil, fmt.Errorf("%s; %w", schemaType, ErrUnsupportedSchema)
	}
}
