package schema

import (
	"errors"
	"fmt"

	"github.com/apache/arrow/go/arrow"
)

const (
	SchemaTypeAvro     = "avro"
	SchemaTypeBigquery = "bigquery"
)

var (
	ErrUnsupportedSchema   = errors.New("unsupported schema")
	ErrInvalidSchema       = errors.New("invalid schema")
	ErrUnconvertibleSchema = errors.New("input schema is unable to convert")
)

type IntermediateSchema struct {
	ArrowSchema *arrow.Schema
	Name        string
}

func NewIntermediateSchema(s *arrow.Schema, name string) *IntermediateSchema {
	return &IntermediateSchema{
		ArrowSchema: s,
		Name:        name,
	}
}

// GetSchema converts input schema intermediate schema.
func GetSchema(content []byte, schemaType string) (*IntermediateSchema, error) {
	switch schemaType {
	case SchemaTypeAvro:
		return NewSchemaFromAvroSchema(content)
	case SchemaTypeBigquery:
		return NewSchemaFromBigQuerySchema(content)
	default:
		return nil, fmt.Errorf("%s: %w", schemaType, ErrUnsupportedSchema)
	}
}
