package schema

import "fmt"

const (
	SchemaTypeAvro     = "avro"
	SchemaTypeBigquery = "bigquery"
)

func GetSchema(content []byte, schemaType string) (*IntermediateSchema, error) {
	switch schemaType {
	case SchemaTypeAvro:
		return NewSchemaFromAvroSchema(content)
	case SchemaTypeBigquery:
		return NewSchemaFromBigQuerySchema(content)
	default:
		return nil, fmt.Errorf("unsupported schema type: %s", schemaType)
	}
}
