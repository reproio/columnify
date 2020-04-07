package schema

import (
	"testing"
)

func TestGetSchema(t *testing.T) {
	cases := []struct {
		content    []byte
		schemaType string
		err        error
	}{
		// Avro
		{
			content: []byte(`
{
  "type": "record",
  "name": "Primitives",
  "fields" : [
    {"name": "boolean", "type": "boolean"},
    {"name": "int",     "type": "int"},
    {"name": "long",    "type": "long"},
    {"name": "float",   "type": "float"},
    {"name": "double",  "type": "double"},
    {"name": "bytes",   "type": "bytes"},
    {"name": "string",  "type": "string"}
  ]
}
`),
			schemaType: SchemaTypeAvro,
			err:        nil,
		},

		// BigQuery
		{
			content: []byte(`
[
  {
    "name": "boolean",
    "type": "BOOLEAN",
    "mode": "REQUIRED"
  },
  {
    "name": "int",
    "type": "INTEGER",
    "mode": "REQUIRED"
  },
  {
    "name": "long",
    "type": "INTEGER",
    "mode": "REQUIRED"
  },
  {
    "name": "float",
    "type": "FLOAT",
    "mode": "REQUIRED"
  },
  {
    "name": "double",
    "type": "FLOAT",
    "mode": "REQUIRED"
  },
  {
    "name": "bytes",
    "type": "BYTES",
    "mode": "REQUIRED"
  },
  {
    "name": "string",
    "type": "STRING",
    "mode": "REQUIRED"
  }
]
`),
			schemaType: SchemaTypeBigquery,
			err:        nil,
		},

		// Invalid
		/*
			{
				content:    []byte("invalid"),
				schemaType: "unknown",
				err:        nil,
			},
		*/
	}

	for _, c := range cases {
		_, err := GetSchema(c.content, c.schemaType)

		if err != c.err {
			t.Errorf("expected: %v, but actual: %v\n", c.err, err)
		}
	}
}
