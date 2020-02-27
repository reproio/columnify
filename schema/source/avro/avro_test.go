package avro

import (
	"encoding/json"
	"testing"
)

func areSameRecordType(l, r recordType) bool {
	ll, err := json.Marshal(l)
	if err != nil {
		return false
	}

	rr, err := json.Marshal(r)
	if err != nil {
		return false
	}

	return string(ll) == string(rr)
}

func TestUnmarshal(t *testing.T) {
	cases := []struct {
		schema   string
		expected recordType
		err      error
	}{
		{
			schema: `
{
  "type": "record",
  "name": "LongList",
  "aliases": ["LinkedLongs"],
  "fields" : [
    {"name": "value", "type": "long"},
    {"name": "next", "type": ["null", "LongList"]}
  ]
}
`,
			expected: recordType{
				Name:    "LongList",
				Aliases: []string{"LinkedLongs"},
				Fields: []recordField{
					{
						Name: "value",
						Type: avroType{
							primitiveType: toPrimitiveType("long"),
						},
					},
					{
						Name: "next",
						Type: avroType{
							unionType: &unionType{
								{
									primitiveType: toPrimitiveType("null"),
								},
								{
									definedType: toDefinedType("LongList"),
								},
							},
						},
					},
				},
			},
			err: nil,
		},
	}

	for _, c := range cases {
		var actual recordType
		err := json.Unmarshal([]byte(c.schema), &actual)

		if err != c.err {
			t.Errorf("expected: %v, but actual: %v\n", c.err, err)
		}

		if areSameRecordType(actual, c.expected) {
			t.Errorf("expected: %v, but actual: %v\n", c.expected, actual)
		}
	}
}
