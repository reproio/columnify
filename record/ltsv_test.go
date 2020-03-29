package record

import (
	"reflect"
	"testing"
)

func TestFormatLtsvToMap(t *testing.T) {
	cases := []struct {
		input    []byte
		expected []map[string]interface{}
		err      error
	}{
		// Primitives
		{
			input: []byte(`boolean:false	int:1	long:1	float:1.1	double:1.1	bytes:foo	string:foo
boolean:true	int:2	long:2	float:2.2	double:2.2	bytes:bar	string:bar`),
			expected: []map[string]interface{}{
				{
					"boolean": "false",
					"bytes":   string([]byte("foo")),
					"double":  "1.1", // TODO cast to actual types
					"float":   "1.1",
					"int":     "1",
					"long":    "1",
					"string":  "foo",
				},
				{
					"boolean": "true",
					"bytes":   string([]byte("bar")),
					"double":  "2.2",
					"float":   "2.2",
					"int":     "2",
					"long":    "2",
					"string":  "bar",
				},
			},
			err: nil,
		},
	}

	for _, c := range cases {
		actual, err := FormatLtsvToMap(c.input)

		if err != c.err {
			t.Errorf("expected: %v, but actual: %v\n", c.err, err)
		}

		if !reflect.DeepEqual(actual, c.expected) {
			t.Errorf("expected: %v, but actual: %v\n", c.expected, actual)
		}
	}
}
