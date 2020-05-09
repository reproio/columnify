package record

import (
	"reflect"
	"testing"
)

func TestFormatLtsvToMap(t *testing.T) {
	cases := []struct {
		input    []byte
		expected []map[string]interface{}
		isErr    bool
	}{
		// Primitives
		{
			input: []byte(`boolean:false	int:1	long:1	float:1.1	double:1.1	bytes:foo	string:foo
boolean:true	int:2	long:2	float:2.2	double:2.2	bytes:bar	string:bar`),
			expected: []map[string]interface{}{
				{
					"boolean": false,
					"bytes":   string([]byte("foo")),
					"double":  float64(1.1),
					"float":   float64(1.1),
					"int":     int64(1),
					"long":    int64(1),
					"string":  "foo",
				},
				{
					"boolean": true,
					"bytes":   string([]byte("bar")),
					"double":  float64(2.2),
					"float":   float64(2.2),
					"int":     int64(2),
					"long":    int64(2),
					"string":  "bar",
				},
			},
			isErr: false,
		},

		// Not LTSV
		{
			input:    []byte("not-valid-ltsv"),
			expected: nil,
			isErr:    true,
		},
	}

	for _, c := range cases {
		actual, err := FormatLtsvToMap(c.input)

		if err != nil != c.isErr {
			t.Errorf("expected: %v, but actual: %v\n", c.isErr, err)
		}

		if !reflect.DeepEqual(actual, c.expected) {
			t.Errorf("expected: %v, but actual: %v\n", c.expected, actual)
		}
	}
}
