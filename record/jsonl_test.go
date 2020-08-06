package record

import (
	"bytes"
	"io"
	"reflect"
	"testing"
)

func TestJsonlInnerDecoder_Decode(t *testing.T) {
	cases := []struct {
		input    []byte
		expected []map[string]interface{}
		isErr    bool
	}{
		// Primitives
		{
			input: []byte(
				`{"boolean": false, "int": 1, "long": 1, "float": 1.1, "double": 1.1, "bytes": "foo", "string": "foo"}
{"boolean": true, "int": 2, "long": 2, "float": 2.2, "double": 2.2, "bytes": "bar", "string": "bar"}`,
			),
			expected: []map[string]interface{}{
				{
					"boolean": false,
					"bytes":   string([]byte("foo")),
					"double":  float64(1.1),
					"float":   float64(1.1),
					"int":     float64(1),
					"long":    float64(1),
					"string":  "foo",
				},
				{
					"boolean": true,
					"bytes":   string([]byte("bar")),
					"double":  float64(2.2),
					"float":   float64(2.2),
					"int":     float64(2),
					"long":    float64(2),
					"string":  "bar",
				},
			},
			isErr: false,
		},

		// Not JSONL
		{
			input:    []byte("not-valid-json"),
			expected: []map[string]interface{}{},
			isErr:    true,
		},
	}

	for _, c := range cases {
		buf := bytes.NewReader(c.input)
		d := newJsonlInnerDecoder(buf)

		actual := make([]map[string]interface{}, 0)
		var err error
		for {
			var v map[string]interface{}
			err = d.Decode(&v)
			if err != nil {
				break
			}
			actual = append(actual, v)
		}

		if (err != nil && err != io.EOF) != c.isErr {
			t.Errorf("expected: %v, but actual: %v\n", c.isErr, err)
			continue
		}

		if !reflect.DeepEqual(actual, c.expected) {
			t.Errorf("expected: %v, but actual: %v\n", c.expected, actual)
		}
	}
}
