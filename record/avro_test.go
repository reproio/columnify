package record

import (
	"bytes"
	"io"
	"reflect"
	"testing"

	"github.com/linkedin/goavro/v2"
)

func TestFlattenAvroUnion(t *testing.T) {
	input := map[string]interface{}{
		"primitive": 42,
		"nested-single": map[string]interface{}{
			"string": "test",
		},
		"nested-multiple": map[string]interface{}{
			"int":    42,
			"string": "test",
		},
	}
	expected := map[string]interface{}{
		"primitive":     42,
		"nested-single": "test",
		"nested-multiple": map[string]interface{}{
			"int":    42,
			"string": "test",
		},
	}

	actual := flattenAvroUnion(input)

	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("expected: %v, but actual: %v\n", expected, actual)
	}
}

func TestAvroInnerDecoder_Decode(t *testing.T) {
	cases := []struct {
		input    []byte
		expected []map[string]interface{}
	}{
		{
			input: func() []byte {
				w := &bytes.Buffer{}

				r, err := goavro.NewOCFWriter(goavro.OCFConfig{
					W: w,
					Schema: `
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
`,
				})
				if err != nil {
					t.Fatal(err)
				}

				err = r.Append([]map[string]interface{}{
					{
						"boolean": false,
						"bytes":   string([]byte("foo")),
						"double":  1.1,
						"float":   1.1,
						"int":     1,
						"long":    1,
						"string":  "foo",
					},
					{
						"boolean": true,
						"bytes":   string([]byte("bar")),
						"double":  2.2,
						"float":   2.2,
						"int":     2,
						"long":    2,
						"string":  "bar",
					},
				})
				if err != nil {
					t.Fatal(err)
				}

				return w.Bytes()
			}(),
			expected: []map[string]interface{}{
				{
					"boolean": false,
					"bytes":   []byte("foo"),
					"double":  float64(1.1),
					"float":   float32(1.1),
					"int":     int32(1),
					"long":    int64(1),
					"string":  "foo",
				},
				{
					"boolean": true,
					"bytes":   []byte("bar"),
					"double":  float64(2.2),
					"float":   float32(2.2),
					"int":     int32(2),
					"long":    int64(2),
					"string":  "bar",
				},
			},
		},
	}

	for _, c := range cases {
		buf := bytes.NewReader(c.input)
		d, err := newAvroInnerDecoder(buf)
		if err != nil {
			t.Fatal(err)
		}

		actual := make([]map[string]interface{}, 0)
		for {
			var v map[string]interface{}
			err = d.Decode(&v)
			if err != nil {
				break
			}
			actual = append(actual, v)
		}

		if err != nil && err != io.EOF {
			t.Errorf("expected no error or io.EOF, but actual: %v\n", err)
			continue
		}

		if !reflect.DeepEqual(actual, c.expected) {
			t.Errorf("expected: %v, but actual: %v\n", c.expected, actual)
		}
	}
}
