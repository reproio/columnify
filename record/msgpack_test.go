package record

import (
	"bytes"
	"errors"
	"io"
	"reflect"
	"testing"
)

func TestMsgpackInnnerDecoder_Decode(t *testing.T) {
	cases := []struct {
		input    []byte
		expected []map[string]interface{}
		err      error
	}{
		// Primitives
		{
			// examples/record/primitives.msgpack
			input: bytes.Join([][]byte{
				[]byte("\x87\xa7\x62\x6f\x6f\x6c\x65\x61\x6e\xc2\xa3\x69\x6e\x74\x01\xa4"),
				[]byte("\x6c\x6f\x6e\x67\x01\xa5\x66\x6c\x6f\x61\x74\xcb\x3f\xf1\x99\x99"),
				[]byte("\x99\x99\x99\x9a\xa6\x64\x6f\x75\x62\x6c\x65\xcb\x3f\xf1\x99\x99"),
				[]byte("\x99\x99\x99\x9a\xa5\x62\x79\x74\x65\x73\xa3\x66\x6f\x6f\xa6\x73"),
				[]byte("\x74\x72\x69\x6e\x67\xa3\x66\x6f\x6f\x87\xa7\x62\x6f\x6f\x6c\x65"),
				[]byte("\x61\x6e\xc3\xa3\x69\x6e\x74\x02\xa4\x6c\x6f\x6e\x67\x02\xa5\x66"),
				[]byte("\x6c\x6f\x61\x74\xcb\x40\x01\x99\x99\x99\x99\x99\x9a\xa6\x64\x6f"),
				[]byte("\x75\x62\x6c\x65\xcb\x40\x01\x99\x99\x99\x99\x99\x9a\xa5\x62\x79"),
				[]byte("\x74\x65\x73\xa3\x62\x61\x72\xa6\x73\x74\x72\x69\x6e\x67\xa3\x62"),
				[]byte("\x61\x72"),
			}, []byte("")),
			expected: []map[string]interface{}{
				{
					"boolean": false,
					"bytes":   string([]byte("foo")),
					"double":  float64(1.1),
					"float":   float64(1.1),
					"int":     int8(1),
					"long":    int8(1),
					"string":  "foo",
				},
				{
					"boolean": true,
					"bytes":   string([]byte("bar")),
					"double":  float64(2.2),
					"float":   float64(2.2),
					"int":     int8(2),
					"long":    int8(2),
					"string":  "bar",
				},
			},
			err: io.EOF,
		},

		// Not map type
		{
			input:    []byte("\xa7compact"),
			expected: []map[string]interface{}{},
			err:      ErrUnconvertibleRecord,
		},
	}

	for _, c := range cases {
		buf := bytes.NewReader(c.input)
		d := newMsgpackInnerDecoder(buf)

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

		if !errors.Is(err, c.err) {
			t.Errorf("expected: %v, but actual: %v\n", c.err, err)
			continue
		}

		if !reflect.DeepEqual(actual, c.expected) {
			t.Errorf("expected: %v, but actual: %v\n", c.expected, actual)
		}
	}
}
