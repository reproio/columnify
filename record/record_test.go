package record

import (
	"encoding/json"
	"testing"
)

type nopInnerDecoder struct {
	r   map[string]interface{}
	err error
}

func (d *nopInnerDecoder) Decode(r *map[string]interface{}) error {
	*r = d.r
	return d.err
}

func TestJsonStringConverter_Convert(t *testing.T) {
	inner := &nopInnerDecoder{
		r: map[string]interface{}{
			"key1": 42,
			"key2": "test",
		},
		err: nil,
	}

	d := jsonStringConverter{
		inner: inner,
	}

	var v string
	err := d.Convert(&v)
	if err != nil {
		t.Fatalf("expected no error, but actual: %v\n", err)
	}

	data, err := json.Marshal(inner.r)
	if err != nil {
		t.Fatalf("expected no error, but actual: %v\n", err)
	}
	if v != string(data) {
		t.Fatalf("expected: %v, but actual: %v\n", string(data), v)
	}
}
