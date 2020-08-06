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

func TestJsonDecoder_Decode(t *testing.T) {
	inner := &nopInnerDecoder{
		r: map[string]interface{}{
			"key1": 42,
			"key2": "test",
		},
		err: nil,
	}

	d := jsonDecoder{
		inner: inner,
	}

	var v string
	err := d.Decode(&v)
	if err != nil {
		t.Errorf("expected no error, but actual: %v\n", err)
	}

	data, err := json.Marshal(inner.r)
	if err != nil {
		t.Errorf("expected no error, but actual: %v\n", err)
	}
	if v != string(data) {
		t.Errorf("expected: %v, but actual: %v\n", string(data), v)
	}
}
