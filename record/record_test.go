package record

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/reproio/columnify/schema"
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

func TestFormatToArrow(t *testing.T) {
	cases := []struct {
		input      []byte
		schema     *schema.IntermediateSchema
		recordType string
		err        error
	}{
		// TODO valid cases

		{
			input:      nil,
			schema:     nil,
			recordType: "Unknown",
			err:        ErrUnsupportedRecord,
		},
	}

	for _, c := range cases {
		_, err := FormatToArrow(c.input, c.schema, c.recordType)

		if !errors.Is(err, c.err) {
			t.Errorf("expected: %v, but actual: %v\n", c.err, err)
		}
	}
}

func TestFormatToMap(t *testing.T) {
	cases := []struct {
		input      []byte
		schema     *schema.IntermediateSchema
		recordType string
		err        error
	}{
		// TODO valid cases

		{
			input:      nil,
			schema:     nil,
			recordType: "Unknown",
			err:        ErrUnsupportedRecord,
		},
	}

	for _, c := range cases {
		_, err := FormatToMap(c.input, c.schema, c.recordType)

		if !errors.Is(err, c.err) {
			t.Errorf("expected: %v, but actual: %v\n", c.err, err)
		}
	}
}
