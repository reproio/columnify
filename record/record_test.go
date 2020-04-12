package record

import (
	"errors"
	"testing"

	"github.com/reproio/columnify/schema"
)

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
