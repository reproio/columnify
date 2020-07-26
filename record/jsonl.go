package record

import (
	"encoding/json"
	"strings"

	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/memory"

	"github.com/reproio/columnify/schema"
)

func FormatJsonlToMap(data []byte) ([]map[string]interface{}, error) {
	lines := strings.Split(string(data), "\n")

	records := make([]map[string]interface{}, 0)
	for _, l := range lines {
		if l == "" {
			// skip blank line
			continue
		}

		var e map[string]interface{}
		if err := json.Unmarshal([]byte(l), &e); err != nil {
			return nil, err
		}

		records = append(records, e)
	}

	return records, nil
}

func FormatJsonlToArrow(s *schema.IntermediateSchema, data []byte) (*WrappedRecord, error) {
	pool := memory.NewGoAllocator()
	b := array.NewRecordBuilder(pool, s.ArrowSchema)
	defer b.Release()

	for _, l := range strings.Split(string(data), "\n") {
		if l == "" {
			// skip blank line
			continue
		}

		var e map[string]interface{}
		if err := json.Unmarshal([]byte(l), &e); err != nil {
			return nil, err
		}

		if _, err := formatMapToArrowRecord(b, e); err != nil {
			return nil, err
		}
	}

	return NewWrappedRecord(b), nil
}
