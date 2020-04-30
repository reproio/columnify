package record

import (
	"encoding/json"
	"fmt"
	"strings"

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
			return nil, fmt.Errorf("jsonl parse error %v: %w", err, ErrUnconvertibleRecord)
		}

		records = append(records, e)
	}

	return records, nil
}

func FormatJsonlToArrow(s *schema.IntermediateSchema, data []byte) (*WrappedRecord, error) {
	maps, err := FormatJsonlToMap(data)
	if err != nil {
		return nil, err
	}

	return formatMapToArrowRecord(s.ArrowSchema, maps)
}
