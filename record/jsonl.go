package record

import (
	"encoding/json"
	"strings"

	"github.com/apache/arrow/go/arrow"
)

func FormatJsonlToMap(data []byte) ([]map[string]interface{}, error) {
	lines := strings.Split(string(data), "\n")

	records := make([]map[string]interface{}, 0)
	for _, l := range lines {
		var e map[string]interface{}
		if err := json.Unmarshal([]byte(l), &e); err != nil {
			return nil, err
		}

		records = append(records, e)
	}

	return records, nil
}

func FormatJsonlToArrow(s *arrow.Schema, data []byte) (*WrappedRecord, error) {
	maps, err := FormatJsonlToMap(data)
	if err != nil {
		return nil, err
	}

	return formatMapToArrow(s, maps)
}
