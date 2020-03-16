package record

import (
	"encoding/json"
	"strings"
)

func FormatJsonl(data []byte) ([]map[string]interface{}, error) {
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
