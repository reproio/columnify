package record

import (
	"strings"

	"github.com/Songmu/go-ltsv"
)

func FormatLtsvToMap(data []byte) ([]map[string]interface{}, error) {
	lines := strings.Split(string(data), "\n")

	records := make([]map[string]interface{}, 0)
	for _, l := range lines {
		v := map[string]string{}

		err := ltsv.Unmarshal([]byte(l), &v)
		if err != nil {
			return nil, err
		}

		m := make(map[string]interface{}, 0)
		for k, v := range v {
			m[k] = v
		}

		records = append(records, m)
	}

	return records, nil
}
