package record

import (
	"encoding/json"
	"strings"

	"github.com/Songmu/go-ltsv"
)

func FormatLtsv(data []byte) ([]string, error) {
	lines := strings.Split(string(data), "\n")

	records := make([]string, 0)
	for _, l := range lines {
		v := map[string]string{}

		err := ltsv.Unmarshal([]byte(l), &v)
		if err != nil {
			return nil, err
		}

		marshaled, err := json.Marshal(v)
		if err != nil {
			return nil, err
		}

		records = append(records, string(marshaled))
	}

	return records, nil
}
