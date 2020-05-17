package record

import (
	"strconv"
	"strings"

	"github.com/reproio/columnify/schema"

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

		m := make(map[string]interface{})
		for k, v := range v {
			// bool
			if v != "0" && v != "1" {
				if vv, err := strconv.ParseBool(v); err == nil {
					m[k] = vv
					continue
				}
			}

			// int
			if vv, err := strconv.ParseInt(v, 10, 64); err == nil {
				m[k] = vv
				continue
			}

			// float
			if vv, err := strconv.ParseFloat(v, 64); err == nil {
				m[k] = vv
				continue
			}

			// others; to string
			m[k] = v
		}

		records = append(records, m)
	}

	return records, nil
}

func FormatLtsvToArrow(s *schema.IntermediateSchema, data []byte) (*WrappedRecord, error) {
	maps, err := FormatLtsvToMap(data)
	if err != nil {
		return nil, err
	}

	return formatMapToArrowRecord(s.ArrowSchema, maps)
}
