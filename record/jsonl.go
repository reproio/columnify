package record

import (
	"bufio"
	"encoding/json"
	"io"
	"strings"

	"github.com/reproio/columnify/schema"
)

type jsonlInnerDecoder struct {
	s *bufio.Scanner
}

func newJsonlInnerDecoder(r io.Reader) *jsonlInnerDecoder {
	return &jsonlInnerDecoder{
		s: bufio.NewScanner(r),
	}
}

func (d *jsonlInnerDecoder) Decode(r *map[string]interface{}) error {
	if d.s.Scan() {
		if err := json.Unmarshal(d.s.Bytes(), r); err != nil {
			return err
		}
	} else {
		return io.EOF
	}

	return d.s.Err()
}

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
	maps, err := FormatJsonlToMap(data)
	if err != nil {
		return nil, err
	}

	return formatMapToArrowRecord(s.ArrowSchema, maps)
}
