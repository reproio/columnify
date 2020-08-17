package record

import (
	"bufio"
	"encoding/json"
	"io"
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
		if err := d.s.Err(); err != nil {
			return err
		}
		return io.EOF
	}

	return d.s.Err()
}
