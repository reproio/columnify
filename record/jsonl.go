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
	nS := bufio.NewScanner(r)
	bufferSize := 16 * 1024 * 1024
	buf := make([]byte, 1024*1024)
	nS.Buffer(buf, bufferSize)
	return &jsonlInnerDecoder{
		s: nS,
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
