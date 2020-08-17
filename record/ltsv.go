package record

import (
	"bufio"
	"io"
	"strconv"

	"github.com/Songmu/go-ltsv"
)

type ltsvInnerDecoder struct {
	s *bufio.Scanner
}

func newLtsvInnerDecoder(r io.Reader) *ltsvInnerDecoder {
	return &ltsvInnerDecoder{
		s: bufio.NewScanner(r),
	}
}

func (d *ltsvInnerDecoder) Decode(r *map[string]interface{}) error {
	if d.s.Scan() {
		data := d.s.Bytes()

		m := map[string]string{}
		err := ltsv.Unmarshal(data, &m)
		if err != nil {
			return err
		}

		*r = make(map[string]interface{})
		for k, v := range m {
			// bool
			if v != "0" && v != "1" {
				if vv, err := strconv.ParseBool(v); err == nil {
					(*r)[k] = vv
					continue
				}
			}

			// int
			if vv, err := strconv.ParseInt(v, 10, 64); err == nil {
				(*r)[k] = vv
				continue
			}

			// float
			if vv, err := strconv.ParseFloat(v, 64); err == nil {
				(*r)[k] = vv
				continue
			}

			// others; to string
			(*r)[k] = v
		}
	} else {
		if err := d.s.Err(); err != nil {
			return err
		}
		return io.EOF
	}

	return d.s.Err()
}
