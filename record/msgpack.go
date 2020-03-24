package record

import (
	"bytes"
	"fmt"
	"io"

	"github.com/vmihailenco/msgpack/v4"
)

func FormatMsgpackToMap(data []byte) ([]map[string]interface{}, error) {
	d := msgpack.NewDecoder(bytes.NewReader(data))

	maps := make([]map[string]interface{}, 0)
	for {
		arr, err := d.DecodeInterface()
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}

		m, mapOk := arr.(map[string]interface{})
		if !mapOk {
			return nil, fmt.Errorf("invalid input: %v", arr)
		}

		maps = append(maps, m)
	}

	return maps, nil
}
