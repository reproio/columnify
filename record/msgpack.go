package record

import (
	"bytes"
	"fmt"
	"io"

	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/memory"

	"github.com/reproio/columnify/schema"

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
			return nil, fmt.Errorf("invalid input %v: %w", arr, ErrUnconvertibleRecord)
		}

		maps = append(maps, m)
	}

	return maps, nil
}

func FormatMsgpackToArrow(s *schema.IntermediateSchema, data []byte) (*WrappedRecord, error) {
	pool := memory.NewGoAllocator()
	b := array.NewRecordBuilder(pool, s.ArrowSchema)
	defer b.Release()

	d := msgpack.NewDecoder(bytes.NewReader(data))
	for {
		arr, err := d.DecodeInterface()
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}

		m, mapOk := arr.(map[string]interface{})
		if !mapOk {
			return nil, fmt.Errorf("invalid input %v: %w", arr, ErrUnconvertibleRecord)
		}

		if _, err = formatMapToArrowRecord(b, m); err != nil {
			return nil, err
		}
	}

	return NewWrappedRecord(b), nil
}
