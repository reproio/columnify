package record

import (
	"fmt"
	"io"

	"github.com/vmihailenco/msgpack/v4"
)

type msgpackInnerDecoder struct {
	d *msgpack.Decoder
}

func newMsgpackInnerDecoder(r io.Reader) *msgpackInnerDecoder {
	return &msgpackInnerDecoder{
		d: msgpack.NewDecoder(r),
	}
}

func (d *msgpackInnerDecoder) Decode(r *map[string]interface{}) error {
	arr, err := d.d.DecodeInterface()
	if err != nil {
		return err
	}

	m, mapOk := arr.(map[string]interface{})
	if !mapOk {
		return fmt.Errorf("invalid input %v: %w", arr, ErrUnconvertibleRecord)
	}
	*r = m

	return nil
}
