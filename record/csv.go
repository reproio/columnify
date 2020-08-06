package record

import (
	"encoding/csv"
	"fmt"
	"io"
	"strconv"

	"github.com/reproio/columnify/schema"
)

type delimiter rune

const (
	CsvDelimiter delimiter = ','
	TsvDelimiter delimiter = '\t'
)

type csvInnerDecoder struct {
	r     *csv.Reader
	names []string
}

func newCsvInnerDecoder(r io.Reader, s *schema.IntermediateSchema, delimiter delimiter) (*csvInnerDecoder, error) {
	names, err := getFieldNamesFromSchema(s)
	if err != nil {
		return nil, err
	}

	reader := csv.NewReader(r)
	reader.Comma = rune(delimiter)

	return &csvInnerDecoder{
		r:     reader,
		names: names,
	}, nil
}

func (d *csvInnerDecoder) Decode(r *map[string]interface{}) error {
	values, err := d.r.Read()
	if err != nil {
		return err
	}

	if len(d.names) != len(values) {
		return fmt.Errorf("incompleted value %v: %w", values, ErrUnconvertibleRecord)
	}

	*r = make(map[string]interface{})
	for i, v := range values {
		n := d.names[i]

		// bool
		if v != "0" && v != "1" {
			if vv, err := strconv.ParseBool(v); err == nil {
				(*r)[n] = vv
				continue
			}
		}

		// int
		if vv, err := strconv.ParseInt(v, 10, 64); err == nil {
			(*r)[n] = vv
			continue
		}

		// float
		if vv, err := strconv.ParseFloat(v, 64); err == nil {
			(*r)[n] = vv
			continue
		}

		// others; to string
		(*r)[n] = v
	}

	return nil
}

func getFieldNamesFromSchema(s *schema.IntermediateSchema) ([]string, error) {
	elems := s.ArrowSchema.Fields()

	if len(elems) == 0 {
		return nil, fmt.Errorf("no element is available: %w", ErrUnconvertibleRecord)
	}

	names := make([]string, 0, len(elems))
	for _, e := range elems {
		names = append(names, e.Name)
	}

	return names, nil
}
