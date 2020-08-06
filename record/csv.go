package record

import (
	"encoding/csv"
	"fmt"
	"io"
	"strconv"
	"strings"

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

func FormatCsvToMap(s *schema.IntermediateSchema, data []byte, delimiter delimiter) ([]map[string]interface{}, error) {
	names, err := getFieldNamesFromSchema(s)
	if err != nil {
		return nil, err
	}

	reader := csv.NewReader(strings.NewReader(string(data)))
	reader.Comma = rune(delimiter)

	numFields := len(names)
	arr := make([]map[string]interface{}, 0)
	for {
		values, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		if numFields != len(values) {
			return nil, fmt.Errorf("incompleted value %v: %w", values, ErrUnconvertibleRecord)
		}

		e := make(map[string]interface{})
		for i, v := range values {
			// bool
			if v != "0" && v != "1" {
				if vv, err := strconv.ParseBool(v); err == nil {
					e[names[i]] = vv
					continue
				}
			}

			// int
			if vv, err := strconv.ParseInt(v, 10, 64); err == nil {
				e[names[i]] = vv
				continue
			}

			// float
			if vv, err := strconv.ParseFloat(v, 64); err == nil {
				e[names[i]] = vv
				continue
			}

			// others; to string
			e[names[i]] = v
		}

		arr = append(arr, e)
	}

	return arr, nil
}

func FormatCsvToArrow(s *schema.IntermediateSchema, data []byte, delimiter delimiter) (*WrappedRecord, error) {
	maps, err := FormatCsvToMap(s, data, delimiter)
	if err != nil {
		return nil, err
	}

	return formatMapToArrowRecord(s.ArrowSchema, maps)
}
