package record

import (
	"encoding/csv"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/memory"

	"github.com/reproio/columnify/schema"
)

type delimiter rune

const (
	CsvDelimiter delimiter = ','
	TsvDelimiter delimiter = '\t'
)

func getFieldNamesFromSchema(s *schema.IntermediateSchema) ([]string, error) {
	elems := s.ArrowSchema.Fields()

	if len(elems) < 2 {
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
	pool := memory.NewGoAllocator()
	b := array.NewRecordBuilder(pool, s.ArrowSchema)
	defer b.Release()

	names, err := getFieldNamesFromSchema(s)
	if err != nil {
		return nil, err
	}

	reader := csv.NewReader(strings.NewReader(string(data)))
	reader.Comma = rune(delimiter)

	numFields := len(names)
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

		if _, err := formatMapToArrowRecord(b, e); err != nil {
			return nil, err
		}
	}

	return NewWrappedRecord(b), nil
}
