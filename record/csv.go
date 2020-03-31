package record

import (
	"encoding/csv"
	"fmt"
	"io"
	"strings"

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
		return nil, fmt.Errorf("no element is available for format")
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
			return nil, fmt.Errorf("values are incompleted: %v", values)
		}

		e := make(map[string]interface{}, 0)
		for i, v := range values {
			// TODO cast to actual types
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
