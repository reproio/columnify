package record

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/xitongsys/parquet-go/schema"
)

type delimiter rune

const (
	CsvDelimiter delimiter = ','
	TsvDelimiter delimiter = '\t'
)

func getFieldNamesFromSchemaHandler(sh *schema.SchemaHandler) ([]string, error) {
	elems := sh.SchemaElements

	if len(elems) < 2 {
		return nil, fmt.Errorf("no element is available for format")
	}

	names := make([]string, 0, len(elems[1:]))
	for _, e := range elems[1:] {
		names = append(names, e.Name)
	}

	return names, nil
}

func formatCsvToJson(names []string, data []byte, delimiter delimiter) ([]string, error) {
	reader := csv.NewReader(strings.NewReader(string(data)))
	reader.Comma = rune(delimiter)

	numFields := len(names)
	arr := make([]string, 0)
	for {
		values, err := reader.Read()
		if err == io.EOF {
			break
		}

		if err != nil {
			return nil, err
		}

		if numFields != len(values) {
			return nil, fmt.Errorf("value is incompleted")
		}

		e := make(map[string]string, 0)
		for i, v := range values {
			e[names[i]] = v
		}

		marshaled, err := json.Marshal(e)
		if err != nil {
			return nil, err
		}

		arr = append(arr, string(marshaled))
	}

	return arr, nil
}

func FormatCsv(sh *schema.SchemaHandler, data []byte, delimiter delimiter) ([]string, error) {
	fieldNames, err := getFieldNamesFromSchemaHandler(sh)
	if err != nil {
		return nil, err
	}

	records, err := formatCsvToJson(fieldNames, data, delimiter)
	if err != nil {
		return nil, err
	}

	return records, nil
}
