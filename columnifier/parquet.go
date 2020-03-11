package columnifier

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	columnifyParquet "github.com/repro/columnify/parquetgo"
	"github.com/repro/columnify/schema/sink/parquet"
	"github.com/repro/columnify/schema/source/avro"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/marshal"
	"github.com/xitongsys/parquet-go/schema"
	"github.com/xitongsys/parquet-go/source"
	"github.com/xitongsys/parquet-go/writer"
	"io"
	"io/ioutil"
	"strings"
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

func formatCsvToJson(names []string, data []byte) ([]string, error) {
	reader := csv.NewReader(strings.NewReader(string(data)))

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

type parquetColumnifier struct {
	w  *writer.ParquetWriter
	dt string
}

func NewParquetColumnifier(st string, sf string, dt string, output string) (*parquetColumnifier, error) {
	schemaContent, err := ioutil.ReadFile(sf)
	if err != nil {
		return nil, err
	}

	var sh *schema.SchemaHandler
	switch st {
	case schemaTypeAvro:
		arrowSchema, err := avro.NewArrowSchemaFromAvroSchema(schemaContent)
		if err != nil {
			return nil, err
		}
		sh, err = parquet.NewSchemaHandlerFromArrow(*arrowSchema)
		if err != nil {
			return nil, err
		}
	case schemaTypeJson:
		sh, err = schema.NewSchemaHandlerFromJSON(string(schemaContent))
		if err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("unsupported schema type: %s", st)
	}

	var fw source.ParquetFile
	if output != "" {
		fw, err = local.NewLocalFileWriter(output)
		if err != nil {
			return nil, err
		}
	} else {
		fw = columnifyParquet.NewStdioFile()
	}

	w, err := writer.NewParquetWriter(fw, nil, 1)
	if err != nil {
		return nil, err
	}
	w.SchemaHandler = sh
	w.Footer.Schema = append(w.Footer.Schema, sh.SchemaElements...)

	// TODO switch marshaler based on data type
	// NOTE Use JSONL as intermediate representation temporarily
	w.MarshalFunc = marshal.MarshalJSON

	return &parquetColumnifier{
		w:  w,
		dt: dt,
	}, nil
}

func (c *parquetColumnifier) Write(data []byte) error {
	switch c.dt {
	case dataTypeCsv:
		fieldNames, err := getFieldNamesFromSchemaHandler(c.w.SchemaHandler)
		if err != nil {
			return err
		}
		jsonArr, err := formatCsvToJson(fieldNames, data)
		if err != nil {
			return err
		}
		for _, e := range jsonArr {
			err := c.w.Write(e)
			if err != nil {
				return err
			}
		}

	case dataTypeJsonl:
		for _, j := range strings.Split(string(data), "\n") {
			err := c.w.Write(j)
			if err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("unsupported data type: %s", c.dt)
	}

	return nil
}

func (c *parquetColumnifier) WriteFromFiles(paths []string) error {
	for _, p := range paths {
		data, err := ioutil.ReadFile(p)
		if err != nil {
			return err
		}
		if err := c.Write(data); err != nil {
			return err
		}
	}

	return nil
}

func (c *parquetColumnifier) Flush() error {
	if err := c.w.WriteStop(); err != nil {
		return err
	}

	return c.w.PFile.Close()
}
