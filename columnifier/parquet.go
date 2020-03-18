package columnifier

import (
	"fmt"
	"github.com/repro/columnify/record"
	"io/ioutil"

	"github.com/repro/columnify/parquetgo"
	"github.com/repro/columnify/schema"
	"github.com/xitongsys/parquet-go-source/local"
	parquetSchema "github.com/xitongsys/parquet-go/schema"
	parquetSource "github.com/xitongsys/parquet-go/source"
	"github.com/xitongsys/parquet-go/writer"
)

type parquetColumnifier struct {
	w  *writer.ParquetWriter
	dt string
}

func NewParquetColumnifier(st string, sf string, dt string, output string) (*parquetColumnifier, error) {
	schemaContent, err := ioutil.ReadFile(sf)
	if err != nil {
		return nil, err
	}

	var sh *parquetSchema.SchemaHandler
	switch st {
	case schemaTypeAvro:
		intermediateSchema, err := schema.NewSchemaFromAvroSchema(schemaContent)
		if err != nil {
			return nil, err
		}
		sh, err = schema.NewSchemaHandlerFromArrow(*intermediateSchema)
		if err != nil {
			return nil, err
		}
	case schemaTypeBigquery:
		intermediateSchema, err := schema.NewSchemaFromBigQuerySchema(schemaContent)
		if err != nil {
			return nil, err
		}
		sh, err = schema.NewSchemaHandlerFromArrow(*intermediateSchema)
		if err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("unsupported schema type: %s", st)
	}

	var fw parquetSource.ParquetFile
	if output != "" {
		fw, err = local.NewLocalFileWriter(output)
		if err != nil {
			return nil, err
		}
	} else {
		fw = parquetgo.NewStdioFile()
	}

	w, err := writer.NewParquetWriter(fw, nil, 1)
	if err != nil {
		return nil, err
	}
	w.SchemaHandler = sh
	w.Footer.Schema = append(w.Footer.Schema, sh.SchemaElements...)


	return &parquetColumnifier{
		w:  w,
		dt: dt,
	}, nil
}

func (c *parquetColumnifier) Write(data []byte) error {
	var records []map[string]interface{}
	var err error

	// Consider intermediate record type is map[string]interface{}
	c.w.MarshalFunc = parquetgo.MarshalMap

	switch c.dt {
	case dataTypeCsv:
		records, err = record.FormatCsv(c.w.SchemaHandler, data, record.CsvDelimiter)
		if err != nil {
			return err
		}

	case dataTypeJsonl:
		records, err = record.FormatJsonl(data)
		if err != nil {
			return err
		}

	case dataTypeLtsv:
		records, err = record.FormatLtsv(data)
		if err != nil {
			return err
		}

	case dataTypeTsv:
		records, err = record.FormatCsv(c.w.SchemaHandler, data, record.TsvDelimiter)
		if err != nil {
			return err
		}

	default:
		return fmt.Errorf("unsupported data type: %s", c.dt)
	}

	for _, r := range records {
		if err := c.w.Write(r); err != nil {
			return err
		}
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
