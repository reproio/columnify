package columnifier

import (
	"io/ioutil"

	"github.com/reproio/columnify/record"

	"github.com/reproio/columnify/parquet"
	"github.com/reproio/columnify/schema"
	"github.com/xitongsys/parquet-go-source/local"
	parquetSource "github.com/xitongsys/parquet-go/source"
	"github.com/xitongsys/parquet-go/writer"
)

type parquetColumnifier struct {
	w      *writer.ParquetWriter
	schema *schema.IntermediateSchema
	rt     string
}

func NewParquetColumnifier(st string, sf string, rt string, output string) (*parquetColumnifier, error) {
	schemaContent, err := ioutil.ReadFile(sf)
	if err != nil {
		return nil, err
	}

	intermediateSchema, err := schema.GetSchema(schemaContent, st)
	if err != nil {
		return nil, err
	}

	sh, err := schema.NewSchemaHandlerFromArrow(*intermediateSchema)
	if err != nil {
		return nil, err
	}

	var fw parquetSource.ParquetFile
	if output != "" {
		fw, err = local.NewLocalFileWriter(output)
		if err != nil {
			return nil, err
		}
	} else {
		fw = parquet.NewStdioFile()
	}

	w, err := writer.NewParquetWriter(fw, nil, 1)
	if err != nil {
		return nil, err
	}
	w.SchemaHandler = sh
	w.Footer.Schema = append(w.Footer.Schema, sh.SchemaElements...)

	return &parquetColumnifier{
		w:      w,
		schema: intermediateSchema,
		rt:     rt,
	}, nil
}

func (c *parquetColumnifier) Write(data []byte) error {
	// Intermediate record type is map[string]interface{}
	c.w.MarshalFunc = parquet.MarshalMap
	records, err := record.FormatToMap(data, c.schema, c.rt)
	if err != nil {
		return err
	}

	for _, r := range records {
		if err := c.w.Write(r); err != nil {
			return err
		}
	}

	// Intermediate record type is wrapped Apache Arrow record
	// It requires Arrow Golang implementation more logical type supports
	// ref. https://github.com/apache/arrow/blob/9c9dc2012266442d0848e4af0cf52874bc4db151/go/arrow/array/builder.go#L211
	/*
		c.w.MarshalFunc = parquet.MarshalArrow
		records, err := record.FormatToArrow(data, c.schema, c.rt)
		if err != nil {
			return err
		}
		if err := c.w.Write(&records); err != nil {
			return err
		}
	*/

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
