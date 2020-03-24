package columnifier

import (
	"io/ioutil"

	"github.com/repro/columnify/record"

	"github.com/repro/columnify/parquetgo"
	"github.com/repro/columnify/schema"
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
		fw = parquetgo.NewStdioFile()
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
	c.w.MarshalFunc = parquetgo.MarshalMap
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
	/*
		c.w.MarshalFunc = parquetgo.MarshalArrow
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
