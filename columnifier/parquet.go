package columnifier

import (
	"fmt"
	"github.com/apache/arrow/go/arrow"
	columnifyParquet "github.com/repro/columnify/parquet"
	"github.com/repro/columnify/schema/sink/parquet"
	"github.com/repro/columnify/schema/source/avro"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/schema"
	"github.com/xitongsys/parquet-go/source"
	"github.com/xitongsys/parquet-go/writer"
	"io/ioutil"
	"strings"
)

type parquetColumnifier struct {
	w  *writer.ParquetWriter
	dt string
	s  *arrow.Schema
}

func NewParquetColumnifier(st string, sf string, dt string, output string) (*parquetColumnifier, error) {
	schemaContent, err := ioutil.ReadFile(sf)
	if err != nil {
		return nil, err
	}

	var s *arrow.Schema
	var sh *schema.SchemaHandler
	switch st {
	case schemaTypeAvro:
		arrowSchema, err := avro.NewArrowSchemaFromAvroSchema(schemaContent)
		if err != nil {
			return nil, err
		}
		s = arrowSchema.ArrowSchema
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

	switch dt {
	case dataTypeJsonl:
		// w.MarshalFunc = marshal.MarshalJSON
		w.MarshalFunc = columnifyParquet.MarshalArrow
	default:
		return nil, fmt.Errorf("unsupported data type: %s", dt)
	}

	return &parquetColumnifier{
		w:  w,
		dt: dt,
		s:  s,
	}, nil
}

func (c *parquetColumnifier) Write(data []byte) error {
	switch c.dt {
	case dataTypeJsonl:
		r, err := FromJsonlToArrow(c.s, strings.Split(string(data), "\n"))
		if err != nil {
			return err
		}
		err = c.w.Write(r)
		if err != nil {
			return err
		}
		/*
		for _, j := range strings.Split(string(data), "\n") {
			err := c.w.Write(j)
			if err != nil {
				return err
			}
		}
		*/
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
