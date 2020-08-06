package columnifier

import (
	"io"
	"io/ioutil"
	"os"

	"github.com/reproio/columnify/record"

	"github.com/reproio/columnify/parquet"
	"github.com/reproio/columnify/schema"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/marshal"
	parquetSource "github.com/xitongsys/parquet-go/source"
	"github.com/xitongsys/parquet-go/writer"
)

// Columnifier is a parquet specific Columninifier implementation.
type parquetColumnifier struct {
	w      *writer.ParquetWriter
	schema *schema.IntermediateSchema
	rt     string
}

// NewParquetColumnifier creates a new parquetColumnifier.
func NewParquetColumnifier(st string, sf string, rt string, output string, config Config) (*parquetColumnifier, error) {
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

	w.PageSize = config.Parquet.PageSize
	w.RowGroupSize = config.Parquet.RowGroupSize
	w.CompressionType = config.Parquet.CompressionCodec

	return &parquetColumnifier{
		w:      w,
		schema: intermediateSchema,
		rt:     rt,
	}, nil
}

// Write reads, converts input binary data and write it to buffer.
func (c *parquetColumnifier) WriteFromReader(reader io.Reader) (int, error) {
	// Intermediate record type is map[string]interface{}
	c.w.MarshalFunc = marshal.MarshalJSON
	decoder, err := record.NewJsonDecoder(reader, c.schema, c.rt)
	if err != nil {
		return -1, err
	}

	beforeSize := c.w.Size
	for {
		var v string
		err = decoder.Decode(&v)
		if err != nil {
			if err == io.EOF {
				break
			} else {
				return -1, err
			}
		}

		if err := c.w.Write(v); err != nil {
			return -1, err
		}
	}
	afterSize := c.w.Size

	return int(afterSize - beforeSize), nil
}

// WriteFromFiles reads, converts input binary files.
func (c *parquetColumnifier) WriteFromFiles(paths []string) (int, error) {
	var n int

	for _, p := range paths {
		f, err := os.Open(p)
		if err != nil {
			return -1, err
		}

		if n, err = c.WriteFromReader(f); err != nil {
			return -1, err
		}
	}

	return n, nil
}

// Close stops writing parquet files ant finalize this conversion.
func (c *parquetColumnifier) Close() error {
	if err := c.w.WriteStop(); err != nil {
		return err
	}

	return c.w.PFile.Close()
}
