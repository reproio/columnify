package main

import (
	"flag"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/marshal"
	"github.com/xitongsys/parquet-go/schema"
	"github.com/xitongsys/parquet-go/writer"
	"io/ioutil"
	"log"
	"strings"
)

type columnifier struct {
	w *writer.ParquetWriter
}

func newColumnifier(sf string, o string) (*columnifier, error) {
	schemaContent, err := ioutil.ReadFile(sf)
	if err != nil {
		return nil, err
	}

	// TODO check schema type

	// TODO support other types
	sh, err := schema.NewSchemaHandlerFromJSON(string(schemaContent))
	if err != nil {
		return nil, err
	}

	fw, err := local.NewLocalFileWriter(o)
	if err != nil {
		return nil, err
	}

	w, err := writer.NewParquetWriter(fw, nil, 4)
	if err != nil {
		return nil, err
	}
	w.SchemaHandler = sh
	w.Footer.Schema = append(w.Footer.Schema, sh.SchemaElements...)

	return &columnifier{
		w: w,
	}, nil
}

func (c *columnifier) Write(df string) error {
	paths := strings.Split(strings.ReplaceAll(df, " ", ""), ",")

	// TODO support other types
	c.w.MarshalFunc = marshal.MarshalJSON
	for _, p := range paths {
		jsonl, err := ioutil.ReadFile(p)
		if err != nil {
			return err
		}

		for _, j := range strings.Split(string(jsonl), "\n") {
			err = c.w.Write(j)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (c *columnifier) Flush() error {
	if err := c.w.WriteStop(); err != nil {
		return err
	}

	return c.w.PFile.Close()
}

func main() {
	// schemaType := flag.String("schema-type", "", "")
	schemaFile := flag.String("schema-file", "", "")
	// dataType := flag.String("data-type", "", "")
	dataFiles := flag.String("data-files", "", "") // TODO accept pattern
	output := flag.String("output", "", "")

	flag.Parse()

	c, err := newColumnifier(*schemaFile, *output)
	if err != nil {
		log.Fatalf("Failed to init: %v\n", err)
	}

	err = c.Write(*dataFiles)
	if err != nil {
		log.Fatalf("Failed to write: %v\n", err)
	}

	err = c.Flush()
	if err != nil {
		log.Fatalf("Failed to flush: %v\n", err)
	}
}
