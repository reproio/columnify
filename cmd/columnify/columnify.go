package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/reproio/columnify/columnifier"
)

func printUsage() {
	_, err := fmt.Fprintf(flag.CommandLine.Output(), "Usage of columnify: columnify [-flags] [input files]\n")
	if err != nil {
		log.Fatal(err)
		return
	}

	flag.PrintDefaults()
}

func columnify(c columnifier.Columnifier, files []string) (err error) {
	defer func() {
		if cerr := c.Close(); cerr != nil {
			if err == nil {
				err = cerr
			} else {
				// Don't overwrite existing errors
				_, _ = fmt.Fprintf(os.Stderr, "Failed to close columnifier: %v", cerr)
			}
		}
	}()

	_, err = c.WriteFromFiles(files)

	return
}

func main() {
	flag.Usage = printUsage

	schemaType := flag.String("schemaType", "", "schema type, [avro|bigquery]")
	schemaFile := flag.String("schemaFile", "", "path to schema file")
	recordType := flag.String("recordType", "jsonl", "record data format type, [avro|csv|jsonl|ltsv|msgpack|tsv]")
	output := flag.String("output", "", "path to output file; default: stdout")

	// parquet specific options
	parquetPageSize := flag.Int64("parquetPageSize", 8*1024, "parquet file page size, default: 8kB")
	parquetRowGroupSize := flag.Int64("parquetRowGroupSize", 128*1024*1024, "parquet file row group size, default: 128MB")
	parquetCompressionCodec := flag.String("parquetCompressionCodec", "SNAPPY", "parquet compression codec, default: SNAPPY")

	flag.Parse()

	files := flag.Args()

	if *schemaType == "" || *schemaFile == "" || len(files) == 0 {
		printUsage()
		log.Fatalf("Missed required parameter(s)")
	}

	config, err := columnifier.NewConfig(*parquetPageSize, *parquetRowGroupSize, *parquetCompressionCodec)
	if err != nil {
		log.Fatalf("Failed to init: %v\n", err)
	}

	c, err := columnifier.NewColumnifier(*schemaType, *schemaFile, *recordType, *output, *config)
	if err != nil {
		log.Fatalf("Failed to init: %v\n", err)
	}

	if err := columnify(c, files); err != nil {
		log.Fatalf("Failed to write: %v\n", err)
	}
}
