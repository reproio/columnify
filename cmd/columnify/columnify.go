package main

import (
	"flag"
	"fmt"
	"log"

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

func main() {
	flag.Usage = printUsage

	schemaType := flag.String("schemaType", "", "schema type, [avro|bigquery]")
	schemaFile := flag.String("schemaFile", "", "path to schema file")
	recordType := flag.String("recordType", "jsonl", "record data format type, [avro|csv|jsonl|ltsv|msgpack|tsv]")
	output := flag.String("output", "", "path to output file; default: stdout")

	flag.Parse()

	files := flag.Args()

	c, err := columnifier.NewColumnifier(*schemaType, *schemaFile, *recordType, *output)
	if err != nil {
		log.Fatalf("Failed to init: %v\n", err)
	}
	defer func() {
		if err := c.Close(); err != nil {
			log.Fatalf("Failed to close: %v\n", err)
		}
	}()

	_, err = c.WriteFromFiles(files)
	if err != nil {
		log.Fatalf("Failed to write: %v\n", err)
	}

	err = c.Finalize()
	if err != nil {
		log.Fatalf("Failed to finalize: %v\n", err)
	}
}
