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

func columnify(c columnifier.Columnifier, files []string) (err error) {
	defer func() {
		err = c.Close()
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

	flag.Parse()

	files := flag.Args()

	if *schemaType == "" || *schemaFile == "" || len(files) == 0 {
		printUsage()
		log.Fatalf("Missed required parameter(s)")
	}

	c, err := columnifier.NewColumnifier(*schemaType, *schemaFile, *recordType, *output)
	if err != nil {
		log.Fatalf("Failed to init: %v\n", err)
	}

	if err := columnify(c, files); err != nil {
		log.Fatalf("Failed to write: %v\n", err)
	}
}
