package main

import (
	"flag"
	"github.com/repro/columnify/columnifier"
	"log"
)

func main() {
	schemaType := flag.String("schemaType", "", "schema type, [avro|json]")
	schemaFile := flag.String("schemaFile", "", "path to schema file")
	dataType := flag.String("dataType", "jsonl", "data type, [jsonl]")
	output := flag.String("output", "", "path to output file")

	flag.Parse()

	files := flag.Args()

	c, err := columnifier.NewColumnifier(*schemaType, *schemaFile, *dataType, *output)
	if err != nil {
		log.Fatalf("Failed to init: %v\n", err)
	}

	err = c.WriteFromFiles(files)
	if err != nil {
		log.Fatalf("Failed to write: %v\n", err)
	}

	err = c.Flush()
	if err != nil {
		log.Fatalf("Failed to flush: %v\n", err)
	}
}
