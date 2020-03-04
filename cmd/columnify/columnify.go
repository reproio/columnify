package main

import (
	"flag"
	"fmt"
	"log"

	"github.com/repro/columnify/columnifier"
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

	schemaType := flag.String("schemaType", "", "schema type, [avro|json]")
	schemaFile := flag.String("schemaFile", "", "path to schema file")
	dataType := flag.String("dataType", "jsonl", "data type, [jsonl]")
	output := flag.String("output", "", "path to output file; default: stdout")

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
