package main

import (
	"flag"
	"log"

	"github.com/repro/columnify/columnifier"
)

func main() {
	schemaType := flag.String("schema-type", "", "")
	schemaFile := flag.String("schema-file", "", "")
	dataType := flag.String("data-type", "jsonl", "")
	dataFiles := flag.String("data-files", "", "") // TODO accept pattern
	output := flag.String("output", "", "")

	flag.Parse()

	c, err := columnifier.NewColumnifier(*schemaType, *schemaFile, *dataType, *output)
	if err != nil {
		log.Fatalf("Failed to init: %v\n", err)
	}

	err = c.WriteFromFiles([]string{*dataFiles})
	if err != nil {
		log.Fatalf("Failed to write: %v\n", err)
	}

	err = c.Flush()
	if err != nil {
		log.Fatalf("Failed to flush: %v\n", err)
	}
}
