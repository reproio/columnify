.PHONY: build
build:
	go build cmd/columnify/columnify.go

.PHONY: clean
clean:
	rm columnify

.PHONY: test
test:
	go test ./...

.PHONY: it
it: build
	./columnify -schemaType avro -schemaFile examples/json/primitives.avsc -dataType csv examples/csv/primitives.csv > /dev/null
	./columnify -schemaType avro -schemaFile examples/json/nested.avsc -dataType jsonl examples/json/nested.jsonl > /dev/null
	./columnify -schemaType avro -schemaFile examples/csv/primitives.avsc -dataType jsonl examples/json/primitives.jsonl > /dev/null
	./columnify -schemaType avro -schemaFile examples/tsv/primitives.avsc -dataType ltsv examples/ltsv/primitives.ltsv > /dev/null
	./columnify -schemaType avro -schemaFile examples/tsv/primitives.avsc -dataType tsv examples/tsv/primitives.tsv > /dev/null
