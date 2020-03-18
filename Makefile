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
	./columnify -schemaType avro -schemaFile examples/primitives.avsc -dataType csv examples/primitives.csv > /dev/null
	./columnify -schemaType avro -schemaFile examples/primitives.avsc -dataType jsonl examples/primitives.jsonl > /dev/null
	./columnify -schemaType avro -schemaFile examples/primitives.avsc -dataType ltsv examples/primitives.ltsv > /dev/null
	./columnify -schemaType avro -schemaFile examples/primitives.avsc -dataType tsv examples/primitives.tsv > /dev/null
	./columnify -schemaType avro -schemaFile examples/nested.avsc -dataType jsonl examples/nested.jsonl > /dev/null
	./columnify -schemaType avro -schemaFile examples/array.avsc -dataType jsonl examples/array.jsonl > /dev/null
