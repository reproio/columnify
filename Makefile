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
	./columnify -schemaType avro -schemaFile examples/schema/primitives.avsc -dataType csv examples/record/primitives.csv > /dev/null
	./columnify -schemaType avro -schemaFile examples/schema/primitives.avsc -dataType jsonl examples/record/primitives.jsonl > /dev/null
	./columnify -schemaType avro -schemaFile examples/schema/primitives.avsc -dataType ltsv examples/record/primitives.ltsv > /dev/null
	./columnify -schemaType avro -schemaFile examples/schema/primitives.avsc -dataType tsv examples/record/primitives.tsv > /dev/null
	./columnify -schemaType avro -schemaFile examples/schema/nested.avsc -dataType jsonl examples/record/nested.jsonl > /dev/null
	./columnify -schemaType avro -schemaFile examples/schema/array.avsc -dataType jsonl examples/record/array.jsonl > /dev/null
	./columnify -schemaType bigquery -schemaFile examples/schema/primitives.bq.json -dataType csv examples/record/primitives.csv > /dev/null
	./columnify -schemaType bigquery -schemaFile examples/schema/primitives.bq.json -dataType jsonl examples/record/primitives.jsonl > /dev/null
	./columnify -schemaType bigquery -schemaFile examples/schema/primitives.bq.json -dataType ltsv examples/record/primitives.ltsv > /dev/null
	./columnify -schemaType bigquery -schemaFile examples/schema/primitives.bq.json -dataType tsv examples/record/primitives.tsv > /dev/null
	./columnify -schemaType bigquery -schemaFile examples/schema/nested.bq.json -dataType jsonl examples/record/nested.jsonl > /dev/null
	./columnify -schemaType bigquery -schemaFile examples/schema/array.bq.json -dataType jsonl examples/record/array.jsonl > /dev/null
