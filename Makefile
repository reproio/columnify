.PHONY: build
build:
	go build cmd/columnify/columnify.go

.PHONY: clean
clean:
	rm columnify

.PHONY: fmt
fmt:
	gofmt -w **/*.go

.PHONY: test
test:
	go test ./...

.PHONY: it
it: build
	./columnify -schemaType avro -schemaFile examples/schema/primitives.avsc -recordType csv examples/record/primitives.csv > /dev/null
	./columnify -schemaType avro -schemaFile examples/schema/primitives.avsc -recordType jsonl examples/record/primitives.jsonl > /dev/null
	./columnify -schemaType avro -schemaFile examples/schema/primitives.avsc -recordType ltsv examples/record/primitives.ltsv > /dev/null
	./columnify -schemaType avro -schemaFile examples/schema/primitives.avsc -recordType tsv examples/record/primitives.tsv > /dev/null
	./columnify -schemaType avro -schemaFile examples/schema/nested.avsc -recordType jsonl examples/record/nested.jsonl > /dev/null
	./columnify -schemaType avro -schemaFile examples/schema/array.avsc -recordType jsonl examples/record/array.jsonl > /dev/null
	./columnify -schemaType bigquery -schemaFile examples/schema/primitives.bq.json -recordType csv examples/record/primitives.csv > /dev/null
	./columnify -schemaType bigquery -schemaFile examples/schema/primitives.bq.json -recordType jsonl examples/record/primitives.jsonl > /dev/null
	./columnify -schemaType bigquery -schemaFile examples/schema/primitives.bq.json -recordType ltsv examples/record/primitives.ltsv > /dev/null
	./columnify -schemaType bigquery -schemaFile examples/schema/primitives.bq.json -recordType tsv examples/record/primitives.tsv > /dev/null
	./columnify -schemaType bigquery -schemaFile examples/schema/nested.bq.json -recordType jsonl examples/record/nested.jsonl > /dev/null
	./columnify -schemaType bigquery -schemaFile examples/schema/array.bq.json -recordType jsonl examples/record/array.jsonl > /dev/null
