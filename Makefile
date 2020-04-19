.PHONY: init
init:
	GO111MODULE=off go get -u github.com/golangci/golangci-lint/cmd/golangci-lint
	GO111MODULE=off go get -u github.com/goreleaser/goreleaser

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
	go test -race -cover ./...

.PHONY: it
it: build
	./columnify -schemaType avro -schemaFile examples/schema/primitives.avsc -recordType avro examples/record/primitives.avro > /dev/null
	./columnify -schemaType avro -schemaFile examples/schema/primitives.avsc -recordType csv examples/record/primitives.csv > /dev/null
	./columnify -schemaType avro -schemaFile examples/schema/primitives.avsc -recordType jsonl examples/record/primitives.jsonl > /dev/null
	./columnify -schemaType avro -schemaFile examples/schema/primitives.avsc -recordType ltsv examples/record/primitives.ltsv > /dev/null
	./columnify -schemaType avro -schemaFile examples/schema/primitives.avsc -recordType msgpack examples/record/primitives.msgpack > /dev/null
	./columnify -schemaType avro -schemaFile examples/schema/primitives.avsc -recordType tsv examples/record/primitives.tsv > /dev/null
	./columnify -schemaType avro -schemaFile examples/schema/nested.avsc -recordType avro examples/record/nested.avro > /dev/null
	./columnify -schemaType avro -schemaFile examples/schema/nested.avsc -recordType jsonl examples/record/nested.jsonl > /dev/null
	./columnify -schemaType avro -schemaFile examples/schema/nested.avsc -recordType msgpack examples/record/nested.msgpack > /dev/null
	./columnify -schemaType avro -schemaFile examples/schema/array.avsc -recordType avro examples/record/array.avro > /dev/null
	./columnify -schemaType avro -schemaFile examples/schema/array.avsc -recordType jsonl examples/record/array.jsonl > /dev/null
	./columnify -schemaType avro -schemaFile examples/schema/array.avsc -recordType msgpack examples/record/array.msgpack > /dev/null
	./columnify -schemaType avro -schemaFile examples/schema/complicated.avsc -recordType avro examples/record/complicated.avro > /dev/null
	./columnify -schemaType avro -schemaFile examples/schema/complicated.avsc -recordType jsonl examples/record/complicated.json > /dev/null
	./columnify -schemaType bigquery -schemaFile examples/schema/primitives.bq.json -recordType avro examples/record/primitives.avro > /dev/null
	./columnify -schemaType bigquery -schemaFile examples/schema/primitives.bq.json -recordType csv examples/record/primitives.csv > /dev/null
	./columnify -schemaType bigquery -schemaFile examples/schema/primitives.bq.json -recordType jsonl examples/record/primitives.jsonl > /dev/null
	./columnify -schemaType bigquery -schemaFile examples/schema/primitives.bq.json -recordType ltsv examples/record/primitives.ltsv > /dev/null
	./columnify -schemaType bigquery -schemaFile examples/schema/primitives.bq.json -recordType msgpack examples/record/primitives.msgpack > /dev/null
	./columnify -schemaType bigquery -schemaFile examples/schema/primitives.bq.json -recordType tsv examples/record/primitives.tsv > /dev/null
	./columnify -schemaType bigquery -schemaFile examples/schema/nested.bq.json -recordType avro examples/record/nested.avro > /dev/null
	./columnify -schemaType bigquery -schemaFile examples/schema/nested.bq.json -recordType jsonl examples/record/nested.jsonl > /dev/null
	./columnify -schemaType bigquery -schemaFile examples/schema/nested.bq.json -recordType msgpack examples/record/nested.msgpack > /dev/null
	./columnify -schemaType bigquery -schemaFile examples/schema/array.bq.json -recordType avro examples/record/array.avro > /dev/null
	./columnify -schemaType bigquery -schemaFile examples/schema/array.bq.json -recordType jsonl examples/record/array.jsonl > /dev/null
	./columnify -schemaType bigquery -schemaFile examples/schema/array.bq.json -recordType msgpack examples/record/array.msgpack > /dev/null

# Set GITHUB_TOKEN and create release git tag
.PHONY: release
release:
	goreleaser --rm-dist
