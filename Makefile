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

# TODO Enable -race after we resolve data race in parquet-go
# ref. https://github.com/xitongsys/parquet-go/issues/256
.PHONY: test
test:
	go test -cover ./...

.PHONY: it
it: build
	./columnify -schemaType avro -schemaFile testdata/schema/primitives.avsc -recordType avro testdata/record/primitives.avro > /dev/null
	./columnify -schemaType avro -schemaFile testdata/schema/primitives.avsc -recordType csv testdata/record/primitives.csv > /dev/null
	./columnify -schemaType avro -schemaFile testdata/schema/primitives.avsc -recordType jsonl testdata/record/primitives.jsonl > /dev/null
	./columnify -schemaType avro -schemaFile testdata/schema/primitives.avsc -recordType ltsv testdata/record/primitives.ltsv > /dev/null
	./columnify -schemaType avro -schemaFile testdata/schema/primitives.avsc -recordType msgpack testdata/record/primitives.msgpack > /dev/null
	./columnify -schemaType avro -schemaFile testdata/schema/primitives.avsc -recordType tsv testdata/record/primitives.tsv > /dev/null
	./columnify -schemaType avro -schemaFile testdata/schema/nullables.avsc -recordType avro testdata/record/nullables.avro > /dev/null
	./columnify -schemaType avro -schemaFile testdata/schema/nullables.avsc -recordType jsonl testdata/record/nullables.jsonl > /dev/null
	./columnify -schemaType avro -schemaFile testdata/schema/nullables.avsc -recordType msgpack testdata/record/nullables.msgpack > /dev/null
	./columnify -schemaType avro -schemaFile testdata/schema/logicals.avsc -recordType avro testdata/record/logicals.avro > /dev/null
	./columnify -schemaType avro -schemaFile testdata/schema/logicals.avsc -recordType csv testdata/record/logicals.csv > /dev/null
	./columnify -schemaType avro -schemaFile testdata/schema/logicals.avsc -recordType jsonl testdata/record/logicals.jsonl > /dev/null
	./columnify -schemaType avro -schemaFile testdata/schema/logicals.avsc -recordType ltsv testdata/record/logicals.ltsv > /dev/null
	./columnify -schemaType avro -schemaFile testdata/schema/logicals.avsc -recordType msgpack testdata/record/logicals.msgpack > /dev/null
	./columnify -schemaType avro -schemaFile testdata/schema/logicals.avsc -recordType tsv testdata/record/logicals.tsv > /dev/null
	./columnify -schemaType avro -schemaFile testdata/schema/nested.avsc -recordType avro testdata/record/nested.avro > /dev/null
	./columnify -schemaType avro -schemaFile testdata/schema/nested.avsc -recordType jsonl testdata/record/nested.jsonl > /dev/null
	./columnify -schemaType avro -schemaFile testdata/schema/nested.avsc -recordType msgpack testdata/record/nested.msgpack > /dev/null
	./columnify -schemaType avro -schemaFile testdata/schema/array.avsc -recordType avro testdata/record/array.avro > /dev/null
	./columnify -schemaType avro -schemaFile testdata/schema/array.avsc -recordType jsonl testdata/record/array.jsonl > /dev/null
	./columnify -schemaType avro -schemaFile testdata/schema/array.avsc -recordType msgpack testdata/record/array.msgpack > /dev/null
	./columnify -schemaType avro -schemaFile testdata/schema/logicals.avsc -recordType jsonl testdata/record/logicals.jsonl > /dev/null
	./columnify -schemaType avro -schemaFile testdata/schema/logicals.avsc -recordType avro testdata/record/logicals.avro > /dev/null
	./columnify -schemaType avro -schemaFile testdata/schema/logicals.avsc -recordType msgpack testdata/record/logicals.msgpack > /dev/null
	./columnify -schemaType avro -schemaFile testdata/schema/nullable_complex.avsc -recordType avro testdata/record/nullable_complex.avro > /dev/null
	./columnify -schemaType avro -schemaFile testdata/schema/nullable_complex.avsc -recordType jsonl testdata/record/nullable_complex.jsonl > /dev/null
	./columnify -schemaType avro -schemaFile testdata/schema/nullable_complex.avsc -recordType msgpack testdata/record/nullable_complex.msgpack > /dev/null
	./columnify -schemaType bigquery -schemaFile testdata/schema/primitives.bq.json -recordType avro testdata/record/primitives.avro > /dev/null
	./columnify -schemaType bigquery -schemaFile testdata/schema/primitives.bq.json -recordType csv testdata/record/primitives.csv > /dev/null
	./columnify -schemaType bigquery -schemaFile testdata/schema/primitives.bq.json -recordType jsonl testdata/record/primitives.jsonl > /dev/null
	./columnify -schemaType bigquery -schemaFile testdata/schema/primitives.bq.json -recordType ltsv testdata/record/primitives.ltsv > /dev/null
	./columnify -schemaType bigquery -schemaFile testdata/schema/primitives.bq.json -recordType msgpack testdata/record/primitives.msgpack > /dev/null
	./columnify -schemaType bigquery -schemaFile testdata/schema/primitives.bq.json -recordType tsv testdata/record/primitives.tsv > /dev/null
	./columnify -schemaType bigquery -schemaFile testdata/schema/nullables.bq.json -recordType avro testdata/record/nullables.avro > /dev/null
	./columnify -schemaType bigquery -schemaFile testdata/schema/nullables.bq.json -recordType jsonl testdata/record/nullables.jsonl > /dev/null
	./columnify -schemaType bigquery -schemaFile testdata/schema/nullables.bq.json -recordType msgpack testdata/record/nullables.msgpack > /dev/null
	./columnify -schemaType bigquery -schemaFile testdata/schema/nested.bq.json -recordType avro testdata/record/nested.avro > /dev/null
	./columnify -schemaType bigquery -schemaFile testdata/schema/nested.bq.json -recordType jsonl testdata/record/nested.jsonl > /dev/null
	./columnify -schemaType bigquery -schemaFile testdata/schema/nested.bq.json -recordType msgpack testdata/record/nested.msgpack > /dev/null
	./columnify -schemaType bigquery -schemaFile testdata/schema/array.bq.json -recordType avro testdata/record/array.avro > /dev/null
	./columnify -schemaType bigquery -schemaFile testdata/schema/array.bq.json -recordType jsonl testdata/record/array.jsonl > /dev/null
	./columnify -schemaType bigquery -schemaFile testdata/schema/array.bq.json -recordType msgpack testdata/record/array.msgpack > /dev/null

# Set GITHUB_TOKEN and create release git tag
.PHONY: release
release:
	goreleaser --rm-dist
