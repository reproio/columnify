.PHONY: init
init:
	GO111MODULE=off go get -u github.com/golangci/golangci-lint/cmd/golangci-lint
	GO111MODULE=off go get -u github.com/goreleaser/goreleaser
	GO111MODULE=off go get -u github.com/Songmu/gocredits/cmd/gocredits

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
	go test -cover -coverprofile=cover.out ./...

.PHONY: it
it: build
	./columnify -schemaType avro -schemaFile columnifier/testdata/schema/primitives.avsc -recordType avro columnifier/testdata/record/primitives.avro > /dev/null
	./columnify -schemaType avro -schemaFile columnifier/testdata/schema/primitives.avsc -recordType csv columnifier/testdata/record/primitives.csv > /dev/null
	./columnify -schemaType avro -schemaFile columnifier/testdata/schema/primitives.avsc -recordType jsonl columnifier/testdata/record/primitives.jsonl > /dev/null
	./columnify -schemaType avro -schemaFile columnifier/testdata/schema/primitives.avsc -recordType ltsv columnifier/testdata/record/primitives.ltsv > /dev/null
	./columnify -schemaType avro -schemaFile columnifier/testdata/schema/primitives.avsc -recordType msgpack columnifier/testdata/record/primitives.msgpack > /dev/null
	./columnify -schemaType avro -schemaFile columnifier/testdata/schema/primitives.avsc -recordType tsv columnifier/testdata/record/primitives.tsv > /dev/null
	./columnify -schemaType avro -schemaFile columnifier/testdata/schema/nullables.avsc -recordType avro columnifier/testdata/record/nullables.avro > /dev/null
	./columnify -schemaType avro -schemaFile columnifier/testdata/schema/nullables.avsc -recordType jsonl columnifier/testdata/record/nullables.jsonl > /dev/null
	./columnify -schemaType avro -schemaFile columnifier/testdata/schema/nullables.avsc -recordType msgpack columnifier/testdata/record/nullables.msgpack > /dev/null
	./columnify -schemaType avro -schemaFile columnifier/testdata/schema/logicals.avsc -recordType avro columnifier/testdata/record/logicals.avro > /dev/null
	./columnify -schemaType avro -schemaFile columnifier/testdata/schema/logicals.avsc -recordType csv columnifier/testdata/record/logicals.csv > /dev/null
	./columnify -schemaType avro -schemaFile columnifier/testdata/schema/logicals.avsc -recordType jsonl columnifier/testdata/record/logicals.jsonl > /dev/null
	./columnify -schemaType avro -schemaFile columnifier/testdata/schema/logicals.avsc -recordType ltsv columnifier/testdata/record/logicals.ltsv > /dev/null
	./columnify -schemaType avro -schemaFile columnifier/testdata/schema/logicals.avsc -recordType msgpack columnifier/testdata/record/logicals.msgpack > /dev/null
	./columnify -schemaType avro -schemaFile columnifier/testdata/schema/logicals.avsc -recordType tsv columnifier/testdata/record/logicals.tsv > /dev/null
	./columnify -schemaType avro -schemaFile columnifier/testdata/schema/nested.avsc -recordType avro columnifier/testdata/record/nested.avro > /dev/null
	./columnify -schemaType avro -schemaFile columnifier/testdata/schema/nested.avsc -recordType jsonl columnifier/testdata/record/nested.jsonl > /dev/null
	./columnify -schemaType avro -schemaFile columnifier/testdata/schema/nested.avsc -recordType msgpack columnifier/testdata/record/nested.msgpack > /dev/null
	./columnify -schemaType avro -schemaFile columnifier/testdata/schema/array.avsc -recordType avro columnifier/testdata/record/array.avro > /dev/null
	./columnify -schemaType avro -schemaFile columnifier/testdata/schema/array.avsc -recordType jsonl columnifier/testdata/record/array.jsonl > /dev/null
	./columnify -schemaType avro -schemaFile columnifier/testdata/schema/array.avsc -recordType msgpack columnifier/testdata/record/array.msgpack > /dev/null
	./columnify -schemaType avro -schemaFile columnifier/testdata/schema/logicals.avsc -recordType jsonl columnifier/testdata/record/logicals.jsonl > /dev/null
	./columnify -schemaType avro -schemaFile columnifier/testdata/schema/logicals.avsc -recordType avro columnifier/testdata/record/logicals.avro > /dev/null
	./columnify -schemaType avro -schemaFile columnifier/testdata/schema/logicals.avsc -recordType msgpack columnifier/testdata/record/logicals.msgpack > /dev/null
	./columnify -schemaType avro -schemaFile columnifier/testdata/schema/nullable_complex.avsc -recordType avro columnifier/testdata/record/nullable_complex.avro > /dev/null
	./columnify -schemaType avro -schemaFile columnifier/testdata/schema/nullable_complex.avsc -recordType jsonl columnifier/testdata/record/nullable_complex.jsonl > /dev/null
	./columnify -schemaType avro -schemaFile columnifier/testdata/schema/nullable_complex.avsc -recordType msgpack columnifier/testdata/record/nullable_complex.msgpack > /dev/null
	./columnify -schemaType bigquery -schemaFile columnifier/testdata/schema/primitives.bq.json -recordType avro columnifier/testdata/record/primitives.avro > /dev/null
	./columnify -schemaType bigquery -schemaFile columnifier/testdata/schema/primitives.bq.json -recordType csv columnifier/testdata/record/primitives.csv > /dev/null
	./columnify -schemaType bigquery -schemaFile columnifier/testdata/schema/primitives.bq.json -recordType jsonl columnifier/testdata/record/primitives.jsonl > /dev/null
	./columnify -schemaType bigquery -schemaFile columnifier/testdata/schema/primitives.bq.json -recordType ltsv columnifier/testdata/record/primitives.ltsv > /dev/null
	./columnify -schemaType bigquery -schemaFile columnifier/testdata/schema/primitives.bq.json -recordType msgpack columnifier/testdata/record/primitives.msgpack > /dev/null
	./columnify -schemaType bigquery -schemaFile columnifier/testdata/schema/primitives.bq.json -recordType tsv columnifier/testdata/record/primitives.tsv > /dev/null
	./columnify -schemaType bigquery -schemaFile columnifier/testdata/schema/nullables.bq.json -recordType avro columnifier/testdata/record/nullables.avro > /dev/null
	./columnify -schemaType bigquery -schemaFile columnifier/testdata/schema/nullables.bq.json -recordType jsonl columnifier/testdata/record/nullables.jsonl > /dev/null
	./columnify -schemaType bigquery -schemaFile columnifier/testdata/schema/nullables.bq.json -recordType msgpack columnifier/testdata/record/nullables.msgpack > /dev/null
	./columnify -schemaType bigquery -schemaFile columnifier/testdata/schema/nested.bq.json -recordType avro columnifier/testdata/record/nested.avro > /dev/null
	./columnify -schemaType bigquery -schemaFile columnifier/testdata/schema/nested.bq.json -recordType jsonl columnifier/testdata/record/nested.jsonl > /dev/null
	./columnify -schemaType bigquery -schemaFile columnifier/testdata/schema/nested.bq.json -recordType msgpack columnifier/testdata/record/nested.msgpack > /dev/null
	./columnify -schemaType bigquery -schemaFile columnifier/testdata/schema/array.bq.json -recordType avro columnifier/testdata/record/array.avro > /dev/null
	./columnify -schemaType bigquery -schemaFile columnifier/testdata/schema/array.bq.json -recordType jsonl columnifier/testdata/record/array.jsonl > /dev/null
	./columnify -schemaType bigquery -schemaFile columnifier/testdata/schema/array.bq.json -recordType msgpack columnifier/testdata/record/array.msgpack > /dev/null

# Set GITHUB_TOKEN and create release git tag
.PHONY: release
release:
	goreleaser --rm-dist

.PHONY: CREDITS
CREDITS:
	rm -f $@
	gocredits -skip-missing . > $@
