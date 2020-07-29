# columnify

![Go](https://github.com/reproio/columnify/workflows/Go/badge.svg)
![goreleaser](https://github.com/reproio/columnify/workflows/goreleaser/badge.svg)
[![codecov](https://codecov.io/gh/reproio/columnify/branch/master/graph/badge.svg)](https://codecov.io/gh/reproio/columnify)

Make record oriented data to columnar format.

## Synopsis

Columnar formatted data is efficient for analytics queries, lightweight and ease to integrate with Data WareHouse middleware's. Conversion from record oriented data to columnar is sometimes realized by BigData stack like Hadoop ecosystem, and there's no easy way to do it lightly and quickly.

*columnify* is an easy conversion tool for columnar that enables to run single binary written in Go. It also supports some kinds of data format like `JSONL(NewLine delimited JSON)`, `Avro`.

### How to use

### Installation

```sh
$ GO111MODULE=off go get github.com/reproio/columnify/cmd/columnify
```

### Usage

```sh
$ ./columnify -h
Usage of columnify: columnify [-flags] [input files]
  -output string
        path to output file; default: stdout
  -recordType string
        data type, [avro|csv|jsonl|ltsv|msgpack|tsv] (default "jsonl")
  -schemaFile string
        path to schema file
  -schemaType string
        schema type, [avro|bigquery]
```

### Example

```sh
$ cat examples/record/primitives.jsonl
{"boolean": false, "int": 1, "long": 1, "float": 1.1, "double": 1.1, "bytes": "foo", "string": "foo"}
{"boolean": true, "int": 2, "long": 2, "float": 2.2, "double": 2.2, "bytes": "bar", "string": "bar"}

$ ./columnify -schemaType avro -schemaFile examples/primitives.avsc -recordType jsonl examples/primitives.jsonl > out.parquet

$ parquet-tools schema out.parquet
message Primitives {
  required boolean boolean;
  required int32 int;
  required int64 long;
  required float float;
  required double double;
  required binary bytes;
  required binary string (UTF8);
}

$ parquet-tools cat -json out.parquet
{"boolean":false,"int":1,"long":1,"float":1.1,"double":1.1,"bytes":"Zm9v","string":"foo"}
{"boolean":true,"int":2,"long":2,"float":2.2,"double":2.2,"bytes":"YmFy","string":"bar"}
```

## Supported formats

### Input

- [Apache Avro](https://avro.apache.org/docs/1.8.2/spec.html)
- CSV
- JSONL(NewLine delimited JSON)
- LTSV
- [Message Pack](https://msgpack.org/)
- TSV

### Output

- [Apache Parquet](https://parquet.apache.org/)

### Schema

- [Apache Avro](https://avro.apache.org/docs/1.8.2/spec.html)
- [BigQuery Schema](https://cloud.google.com/bigquery/docs/schemas?hl=ja#specifying_a_json_schema_file)

## Integration example

- [fluent-plugin-s3](https://github.com/fluent/fluent-plugin-s3) parquet compressor

  - An example is `examples/fluent-plugin-s3`
  - It works as a Compressor of fluent-plugin-s3 write parquet file to tmp via chunk data.

## Limilations

Currently it has some limitations from schema/record types.

- Some logical types like `Decimal` are unsupported.
- If using `-recordType = avro`, it doesn't support a nested record has only 1 sub field.
- If using `-recordType = avro`, it converts bytes fields to base64 encoded value implicitly.
- The supported values have limitations with considering to record types, e.g. if you use `jsonl`, it might not be able to handle a large value.

## Development

`Columnifier` reads input file(s), converts format based on given parameter, finally writes output files.
Format conversion is separated by schema / record. The `schema` conversion accepts input schema, then converts it to targets via Arrow's schema. And also the `record` conversion uses Arrow's Record as the intermediate data representation.
`columnify` basically depends on existing modules but it contains additional modules like `arrow`, `avro`, `parquet` to fill insufficient features.

## Release

[goreleaser](https://github.com/goreleaser/goreleaser) is integrated in GitHub Actions. It's triggerd on creating a new tag. Create a new release with semvar tag(`vx.y.z`) on this GitHub repo, then you get archives for some environments attached on the release.
