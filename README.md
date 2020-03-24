# columnify

Make record oriented data to columnar format.

## Synopsis

Columnar formatted data is efficient for analytics queries, lightweight and ease to integrate with Data WareHouse middleware's. Conversion from record oriented data to columnar is sometimes realized by BigData stack like Hadoop ecosystem, and there's no easy way to do it lightly and quickly.

*columnify* is an easy conversion tool for columnar that enables to run single binary written in Go. It also supports some kinds of data format like `JSONL(NewLine delimited JSON)`, `Avro`.

## Usage

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
$ cat examples/avro/primitives.jsonl
{"boolean": false, "int": 1, "long": 1, "float": 1.1, "double": 1.1, "bytes": "foo", "string": "foo"}
{"boolean": false, "int": 2, "long": 2, "float": 2.2, "double": 2.2, "bytes": "bar", "string": "bar"}

$ ./columnify -schemaType avro -schemaFile examples/avro/primitives.avsc -dataType jsonl examples/avro/primitives.jsonl > out.parquet

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
{"boolean":false,"int":2,"long":2,"float":2.2,"double":2.2,"bytes":"YmFy","string":"bar"}
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
