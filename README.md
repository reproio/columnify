# columnify

Make record oriented data to columnar format.

## Synopsis

Columnar formatted data is efficient for analytics queries, lightweight and ease to integrate with Data WareHouse middleware's. Conversion from record oriented data to columnar is sometimes realized by BigData stack like Hadoop ecosystem, and there's no easy way to do it lightly and quickly.

*columnify* is an easy conversion tool for columnar that enables to run single binary written in Go. It also supports some kinds of data format like `JSONL(NewLine delimited JSON)`, `Avro`.

## Usage

```sh
$ ./columnify -h
Usage of columnify: columnify [-flags] [input files]
  -dataType string
        data type, [jsonl] (default "jsonl")
  -output string
        path to output file; default: stdout
  -schemaFile string
        path to schema file
  -schemaType string
        schema type, [avro|json]
```

### Example

```sh
$ cat examples/avro/primitives.avsc
{
  "type": "record",
  "name": "Primitives",
  "fields" : [
    {"name": "boolean", "type": "boolean"},
    {"name": "int",     "type": "int"},
    {"name": "long",    "type": "long"},
    {"name": "float",   "type": "float"},
    {"name": "double",  "type": "double"},
    {"name": "bytes",   "type": "bytes"},
    {"name": "string",  "type": "string"}
  ]
}
$ cat examples/avro/primitives.jsonl
{"boolean": false, "int": 42, "long": 420, "float": 4.2, "double": 44.22, "bytes": "bytes", "string": "string"}

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
{"boolean":false,"int":42,"long":420,"float":4.2,"double":44.22,"bytes":"Ynl0ZXM=","string":"string"}
```

## Supported formats

### Input

- Apache Avro
- CSV
- JSONL(NewLine delimited JSON)
- LTSV
- Message Pack
- TSV

### Output

- Apache Parquet

### Schema

- [Apache Avro](https://avro.apache.org/docs/1.8.2/spec.html)
- [BigQuery Schema](https://cloud.google.com/bigquery/docs/schemas?hl=ja#specifying_a_json_schema_file)
