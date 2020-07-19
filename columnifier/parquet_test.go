package columnifier

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"os"
	"testing"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"

	"github.com/reproio/columnify/record"
	"github.com/reproio/columnify/schema"
	"github.com/xitongsys/parquet-go/parquet"
)

var defaultConfig = Config{
	Parquet: Parquet{
		PageSize:         8 * 1024,
		RowGroupSize:     128 * 1024 * 1024,
		CompressionCodec: parquet.CompressionCodec_SNAPPY,
	},
}

func assertWrittenParquet(t *testing.T, expectedPath, actualPath string) {
	actualFileReader, err := local.NewLocalFileReader(actualPath)
	if err != nil {
		t.Fatal(err)
	}
	actualParquetReader, err := reader.NewParquetReader(actualFileReader, nil, 1)
	if err != nil {
		t.Fatal(err)
	}
	actualRows, err := actualParquetReader.ReadByNumber(int(actualParquetReader.GetNumRows()))
	if err != nil {
		t.Fatal(err)
	}
	actualJson, err := json.Marshal(actualRows)
	if err != nil {
		t.Fatal(err)
	}

	expectedFileReader, err := local.NewLocalFileReader(expectedPath)
	if err != nil {
		t.Fatal(err)
	}
	expectedParquetReader, err := reader.NewParquetReader(expectedFileReader, nil, 1)
	if err != nil {
		t.Fatal(err)
	}
	expectedRows, err := expectedParquetReader.ReadByNumber(int(expectedParquetReader.GetNumRows()))
	if err != nil {
		t.Fatal(err)
	}
	expectedJson, err := json.Marshal(expectedRows)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(actualJson, expectedJson) {
		t.Errorf("expected %v, but actual %v", string(expectedJson), string(actualJson))
	}
}

func TestNewParquetColumnifier(t *testing.T) {

	cases := []struct {
		st     string
		sf     string
		rt     string
		output string
		config Config
		isErr  bool
	}{
		// Inalid schema type
		{
			st:     "unknown",
			sf:     "testdata/schema/primitives.avsc",
			rt:     record.RecordTypeJsonl,
			output: "",
			config: Config{},
			isErr:  true,
		},

		// Invalid schema file
		{
			st:     schema.SchemaTypeAvro,
			sf:     "/tmp/nonexistence/invalid.schema",
			rt:     record.RecordTypeJsonl,
			output: "",
			config: Config{},
			isErr:  true,
		},

		// Invalid output
		{
			st:     schema.SchemaTypeAvro,
			sf:     "testdata/schema/primitives.avsc",
			rt:     record.RecordTypeJsonl,
			output: "/tmp/nonexistence/invalid.record",
			config: Config{},
			isErr:  true,
		},

		// Valid
		{
			st:     schema.SchemaTypeAvro,
			sf:     "testdata/schema/primitives.avsc",
			rt:     record.RecordTypeJsonl,
			output: "",
			config: Config{},
			isErr:  false,
		},
	}

	for _, c := range cases {
		_, err := NewParquetColumnifier(c.st, c.sf, c.rt, c.output, c.config)

		if err != nil != c.isErr {
			t.Errorf("expected %v, but actual %v", c.isErr, err)
		}
	}
}

func TestWriteClose(t *testing.T) {
	cases := []struct {
		st       string
		sf       string
		rt       string
		input    string
		expected string
	}{
		// primitives; Avro schema, Avro record
		{
			st:       schema.SchemaTypeAvro,
			sf:       "testdata/schema/primitives.avsc",
			rt:       record.RecordTypeAvro,
			input:    "testdata/record/primitives.avro",
			expected: "testdata/parquet/primitives.parquet",
		},
		// primitives; Avro schema, CSV record
		{
			st:       schema.SchemaTypeAvro,
			sf:       "testdata/schema/primitives.avsc",
			rt:       record.RecordTypeCsv,
			input:    "testdata/record/primitives.csv",
			expected: "testdata/parquet/primitives.parquet",
		},
		// primitives; Avro schema, JSONL record
		{
			st:       schema.SchemaTypeAvro,
			sf:       "testdata/schema/primitives.avsc",
			rt:       record.RecordTypeJsonl,
			input:    "testdata/record/primitives.jsonl",
			expected: "testdata/parquet/primitives.parquet",
		},
		// primitives; Avro schema, LTSV record
		{
			st:       schema.SchemaTypeAvro,
			sf:       "testdata/schema/primitives.avsc",
			rt:       record.RecordTypeLtsv,
			input:    "testdata/record/primitives.ltsv",
			expected: "testdata/parquet/primitives.parquet",
		},
		// primitives; Avro schema, MessagePack record
		{
			st:       schema.SchemaTypeAvro,
			sf:       "testdata/schema/primitives.avsc",
			rt:       record.RecordTypeMsgpack,
			input:    "testdata/record/primitives.msgpack",
			expected: "testdata/parquet/primitives.parquet",
		},
		// primitives; Avro schema, TSV record
		{
			st:       schema.SchemaTypeAvro,
			sf:       "testdata/schema/primitives.avsc",
			rt:       record.RecordTypeTsv,
			input:    "testdata/record/primitives.tsv",
			expected: "testdata/parquet/primitives.parquet",
		},
		// nullables; Avro schema, Avro record
		{
			st:       schema.SchemaTypeAvro,
			sf:       "testdata/schema/nullables.avsc",
			rt:       record.RecordTypeAvro,
			input:    "testdata/record/nullables.avro",
			expected: "testdata/parquet/nullables.parquet",
		},
		// nullables; Avro schema, JSONL record
		/*
			{
				st:       schema.SchemaTypeAvro,
				sf:       "testdata/schema/nullables.avsc",
				rt:       record.RecordTypeJsonl,
				input:    "testdata/record/nullables.jsonl",
				expected: "testdata/parquet/nullables.parquet",
			},
		*/
		// nullables; Avro schema, MessagePack record
		/*
			{
				st:       schema.SchemaTypeAvro,
				sf:       "testdata/schema/nullables.avsc",
				rt:       record.RecordTypeMsgpack,
				input:    "testdata/record/nullables.msgpack",
				expected: "testdata/parquet/nullables.parquet",
			},
		*/
		// logicals; Avro schema, Avro record
		{
			st:       schema.SchemaTypeAvro,
			sf:       "testdata/schema/logicals.avsc",
			rt:       record.RecordTypeAvro,
			input:    "testdata/record/logicals.avro",
			expected: "testdata/parquet/logicals.parquet",
		},
		// logicals; Avro schema, CSV record
		{
			st:       schema.SchemaTypeAvro,
			sf:       "testdata/schema/logicals.avsc",
			rt:       record.RecordTypeCsv,
			input:    "testdata/record/logicals.csv",
			expected: "testdata/parquet/logicals.parquet",
		},
		// logicals; Avro schema, JSONL record
		{
			st:       schema.SchemaTypeAvro,
			sf:       "testdata/schema/logicals.avsc",
			rt:       record.RecordTypeJsonl,
			input:    "testdata/record/logicals.jsonl",
			expected: "testdata/parquet/logicals.parquet",
		},
		// logicals; Avro schema, LTSV record
		{
			st:       schema.SchemaTypeAvro,
			sf:       "testdata/schema/logicals.avsc",
			rt:       record.RecordTypeLtsv,
			input:    "testdata/record/logicals.ltsv",
			expected: "testdata/parquet/logicals.parquet",
		},
		// logicals; Avro schema, MessagePack record
		{
			st:       schema.SchemaTypeAvro,
			sf:       "testdata/schema/logicals.avsc",
			rt:       record.RecordTypeMsgpack,
			input:    "testdata/record/logicals.msgpack",
			expected: "testdata/parquet/logicals.parquet",
		},
		// logicals; Avro schema, TSV record
		{
			st:       schema.SchemaTypeAvro,
			sf:       "testdata/schema/logicals.avsc",
			rt:       record.RecordTypeTsv,
			input:    "testdata/record/logicals.tsv",
			expected: "testdata/parquet/logicals.parquet",
		},
		// nested; Avro schema, Avro record
		{
			st:       schema.SchemaTypeAvro,
			sf:       "testdata/schema/nested.avsc",
			rt:       record.RecordTypeAvro,
			input:    "testdata/record/nested.avro",
			expected: "testdata/parquet/nested.parquet",
		},
		// nested; Avro schema, JSONL record
		{
			st:       schema.SchemaTypeAvro,
			sf:       "testdata/schema/nested.avsc",
			rt:       record.RecordTypeJsonl,
			input:    "testdata/record/nested.jsonl",
			expected: "testdata/parquet/nested.parquet",
		},
		// nested; Avro schema, MessagePack record
		{
			st:       schema.SchemaTypeAvro,
			sf:       "testdata/schema/nested.avsc",
			rt:       record.RecordTypeMsgpack,
			input:    "testdata/record/nested.msgpack",
			expected: "testdata/parquet/nested.parquet",
		},
		// array; Avro schema, Avro record
		{
			st:       schema.SchemaTypeAvro,
			sf:       "testdata/schema/array.avsc",
			rt:       record.RecordTypeAvro,
			input:    "testdata/record/array.avro",
			expected: "testdata/parquet/array.parquet",
		},
		// array; Avro schema, JSONL record
		{
			st:       schema.SchemaTypeAvro,
			sf:       "testdata/schema/array.avsc",
			rt:       record.RecordTypeJsonl,
			input:    "testdata/record/array.jsonl",
			expected: "testdata/parquet/array.parquet",
		},
		// array; Avro schema, MessagePack record
		{
			st:       schema.SchemaTypeAvro,
			sf:       "testdata/schema/array.avsc",
			rt:       record.RecordTypeMsgpack,
			input:    "testdata/record/array.msgpack",
			expected: "testdata/parquet/array.parquet",
		},
		// nullable/complex; Avro schema, Avro record
		{
			st:       schema.SchemaTypeAvro,
			sf:       "testdata/schema/nullable_complex.avsc",
			rt:       record.RecordTypeAvro,
			input:    "testdata/record/nullable_complex.avro",
			expected: "testdata/parquet/nullable_complex.parquet",
		},
		// nullable/complex; Avro schema, JSONL record
		// TODO handle some invalid type handling like long
		/*
			{
				st:       schema.SchemaTypeAvro,
				sf:       "testdata/schema/nullable_complex.avsc",
				rt:       record.RecordTypeJsonl,
				input:    "testdata/record/nullable_complex.jsonl",
				expected: "testdata/parquet/nullable_complex.parquet",
			},
		*/
		// nullable/complex; Avro schema, MessagePack record
		// TODO handle some invalid type handling like long
		/*
			{
				st:       schema.SchemaTypeAvro,
				sf:       "testdata/schema/nullable_complex.avsc",
				rt:       record.RecordTypeMsgpack,
				input:    "testdata/record/nullable_complex.msgpack",
				expected: "testdata/parquet/nullable_complex.parquet",
			},
		*/

		// primitives; BigQuery schema, Avro record
		{
			st:       schema.SchemaTypeBigquery,
			sf:       "testdata/schema/primitives.bq.json",
			rt:       record.RecordTypeAvro,
			input:    "testdata/record/primitives.avro",
			expected: "testdata/parquet/primitives.parquet",
		},
		// primitives; BigQuery schema, CSV record
		{
			st:       schema.SchemaTypeBigquery,
			sf:       "testdata/schema/primitives.bq.json",
			rt:       record.RecordTypeCsv,
			input:    "testdata/record/primitives.csv",
			expected: "testdata/parquet/primitives.parquet",
		},
		// primitives; BigQuery schema, JSONL record
		{
			st:       schema.SchemaTypeBigquery,
			sf:       "testdata/schema/primitives.bq.json",
			rt:       record.RecordTypeJsonl,
			input:    "testdata/record/primitives.jsonl",
			expected: "testdata/parquet/primitives.parquet",
		},
		// primitives; BigQuery schema, LTSV record
		{
			st:       schema.SchemaTypeBigquery,
			sf:       "testdata/schema/primitives.bq.json",
			rt:       record.RecordTypeLtsv,
			input:    "testdata/record/primitives.ltsv",
			expected: "testdata/parquet/primitives.parquet",
		},
		// primitives; BigQuery schema, MessagePack record
		{
			st:       schema.SchemaTypeBigquery,
			sf:       "testdata/schema/primitives.bq.json",
			rt:       record.RecordTypeMsgpack,
			input:    "testdata/record/primitives.msgpack",
			expected: "testdata/parquet/primitives.parquet",
		},
		// primitives; BigQuery schema, TSV record
		{
			st:       schema.SchemaTypeBigquery,
			sf:       "testdata/schema/primitives.bq.json",
			rt:       record.RecordTypeTsv,
			input:    "testdata/record/primitives.tsv",
			expected: "testdata/parquet/primitives.parquet",
		},
		// nullables; Avro schema, Avro record
		{
			st:       schema.SchemaTypeBigquery,
			sf:       "testdata/schema/nullables.bq.json",
			rt:       record.RecordTypeAvro,
			input:    "testdata/record/nullables.avro",
			expected: "testdata/parquet/nullables.parquet",
		},
		// nullables; BigQuery schema, JSONL record
		// TODO handle some invalid type handling like long
		/*
			{
				st:       schema.SchemaTypeBigquery,
				sf:       "testdata/schema/nullables.bq.json",
				rt:       record.RecordTypeJsonl,
				input:    "testdata/record/nullables.jsonl",
				expected: "testdata/parquet/nullables.parquet",
			},
		*/
		// nullables; BigQuery schema, MessagePack record
		// TODO handle some invalid type handling like long
		/*
			{
				st:       schema.SchemaTypeBigquery,
				sf:       "testdata/schema/nullables.bq.json",
				rt:       record.RecordTypeMsgpack,
				input:    "testdata/record/nullables.msgpack",
				expected: "testdata/parquet/nullables.parquet",
			},
		*/
		// nested; BigQuery schema, Avro record
		{
			st:       schema.SchemaTypeBigquery,
			sf:       "testdata/schema/nested.bq.json",
			rt:       record.RecordTypeAvro,
			input:    "testdata/record/nested.avro",
			expected: "testdata/parquet/nested.parquet",
		},
		// nested; BigQuery schema, JSONL record
		{
			st:       schema.SchemaTypeBigquery,
			sf:       "testdata/schema/nested.bq.json",
			rt:       record.RecordTypeJsonl,
			input:    "testdata/record/nested.jsonl",
			expected: "testdata/parquet/nested.parquet",
		},
		// nested; BigQuery schema, MessagePack record
		{
			st:       schema.SchemaTypeBigquery,
			sf:       "testdata/schema/nested.bq.json",
			rt:       record.RecordTypeMsgpack,
			input:    "testdata/record/nested.msgpack",
			expected: "testdata/parquet/nested.parquet",
		},
		// array; BigQuery schema, Avro record
		{
			st:       schema.SchemaTypeBigquery,
			sf:       "testdata/schema/array.bq.json",
			rt:       record.RecordTypeAvro,
			input:    "testdata/record/array.avro",
			expected: "testdata/parquet/array.parquet",
		},
		// array; BigQuery schema, JSONL record
		{
			st:       schema.SchemaTypeBigquery,
			sf:       "testdata/schema/array.bq.json",
			rt:       record.RecordTypeJsonl,
			input:    "testdata/record/array.jsonl",
			expected: "testdata/parquet/array.parquet",
		},
		// array; BigQuery schema, MessagePack record
		{
			st:       schema.SchemaTypeBigquery,
			sf:       "testdata/schema/array.bq.json",
			rt:       record.RecordTypeMsgpack,
			input:    "testdata/record/array.msgpack",
			expected: "testdata/parquet/array.parquet",
		},
	}

	for _, c := range cases {
		out, err := ioutil.TempFile("", "out.parquet")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			_ = os.Remove(out.Name())
		})

		columnifier, err := NewParquetColumnifier(c.st, c.sf, c.rt, out.Name(), defaultConfig)
		if err != nil {
			t.Fatal(err)
		}

		// Check whether writing succeeds
		_, err = columnifier.WriteFromFiles([]string{c.input})
		if err == nil {
			err = columnifier.Close()
		}
		if err != nil {
			t.Errorf("expected success, but actual %v", err)
			continue
		}

		// Check written file
		assertWrittenParquet(t, c.expected, out.Name())
	}
}

func TestWriteClose_Errors(t *testing.T) {
	cases := []struct {
		st    string
		sf    string
		rt    string
		input string
	}{
		// Invalid record type
		{
			st:    schema.SchemaTypeAvro,
			sf:    "testdata/schema/primitives.avsc",
			rt:    "unknown",
			input: "testdata/record/primitives.jsonl",
		},

		// Mismatch schema & record
		{
			st:    schema.SchemaTypeAvro,
			sf:    "testdata/schema/mismatch.avsc",
			rt:    record.RecordTypeJsonl,
			input: "testdata/record/primitives.jsonl",
		},
	}

	for _, c := range cases {
		out, err := ioutil.TempFile("", "out.parquet")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			_ = os.Remove(out.Name())
		})

		columnifier, err := NewParquetColumnifier(c.st, c.sf, c.rt, out.Name(), defaultConfig)
		if err != nil {
			t.Fatal(err)
		}

		_, err = columnifier.WriteFromFiles([]string{c.input})
		if err == nil {
			err = columnifier.Close()
		}

		if err == nil {
			t.Errorf("expected error occurs, but actual it's nil")
		}
	}
}
