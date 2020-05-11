package columnifier

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/reproio/columnify/record"
	"github.com/reproio/columnify/schema"
	"github.com/xitongsys/parquet-go/parquet"
)

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
			sf:     "../testdata/primitives.avsc",
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
			sf:     "../testdata/primitives.avsc",
			rt:     record.RecordTypeJsonl,
			output: "/tmp/nonexistence/invalid.record",
			config: Config{},
			isErr:  true,
		},

		// Valid
		{
			st:     schema.SchemaTypeAvro,
			sf:     "../testdata/primitives.avsc",
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
		st     string
		sf     string
		rt     string
		config Config
		input  string
		isErr  bool
	}{
		// Invalid record type
		{
			st: schema.SchemaTypeAvro,
			sf: "../testdata/primitives.avsc",
			rt: "unknown",
			config: Config{
				Parquet: Parquet{
					PageSize:         8 * 1024,
					RowGroupSize:     128 * 1024 * 1024,
					CompressionCodec: parquet.CompressionCodec_SNAPPY,
				},
			},
			input: "../testdata/primitives.jsonl",
			isErr: true,
		},

		// Mismatch schema & record
		{
			st: schema.SchemaTypeAvro,
			sf: "../testdata/mismatch.avsc",
			rt: record.RecordTypeJsonl,
			config: Config{
				Parquet: Parquet{
					PageSize:         8 * 1024,
					RowGroupSize:     128 * 1024 * 1024,
					CompressionCodec: parquet.CompressionCodec_SNAPPY,
				},
			},
			input: "../testdata/primitives.jsonl",
			isErr: true,
		},

		// Valid
		{
			st: schema.SchemaTypeAvro,
			sf: "../testdata/primitives.avsc",
			rt: record.RecordTypeJsonl,
			config: Config{
				Parquet: Parquet{
					PageSize:         8 * 1024,
					RowGroupSize:     128 * 1024 * 1024,
					CompressionCodec: parquet.CompressionCodec_SNAPPY,
				},
			},
			input: "../testdata/primitives.jsonl",
			isErr: false,
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

		columnifier, err := NewParquetColumnifier(c.st, c.sf, c.rt, out.Name(), c.config)
		if err != nil {
			t.Fatal(err)
		}

		_, err = columnifier.WriteFromFiles([]string{c.input})
		if err == nil {
			err = columnifier.Close()
		}

		if err != nil != c.isErr {
			t.Errorf("expected %v, but actual %v", c.isErr, err)
		}
	}
}
