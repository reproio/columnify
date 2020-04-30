package columnifier

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/reproio/columnify/record"
	"github.com/reproio/columnify/schema"
	"github.com/xitongsys/parquet-go/parquet"
)

// prepareSchemaFiles create a new schema file as a temp file. Caller needs to remove the file.
func prepareSchemaFiles() (string, error) {
	f, err := ioutil.TempFile("", "schema.avsc")
	if err != nil {
		return "", err
	}

	_, err = f.Write([]byte(`
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
`))
	if err != nil {
		return "", err
	}

	err = f.Close()
	if err != nil {
		return "", err
	}

	return f.Name(), nil
}

// prepareRecordFiles create a new record file as a temp file. Caller needs to remove the file.
func prepareRecordFiles() (string, error) {
	f, err := ioutil.TempFile("", "record.json")
	if err != nil {
		return "", err
	}

	_, err = f.Write([]byte(`{"boolean": false, "int": 1, "long": 1, "float": 1.1, "double": 1.1, "bytes": "foo", "string": "foo"}
{"boolean": true, "int": 2, "long": 2, "float": 2.2, "double": 2.2, "bytes": "bar", "string": "bar"}
`))
	if err != nil {
		return "", err
	}

	err = f.Close()
	if err != nil {
		return "", err
	}

	return f.Name(), nil
}

func TestNewParquetColumnifier(t *testing.T) {
	sf, err := prepareSchemaFiles()
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(sf)

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
			sf:     sf,
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
			sf:     sf,
			rt:     record.RecordTypeJsonl,
			output: "/tmp/nonexistence/invalid.record",
			config: Config{},
			isErr:  true,
		},

		// Valid
		{
			st:     schema.SchemaTypeAvro,
			sf:     sf,
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
	sf, err := prepareSchemaFiles()
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(sf)

	rf, err := prepareRecordFiles()
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(rf)

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
			sf: sf,
			rt: "unknown",
			config: Config{
				Parquet: Parquet{
					PageSize:         8 * 1024,
					RowGroupSize:     128 * 1024 * 1024,
					CompressionCodec: parquet.CompressionCodec_SNAPPY,
				},
			},
			input: rf,
			isErr: true,
		},

		// Valid
		{
			st: schema.SchemaTypeAvro,
			sf: sf,
			rt: record.RecordTypeJsonl,
			config: Config{
				Parquet: Parquet{
					PageSize:         8 * 1024,
					RowGroupSize:     128 * 1024 * 1024,
					CompressionCodec: parquet.CompressionCodec_SNAPPY,
				},
			},
			input: rf,
			isErr: false,
		},
	}

	for _, c := range cases {
		func() {
			out, err := ioutil.TempFile("", "out.parquet")
			if err != nil {
				t.Fatal(err)
			}
			defer os.Remove(out.Name())

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
		}()
	}
}
