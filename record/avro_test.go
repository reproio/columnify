package record

import (
	"bytes"
	"io"
	"reflect"
	"testing"
	"time"

	"github.com/apache/arrow/go/arrow"
	"github.com/linkedin/goavro/v2"
	"github.com/reproio/columnify/schema"
)

func TestFlattenAvroUnion(t *testing.T) {
	input := map[string]interface{}{
		"primitive": 42,
		"nested-single": map[string]interface{}{
			"string": "test",
		},
		"nested-multiple": map[string]interface{}{
			"int":    42,
			"string": "test",
		},
	}
	expected := map[string]interface{}{
		"primitive":     42,
		"nested-single": "test",
		"nested-multiple": map[string]interface{}{
			"int":    42,
			"string": "test",
		},
	}

	actual := flattenAvroUnion(input)

	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("expected: %v, but actual: %v\n", expected, actual)
	}
}

func TestAvroInnerDecoder_Decode(t *testing.T) {
	cases := []struct {
		schema   string
		records  []map[string]interface{}
		expected []map[string]interface{}
	}{
		{
			schema: `
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
`,
			records: []map[string]interface{}{
				{
					"boolean": false,
					"bytes":   string([]byte("foo")),
					"double":  1.1,
					"float":   1.1,
					"int":     1,
					"long":    1,
					"string":  "foo",
				},
				{
					"boolean": true,
					"bytes":   string([]byte("bar")),
					"double":  2.2,
					"float":   2.2,
					"int":     2,
					"long":    2,
					"string":  "bar",
				},
			},
			expected: []map[string]interface{}{
				{
					"boolean": false,
					"bytes":   []byte("foo"),
					"double":  float64(1.1),
					"float":   float32(1.1),
					"int":     int32(1),
					"long":    int64(1),
					"string":  "foo",
				},
				{
					"boolean": true,
					"bytes":   []byte("bar"),
					"double":  float64(2.2),
					"float":   float32(2.2),
					"int":     int32(2),
					"long":    int64(2),
					"string":  "bar",
				},
			},
		},
		{
			schema: `
{
  "type": "record",
  "name": "Logicals",
  "fields" : [
    {"name": "date",            "type": {"type": "int",  "logicalType": "date"}},
    {"name": "timemillis",      "type": {"type": "int",  "logicalType": "time-millis"}},
    {"name": "timemicros",      "type": {"type": "long", "logicalType": "time-micros"}},
    {"name": "timestampmillis", "type": {"type": "long", "logicalType": "timestamp-millis"}},
    {"name": "timestampmicros", "type": {"type": "long", "logicalType": "timestamp-micros"}},
    {"name": "nullable",        "type": ["null", {"type": "long", "logicalType": "timestamp-millis"}]},
    {
      "name": "nested",
      "type": {
        "type": "record",
        "name": "Nested",
        "fields": [
          {"name": "int",             "type": "int"},
          {"name": "timestampmillis", "type": {"type": "long", "logicalType": "timestamp-millis"}}
        ]
      }
    },
    {"name": "array", "type": {"type": "array", "items": {"type": "long", "logicalType": "timestamp-micros"}}}
  ]
}
`,
			records: []map[string]interface{}{
				{
					"date":            time.Unix(86400, 0).UTC(),
					"timemillis":      3 * time.Second,
					"timemicros":      3 * time.Second,
					"timestampmillis": time.UnixMilli(1600000000123).UTC(),
					"timestampmicros": time.UnixMicro(1600000000123456).UTC(),
					"nullable":        goavro.Union("long.timestamp-millis", time.UnixMilli(1600000000123).UTC()),
					"nested": map[string]interface{}{
						"int":             1,
						"timestampmillis": time.UnixMilli(1600000000123).UTC(),
					},
					"array": []interface{}{time.UnixMicro(1600000000123456).UTC()},
				},
				{
					"date":            time.Unix(-86400, 0).UTC(),
					"timemillis":      time.Duration(0),
					"timemicros":      time.Duration(0),
					"timestampmillis": time.Unix(0, 0).UTC(),
					"timestampmicros": time.Unix(0, 0).UTC(),
					"nullable":        goavro.Union("null", nil),
					"nested": map[string]interface{}{
						"int":             2,
						"timestampmillis": time.Unix(0, 0).UTC(),
					},
					"array": []interface{}{},
				},
			},
			expected: []map[string]interface{}{
				{
					"date":            int32(1),
					"timemillis":      int32(3000),
					"timemicros":      int64(3000000),
					"timestampmillis": int64(1600000000123),
					"timestampmicros": int64(1600000000123456),
					"nullable":        int64(1600000000123),
					"nested": map[string]interface{}{
						"int":             int32(1),
						"timestampmillis": int64(1600000000123),
					},
					"array": []interface{}{int64(1600000000123456)},
				},
				{
					"date":            int32(-1),
					"timemillis":      int32(0),
					"timemicros":      int64(0),
					"timestampmillis": int64(0),
					"timestampmicros": int64(0),
					"nullable":        nil,
					"nested": map[string]interface{}{
						"int":             int32(2),
						"timestampmillis": int64(0),
					},
					"array": []interface{}{},
				},
			},
		},
	}

	for _, c := range cases {
		w := &bytes.Buffer{}
		ocfw, err := goavro.NewOCFWriter(goavro.OCFConfig{
			W:      w,
			Schema: c.schema,
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := ocfw.Append(c.records); err != nil {
			t.Fatal(err)
		}

		s, err := schema.NewSchemaFromAvroSchema([]byte(c.schema))
		if err != nil {
			t.Fatal(err)
		}

		d, err := newAvroInnerDecoder(bytes.NewReader(w.Bytes()), s)
		if err != nil {
			t.Fatal(err)
		}

		actual := make([]map[string]interface{}, 0)
		for {
			var v map[string]interface{}
			err = d.Decode(&v)
			if err != nil {
				break
			}
			actual = append(actual, v)
		}

		if err != nil && err != io.EOF {
			t.Errorf("expected no error or io.EOF, but actual: %v\n", err)
			continue
		}

		if !reflect.DeepEqual(actual, c.expected) {
			t.Errorf("expected: %v, but actual: %v\n", c.expected, actual)
		}
	}
}

func TestConvertAvroLogicalValue(t *testing.T) {
	cases := []struct {
		input    interface{}
		t        arrow.DataType
		expected interface{}
	}{
		{time.Date(1970, 1, 2, 12, 0, 0, 0, time.UTC), arrow.FixedWidthTypes.Date32, int32(1)},
		{time.Date(1969, 12, 31, 12, 0, 0, 0, time.UTC), arrow.FixedWidthTypes.Date32, int32(-1)},
		{time.Date(1969, 12, 31, 0, 0, 0, 0, time.UTC), arrow.FixedWidthTypes.Date32, int32(-1)},
		{time.UnixMilli(1600000000123), arrow.FixedWidthTypes.Timestamp_us, int64(1600000000123000)},
		{1500 * time.Millisecond, arrow.FixedWidthTypes.Time32ms, int32(1500)},
		{"not a time", arrow.FixedWidthTypes.Date32, "not a time"},
	}

	for _, c := range cases {
		actual := convertAvroLogicalValue(c.input, c.t)
		if !reflect.DeepEqual(actual, c.expected) {
			t.Errorf("input %v (%v): expected %#v, but actual %#v\n", c.input, c.t, c.expected, actual)
		}
	}
}
