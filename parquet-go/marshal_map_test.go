package parquet_go

import (
	"bytes"
	"encoding/base64"
	"reflect"
	"testing"

	"github.com/apache/arrow/go/arrow"
	"github.com/reproio/columnify/schema"
	"github.com/xitongsys/parquet-go/layout"
)

func base64Str(d []byte, t *testing.T) string {
	var buf bytes.Buffer
	encoder := base64.NewEncoder(base64.StdEncoding, &buf)

	_, err := encoder.Write(d)
	if err != nil {
		t.Fatalf("invalid test case: %v", err)
	}

	err = encoder.Close()
	if err != nil {
		t.Fatalf("invalid test case: %v", err)
	}

	return buf.String()
}

func TestMarshalMap(t *testing.T) {
	cases := []struct {
		input  []interface{}
		bgn    int
		end    int
		schema *schema.IntermediateSchema
		expect *map[string]*layout.Table
		err    error
	}{
		// Only primitives
		{
			input: []interface{}{
				map[string]interface{}{
					"boolean": false,
					"bytes":   []byte("foo"),
					"double":  1.1,
					"float":   1.1,
					"int":     1,
					"long":    1,
					"string":  "foo",
				},
				map[string]interface{}{
					"boolean": true,
					"bytes":   []byte("bar"),
					"double":  2.2,
					"float":   2.2,
					"int":     2,
					"long":    2,
					"string":  "bar",
				},
			},
			bgn: 0,
			end: 2,
			schema: schema.NewIntermediateSchema(
				arrow.NewSchema(
					[]arrow.Field{
						{
							Name:     "boolean",
							Type:     arrow.FixedWidthTypes.Boolean,
							Nullable: false,
						},
						{
							Name:     "int",
							Type:     arrow.PrimitiveTypes.Uint32,
							Nullable: false,
						},
						{
							Name:     "long",
							Type:     arrow.PrimitiveTypes.Uint64,
							Nullable: false,
						},
						{
							Name:     "float",
							Type:     arrow.PrimitiveTypes.Float32,
							Nullable: false,
						},
						{
							Name:     "double",
							Type:     arrow.PrimitiveTypes.Float64,
							Nullable: false,
						},
						{
							Name:     "bytes",
							Type:     arrow.BinaryTypes.Binary,
							Nullable: false,
						},
						{
							Name:     "string",
							Type:     arrow.BinaryTypes.String,
							Nullable: false,
						},
					}, nil),
				"primitives"),
			expect: &map[string]*layout.Table{
				"Primitives.Boolean": {
					Values:           []interface{}{false, true},
					DefinitionLevels: []int32{0, 0},
					RepetitionLevels: []int32{0, 0},
				},
				"Primitives.Int": {
					Values:           []interface{}{int32(1), int32(2)},
					DefinitionLevels: []int32{0, 0},
					RepetitionLevels: []int32{0, 0},
				},
				"Primitives.Long": {
					Values:           []interface{}{int64(1), int64(2)},
					DefinitionLevels: []int32{0, 0},
					RepetitionLevels: []int32{0, 0},
				},
				"Primitives.Float": {
					Values:           []interface{}{float32(1.1), float32(2.2)},
					DefinitionLevels: []int32{0, 0},
					RepetitionLevels: []int32{0, 0},
				},
				"Primitives.Double": {
					Values:           []interface{}{float64(1.1), float64(2.2)},
					DefinitionLevels: []int32{0, 0},
					RepetitionLevels: []int32{0, 0},
				},
				"Primitives.Bytes": {
					Values:           []interface{}{base64Str([]byte("foo"), t), base64Str([]byte("bar"), t)},
					DefinitionLevels: []int32{0, 0},
					RepetitionLevels: []int32{0, 0},
				},
				"Primitives.String": {
					Values:           []interface{}{"foo", "bar"},
					DefinitionLevels: []int32{0, 0},
					RepetitionLevels: []int32{0, 0},
				},
			},
			err: nil,
		},

		// Nested
		{
			input: []interface{}{
				map[string]interface{}{
					"boolean": false,
					"bytes":   []byte("foo"),
					"double":  1.1,
					"float":   1.1,
					"int":     1,
					"long":    1,
					"string":  "foo",
					"record": map[string]interface{}{
						"boolean": false,
						"bytes":   []byte("foo"),
						"double":  1.1,
						"float":   1.1,
						"int":     1,
						"long":    1,
						"string":  "foo",
					},
				},
				map[string]interface{}{
					"boolean": true,
					"bytes":   []byte("bar"),
					"double":  2.2,
					"float":   2.2,
					"int":     2,
					"long":    2,
					"string":  "bar",
					"record": map[string]interface{}{
						"boolean": true,
						"bytes":   []byte("bar"),
						"double":  2.2,
						"float":   2.2,
						"int":     2,
						"long":    2,
						"string":  "bar",
					},
				},
			},
			bgn: 0,
			end: 2,
			schema: schema.NewIntermediateSchema(
				arrow.NewSchema(
					[]arrow.Field{
						{
							Name: "boolean",
							Type: arrow.FixedWidthTypes.Boolean,
						},
						{
							Name: "int",
							Type: arrow.PrimitiveTypes.Uint32,
						},
						{
							Name: "long",
							Type: arrow.PrimitiveTypes.Uint64,
						},
						{
							Name: "float",
							Type: arrow.PrimitiveTypes.Float32,
						},
						{
							Name: "double",
							Type: arrow.PrimitiveTypes.Float64,
						},
						{
							Name: "bytes",
							Type: arrow.BinaryTypes.Binary,
						},
						{
							Name: "string",
							Type: arrow.BinaryTypes.String,
						},
						{
							Name: "record",
							Type: arrow.StructOf(
								[]arrow.Field{
									{
										Name:     "boolean",
										Type:     arrow.FixedWidthTypes.Boolean,
										Nullable: false,
									},
									{
										Name:     "int",
										Type:     arrow.PrimitiveTypes.Uint32,
										Nullable: false,
									},
									{
										Name:     "long",
										Type:     arrow.PrimitiveTypes.Uint64,
										Nullable: false,
									},
									{
										Name:     "float",
										Type:     arrow.PrimitiveTypes.Float32,
										Nullable: false,
									},
									{
										Name:     "double",
										Type:     arrow.PrimitiveTypes.Float64,
										Nullable: false,
									},
									{
										Name:     "bytes",
										Type:     arrow.BinaryTypes.Binary,
										Nullable: false,
									},
									{
										Name:     "string",
										Type:     arrow.BinaryTypes.String,
										Nullable: false,
									},
								}...,
							),
							Nullable: false,
						},
					},
					nil),
				"nested"),
			expect: &map[string]*layout.Table{
				"Nested.Boolean": {
					Values:           []interface{}{false, true},
					DefinitionLevels: []int32{0, 0},
					RepetitionLevels: []int32{0, 0},
				},
				"Nested.Int": {
					Values:           []interface{}{int32(1), int32(2)},
					DefinitionLevels: []int32{0, 0},
					RepetitionLevels: []int32{0, 0},
				},
				"Nested.Long": {
					Values:           []interface{}{int64(1), int64(2)},
					DefinitionLevels: []int32{0, 0},
					RepetitionLevels: []int32{0, 0},
				},
				"Nested.Float": {
					Values:           []interface{}{float32(1.1), float32(2.2)},
					DefinitionLevels: []int32{0, 0},
					RepetitionLevels: []int32{0, 0},
				},
				"Nested.Double": {
					Values:           []interface{}{float64(1.1), float64(2.2)},
					DefinitionLevels: []int32{0, 0},
					RepetitionLevels: []int32{0, 0},
				},
				"Nested.Bytes": {
					Values:           []interface{}{base64Str([]byte("foo"), t), base64Str([]byte("bar"), t)},
					DefinitionLevels: []int32{0, 0},
					RepetitionLevels: []int32{0, 0},
				},
				"Nested.String": {
					Values:           []interface{}{"foo", "bar"},
					DefinitionLevels: []int32{0, 0},
					RepetitionLevels: []int32{0, 0},
				},
				"Nested.Record.Boolean": {
					Values:           []interface{}{false, true},
					DefinitionLevels: []int32{0, 0},
					RepetitionLevels: []int32{0, 0},
				},
				"Nested.Record.Int": {
					Values:           []interface{}{int32(1), int32(2)},
					DefinitionLevels: []int32{0, 0},
					RepetitionLevels: []int32{0, 0},
				},
				"Nested.Record.Long": {
					Values:           []interface{}{int64(1), int64(2)},
					DefinitionLevels: []int32{0, 0},
					RepetitionLevels: []int32{0, 0},
				},
				"Nested.Record.Float": {
					Values:           []interface{}{float32(1.1), float32(2.2)},
					DefinitionLevels: []int32{0, 0},
					RepetitionLevels: []int32{0, 0},
				},
				"Nested.Record.Double": {
					Values:           []interface{}{float64(1.1), float64(2.2)},
					DefinitionLevels: []int32{0, 0},
					RepetitionLevels: []int32{0, 0},
				},
				"Nested.Record.Bytes": {
					Values:           []interface{}{base64Str([]byte("foo"), t), base64Str([]byte("bar"), t)},
					DefinitionLevels: []int32{0, 0},
					RepetitionLevels: []int32{0, 0},
				},
				"Nested.Record.String": {
					Values:           []interface{}{"foo", "bar"},
					DefinitionLevels: []int32{0, 0},
					RepetitionLevels: []int32{0, 0},
				},
			},
			err: nil,
		},

		// Array
		{
			input: []interface{}{
				map[string]interface{}{
					"boolean": false,
					"bytes":   []byte("foo"),
					"double":  1.1,
					"float":   1.1,
					"int":     1,
					"long":    1,
					"string":  "foo",
					"array": []interface{}{
						map[string]interface{}{
							"boolean": false,
							"bytes":   []byte("foo"),
							"double":  1.1,
							"float":   1.1,
							"int":     1,
							"long":    1,
							"string":  "foo",
						},
					},
				},
				map[string]interface{}{
					"boolean": true,
					"bytes":   []byte("bar"),
					"double":  2.2,
					"float":   2.2,
					"int":     2,
					"long":    2,
					"string":  "bar",
					"array": []interface{}{
						map[string]interface{}{
							"boolean": true,
							"bytes":   []byte("bar"),
							"double":  2.2,
							"float":   2.2,
							"int":     2,
							"long":    2,
							"string":  "bar",
						},
					},
				},
			},
			bgn: 0,
			end: 2,
			schema: schema.NewIntermediateSchema(
				arrow.NewSchema(
					[]arrow.Field{
						{
							Name:     "boolean",
							Type:     arrow.FixedWidthTypes.Boolean,
							Nullable: false,
						},
						{
							Name:     "int",
							Type:     arrow.PrimitiveTypes.Uint32,
							Nullable: false,
						},
						{
							Name:     "long",
							Type:     arrow.PrimitiveTypes.Uint64,
							Nullable: false,
						},
						{
							Name:     "float",
							Type:     arrow.PrimitiveTypes.Float32,
							Nullable: false,
						},
						{
							Name:     "double",
							Type:     arrow.PrimitiveTypes.Float64,
							Nullable: false,
						},
						{
							Name:     "bytes",
							Type:     arrow.BinaryTypes.Binary,
							Nullable: false,
						},
						{
							Name:     "string",
							Type:     arrow.BinaryTypes.String,
							Nullable: false,
						},
						{
							Name: "array",
							Type: arrow.ListOf(
								arrow.StructOf(
									[]arrow.Field{
										{
											Name:     "boolean",
											Type:     arrow.FixedWidthTypes.Boolean,
											Nullable: false,
										},
										{
											Name:     "int",
											Type:     arrow.PrimitiveTypes.Uint32,
											Nullable: false,
										},
										{
											Name:     "long",
											Type:     arrow.PrimitiveTypes.Uint64,
											Nullable: false,
										},
										{
											Name:     "float",
											Type:     arrow.PrimitiveTypes.Float32,
											Nullable: false,
										},
										{
											Name:     "double",
											Type:     arrow.PrimitiveTypes.Float64,
											Nullable: false,
										},
										{
											Name:     "bytes",
											Type:     arrow.BinaryTypes.Binary,
											Nullable: false,
										},
										{
											Name:     "string",
											Type:     arrow.BinaryTypes.String,
											Nullable: false,
										},
									}...,
								)),
							Nullable: false,
						},
					}, nil),
				"arrays"),
			expect: &map[string]*layout.Table{
				"Arrays.Boolean": {
					Values:           []interface{}{false, true},
					DefinitionLevels: []int32{0, 0},
					RepetitionLevels: []int32{0, 0},
				},
				"Arrays.Int": {
					Values:           []interface{}{int32(1), int32(2)},
					DefinitionLevels: []int32{0, 0},
					RepetitionLevels: []int32{0, 0},
				},
				"Arrays.Long": {
					Values:           []interface{}{int64(1), int64(2)},
					DefinitionLevels: []int32{0, 0},
					RepetitionLevels: []int32{0, 0},
				},
				"Arrays.Float": {
					Values:           []interface{}{float32(1.1), float32(2.2)},
					DefinitionLevels: []int32{0, 0},
					RepetitionLevels: []int32{0, 0},
				},
				"Arrays.Double": {
					Values:           []interface{}{float64(1.1), float64(2.2)},
					DefinitionLevels: []int32{0, 0},
					RepetitionLevels: []int32{0, 0},
				},
				"Arrays.Bytes": {
					Values:           []interface{}{base64Str([]byte("foo"), t), base64Str([]byte("bar"), t)},
					DefinitionLevels: []int32{0, 0},
					RepetitionLevels: []int32{0, 0},
				},
				"Arrays.String": {
					Values:           []interface{}{"foo", "bar"},
					DefinitionLevels: []int32{0, 0},
					RepetitionLevels: []int32{0, 0},
				},
				"Arrays.Array.Boolean": {
					Values:           []interface{}{false, true},
					DefinitionLevels: []int32{1, 1},
					RepetitionLevels: []int32{0, 0},
				},
				"Arrays.Array.Int": {
					Values:           []interface{}{int32(1), int32(2)},
					DefinitionLevels: []int32{1, 1},
					RepetitionLevels: []int32{0, 0},
				},
				"Arrays.Array.Long": {
					Values:           []interface{}{int64(1), int64(2)},
					DefinitionLevels: []int32{1, 1},
					RepetitionLevels: []int32{0, 0},
				},
				"Arrays.Array.Float": {
					Values:           []interface{}{float32(1.1), float32(2.2)},
					DefinitionLevels: []int32{1, 1},
					RepetitionLevels: []int32{0, 0},
				},
				"Arrays.Array.Double": {
					Values:           []interface{}{float64(1.1), float64(2.2)},
					DefinitionLevels: []int32{1, 1},
					RepetitionLevels: []int32{0, 0},
				},
				"Arrays.Array.Bytes": {
					Values:           []interface{}{base64Str([]byte("foo"), t), base64Str([]byte("bar"), t)},
					DefinitionLevels: []int32{1, 1},
					RepetitionLevels: []int32{0, 0},
				},
				"Arrays.Array.String": {
					Values:           []interface{}{"foo", "bar"},
					DefinitionLevels: []int32{1, 1},
					RepetitionLevels: []int32{0, 0},
				},
			},
			err: nil,
		},
	}

	for _, c := range cases {
		sh, err := schema.NewSchemaHandlerFromArrow(*c.schema)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		tables, err := MarshalMap(c.input, c.bgn, c.end, sh)
		if err != c.err {
			t.Errorf("expected: %v, but actual: %v\n", c.err, err)
		}

		for k, v := range *c.expect {
			actual := (*tables)[k]

			if !reflect.DeepEqual(actual.Values, v.Values) {
				t.Errorf("values:  expected: %v, but actual: %v\n", v.Values, actual.Values)
			}

			if !reflect.DeepEqual(actual.DefinitionLevels, v.DefinitionLevels) {
				t.Errorf("definition levels:  expected: %v, but actual: %v\n", v.DefinitionLevels, actual.DefinitionLevels)
			}
			if !reflect.DeepEqual(actual.RepetitionLevels, v.RepetitionLevels) {
				t.Errorf("repetition levels:  expected: %v, but actual: %v\n", v.RepetitionLevels, actual.RepetitionLevels)
			}
		}
	}
}
