
package record

import (
	"bytes"
	"io"
	"reflect"
	"testing"
	"errors"
	"github.com/apache/arrow/go/arrow"
	"github.com/reproio/columnify/schema"
)

func TestTsvInnerDecoder_Primitives(t *testing.T) {
       schemaObj := schema.NewIntermediateSchema(
	       arrow.NewSchema([]arrow.Field{
		       {Name: "boolean", Type: arrow.FixedWidthTypes.Boolean, Nullable: false},
		       {Name: "int", Type: arrow.PrimitiveTypes.Uint32, Nullable: false},
		       {Name: "long", Type: arrow.PrimitiveTypes.Uint64, Nullable: false},
		       {Name: "float", Type: arrow.PrimitiveTypes.Float32, Nullable: false},
		       {Name: "double", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
		       {Name: "bytes", Type: arrow.BinaryTypes.Binary, Nullable: false},
		       {Name: "string", Type: arrow.BinaryTypes.String, Nullable: false},
	       }, nil), "primitives")

			     cases := []struct {
				     name     string
				     input    []byte
				     expected []map[string]interface{}
				     isErr    bool
				     errIs    error
				     errMsg   string
			     }{
				{  name: "Primitives",
				      input: []byte("false\t1\t1\t1.1\t1.1\tfoo\tfoo\n" +
						   "true\t2\t2\t2.2\t2.2\tbar\tbar\n"),
				      expected: []map[string]interface{}{
					      {
						      "boolean": false,
						      "int": int64(1),
						      "long": int64(1),
						      "float": float64(1.1),
						      "double": float64(1.1),
						      "bytes": "foo",
						      "string": "foo",
					      },
					      {
						      "boolean": true,
						      "int": int64(2),
						      "long": int64(2),
						      "float": float64(2.2),
						      "double": float64(2.2),
						      "bytes": "bar",
						      "string": "bar",
					      },
				      },
				      isErr: false,
				      errIs: nil,
				      errMsg: "",
			      },
				{
					name: "QuotedField",
					input: []byte("false\t1\t1\t1.1\t1.1\t\"quoted\"\t\"quoted string\"\n"),
					expected: []map[string]interface{}{
						{
							"boolean": false,
							"int": int64(1),
							"long": int64(1),
							"float": float64(1.1),
							"double": float64(1.1),
							"bytes": "\"quoted\"",
							"string": "\"quoted string\"",
						},
					},
					isErr: false,
					errIs: nil,
					errMsg: "",
				},
			      {
				      name: "TooFewFields",
				      input: []byte("true\t2\t2\n"),
				      expected: nil,
				      isErr: true,
				      errIs: ErrTooFewFields,
				      errMsg: "too few fields: expected 7, got 3",
			      },
			      {
				      name: "JsonField",
				      input: []byte("false\t1\t1\t1.1\t1.1\t{\"foo\":123}\tbar\n"),
				      expected: []map[string]interface{}{
					      {
						      "boolean": false,
						      "int": int64(1),
						      "long": int64(1),
						      "float": float64(1.1),
						      "double": float64(1.1),
						      "bytes": "{\"foo\":123}",
						      "string": "bar",
					      },
				      },
				      isErr: false,
				      errIs: nil,
				      errMsg: "",
			      },
			      {
				      name: "JsonFieldWithJsonEncodedValue",
				      input: []byte("false\t1\t1\t1.1\t1.1\t\"{\\\"foo\\\":123}\"\tbar\n"),
				      expected: []map[string]interface{}{
					      {
						      "boolean": false,
						      "int": int64(1),
						      "long": int64(1),
						      "float": float64(1.1),
						      "double": float64(1.1),
						      "bytes": "\"{\\\"foo\\\":123}\"",
						      "string": "bar",
					      },
				      },
				      isErr: false,
				      errIs: nil,
				      errMsg: "",
			      },
				  {
					name: "JsonFieldWithNestedJsonEncodedValue",
					input: []byte("false\t1\t1\t1.1\t1.1\t{\"foo\":\"{\\\"bar\\\":123}\"}\tbar\n"),
					expected: []map[string]interface{}{
						{
							"boolean": false,
							"int": int64(1),
							"long": int64(1),
							"float": float64(1.1),
							"double": float64(1.1),
							"bytes": "{\"foo\":\"{\\\"bar\\\":123}\"}",
							"string": "bar",
						},
					},
					isErr: false,
					errIs: nil,
					errMsg: "",
				},
		      }

	       for _, c := range cases {
			       t.Run(c.name, func(t *testing.T) {
				       buf := bytes.NewReader(c.input)
				       d, err := newTsvInnerDecoder(buf, schemaObj)
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
				       if c.isErr {
					       if err == nil || err == io.EOF {
						       t.Fatalf("expected error, got: %v", err)
					       }
					       if c.errIs != nil && !errors.Is(err, c.errIs) {
						       t.Fatalf("expected error to match errors.Is: %v, got: %v", c.errIs, err)
					       }
					       if c.errMsg != "" && err.Error() != c.errMsg {
						       t.Fatalf("expected error message: %q, got: %q", c.errMsg, err.Error())
					       }
					       return
				       }
				       if err != io.EOF {
					       t.Fatalf("expected EOF, got: %v", err)
				       }
				       if !reflect.DeepEqual(actual, c.expected) {
					       t.Errorf("expected: %v, got: %v", c.expected, actual)
				       }
			       })
	       }
}
