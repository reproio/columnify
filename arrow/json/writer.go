package json

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
)

var (
	ErrMismatchFields  = errors.New("arrow/json: number of records mismatch")
	ErrUnsupportedType = errors.New("arrow/json: unsupported type")
)

// JsonEncoder wraps encoding/json.Encoder and writes array.Record based on a schema.
type Encoder struct {
	e      *json.Encoder
	schema *arrow.Schema
}

// NewWriter returns a writer that writes array.Records to the CSV file
// with the given schema.
//
// NewWriter panics if the given schema contains fields that have types that are not
// primitive types.
func NewWriter(w io.Writer, schema *arrow.Schema) *Encoder {
	ww := &Encoder{
		e:      json.NewEncoder(w),
		schema: schema,
	}

	return ww
}

func (e *Encoder) Schema() *arrow.Schema { return e.schema }

// Write writes a single Record as one row to the JSON file
func (e *Encoder) Write(record array.Record) error {
	if !record.Schema().Equal(e.schema) {
		return ErrMismatchFields
	}

	recs := make([]map[string]interface{}, record.NumRows())
	for i := range recs {
		recs[i] = make(map[string]interface{}, record.NumCols())
	}

	for i, col := range record.Columns() {
		if err := writeData(col.Data(), &recs, []string{e.schema.Field(i).Name}); err != nil {
			return err
		}
	}

	return e.e.Encode(recs)
}

func writeData(data *array.Data, recs *[]map[string]interface{}, names []string) error {
	switch data.DataType().ID() {
	case arrow.BOOL:
		arr := array.NewBooleanData(data)
		defer arr.Release()
		for i := 0; i < arr.Len(); i++ {
			if arr.IsValid(i) {
				if err := deepSet(&(*recs)[i], names, arr.Value(i)); err != nil {
					return err
				}
			} else {
				if err := deepSet(&(*recs)[i], names, nil); err != nil {
					return err
				}
			}
		}

	case arrow.INT8:
		arr := array.NewInt8Data(data)
		defer arr.Release()
		for i := 0; i < arr.Len(); i++ {
			if arr.IsValid(i) {
				if err := deepSet(&(*recs)[i], names, arr.Value(i)); err != nil {
					return err
				}
			} else {
				if err := deepSet(&(*recs)[i], names, nil); err != nil {
					return err
				}
			}
		}

	case arrow.INT16:
		arr := array.NewInt16Data(data)
		defer arr.Release()
		for i := 0; i < arr.Len(); i++ {
			if arr.IsValid(i) {
				if err := deepSet(&(*recs)[i], names, arr.Value(i)); err != nil {
					return err
				}
			} else {
				if err := deepSet(&(*recs)[i], names, nil); err != nil {
					return err
				}
			}
		}

	case arrow.INT32:
		arr := array.NewInt32Data(data)
		defer arr.Release()
		for i := 0; i < arr.Len(); i++ {
			if arr.IsValid(i) {
				if err := deepSet(&(*recs)[i], names, arr.Value(i)); err != nil {
					return err
				}
			} else {
				if err := deepSet(&(*recs)[i], names, nil); err != nil {
					return err
				}
			}
		}

	case arrow.INT64:
		arr := array.NewInt64Data(data)
		defer arr.Release()
		for i := 0; i < arr.Len(); i++ {
			if arr.IsValid(i) {
				if err := deepSet(&(*recs)[i], names, arr.Value(i)); err != nil {
					return err
				}
			} else {
				if err := deepSet(&(*recs)[i], names, nil); err != nil {
					return err
				}
			}
		}

	case arrow.UINT8:
		arr := array.NewUint8Data(data)
		defer arr.Release()
		for i := 0; i < arr.Len(); i++ {
			if arr.IsValid(i) {
				if err := deepSet(&(*recs)[i], names, arr.Value(i)); err != nil {
					return err
				}
			} else {
				if err := deepSet(&(*recs)[i], names, nil); err != nil {
					return err
				}
			}
		}

	case arrow.UINT16:
		arr := array.NewUint16Data(data)
		defer arr.Release()
		for i := 0; i < arr.Len(); i++ {
			if arr.IsValid(i) {
				if err := deepSet(&(*recs)[i], names, arr.Value(i)); err != nil {
					return err
				}
			} else {
				if err := deepSet(&(*recs)[i], names, nil); err != nil {
					return err
				}
			}
		}

	case arrow.UINT32:
		arr := array.NewUint32Data(data)
		defer arr.Release()
		for i := 0; i < arr.Len(); i++ {
			if arr.IsValid(i) {
				if err := deepSet(&(*recs)[i], names, arr.Value(i)); err != nil {
					return err
				}
			} else {
				if err := deepSet(&(*recs)[i], names, nil); err != nil {
					return err
				}
			}
		}

	case arrow.UINT64:
		arr := array.NewUint64Data(data)
		defer arr.Release()
		for i := 0; i < arr.Len(); i++ {
			if arr.IsValid(i) {
				if err := deepSet(&(*recs)[i], names, arr.Value(i)); err != nil {
					return err
				}
			} else {
				if err := deepSet(&(*recs)[i], names, nil); err != nil {
					return err
				}
			}
		}

	case arrow.FLOAT32:
		arr := array.NewFloat32Data(data)
		defer arr.Release()
		for i := 0; i < arr.Len(); i++ {
			if arr.IsValid(i) {
				if err := deepSet(&(*recs)[i], names, arr.Value(i)); err != nil {
					return err
				}
			} else {
				if err := deepSet(&(*recs)[i], names, nil); err != nil {
					return err
				}
			}
		}

	case arrow.FLOAT64:
		arr := array.NewFloat64Data(data)
		defer arr.Release()
		for i := 0; i < arr.Len(); i++ {
			if arr.IsValid(i) {
				if err := deepSet(&(*recs)[i], names, arr.Value(i)); err != nil {
					return err
				}
			} else {
				if err := deepSet(&(*recs)[i], names, nil); err != nil {
					return err
				}
			}
		}

	case arrow.STRING:
		arr := array.NewStringData(data)
		defer arr.Release()
		for i := 0; i < arr.Len(); i++ {
			if arr.IsValid(i) {
				if err := deepSet(&(*recs)[i], names, arr.Value(i)); err != nil {
					return err
				}
			} else {
				if err := deepSet(&(*recs)[i], names, nil); err != nil {
					return err
				}
			}
		}

	case arrow.BINARY:
		arr := array.NewBinaryData(data)
		defer arr.Release()
		for i := 0; i < arr.Len(); i++ {
			if arr.IsValid(i) {
				if err := deepSet(&(*recs)[i], names, arr.Value(i)); err != nil {
					return err
				}
			} else {
				if err := deepSet(&(*recs)[i], names, nil); err != nil {
					return err
				}
			}
		}

	case arrow.STRUCT:
		arr := array.NewStructData(data)
		defer arr.Release()
		st, stOk := arr.DataType().(*arrow.StructType)
		if !stOk {
			return fmt.Errorf("unsupported data type %v: %w", arr.DataType(), ErrUnsupportedType)
		}
		for i := 0; i < arr.Len(); i++ {
			if arr.IsValid(i) {
				for i := 0; i < arr.NumField(); i++ {
					n := st.Field(i).Name
					d := arr.Field(i).Data()
					if err := writeData(d, recs, append(names, n)); err != nil {
						return err
					}
				}
			} else {
				if err := deepSet(&(*recs)[i], names, nil); err != nil {
					return err
				}
			}
		}

	case arrow.LIST:
		arr := array.NewListData(data)
		defer arr.Release()
		for i := 0; i < arr.Len(); i++ {
			if arr.IsValid(i) {
				o := i + arr.Offset()
				bgn := int64(arr.Offsets()[o])
				end := int64(arr.Offsets()[o+1])
				slice := array.NewSlice(arr.ListValues(), bgn, end)
				if err := writeData(slice.Data(), recs, names); err != nil {
					return err
				}
			} else {
				if err := deepSet(&(*recs)[i], names, nil); err != nil {
					return err
				}
			}
		}

	default:
		return ErrUnsupportedType
	}

	return nil
}

func deepSet(recv *map[string]interface{}, keys []string, value interface{}) error {
	cur := *recv
	numKeys := len(keys)

	if numKeys > 1 {
		for _, k := range keys[:numKeys-1] {
			sub, subOk := (*recv)[k]
			if !subOk {
				return fmt.Errorf("no entry to %v", strings.Join(keys, "."))
			}

			typed, typedOk := sub.(map[string]interface{})
			if !typedOk {
				return fmt.Errorf("unexpected type of value %v", sub)
			}

			cur = typed
		}
	}

	if vv, ok := cur[keys[numKeys-1]]; ok {
		if arr, arrOk := vv.([]interface{}); arrOk {
			cur[keys[numKeys-1]] = append(arr, value)
		} else {
			cur[keys[numKeys-1]] = []interface{}{vv, value}
		}
	} else {
		cur[keys[numKeys-1]] = value
	}

	return nil
}
