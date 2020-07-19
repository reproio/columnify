package json

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"io"
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
		values, err := convertToGo(col.Data())
		if err != nil {
			return err
		}
		for j, v := range values {
			recs[j][e.schema.Field(i).Name] = v
		}
	}

	return e.e.Encode(recs)
}

func deepSet(recv *map[string]interface{}, keys []string, value interface{}) error {
	cur := *recv
	numKeys := len(keys)

	if numKeys > 1 {
		for _, k := range keys[:numKeys-1] {
			sub, subOk := cur[k]
			if !subOk {
				cur[k] = map[string]interface{}{}
				sub = cur[k]
			}

			typed, typedOk := sub.(map[string]interface{})
			if !typedOk {
				// do nothing with considering to explicitly set nil ... is it really ok?
				return nil
			}
			cur = typed
		}
	}

	k := keys[numKeys-1]
	if vv, ok := cur[k]; ok {
		if arr, arrOk := vv.([]interface{}); arrOk {
			cur[k] = append(arr, value)
		} else {
			cur[k] = []interface{}{vv, value}
		}
	} else {
		cur[k] = value
	}

	return nil
}

// convertToGo converts Arrow values to Go typed values.
func convertToGo(data *array.Data) ([]interface{}, error) {
	recs := make([]interface{}, 0, data.Len())

	switch data.DataType().ID() {
	case arrow.BOOL:
		arr := array.NewBooleanData(data)
		defer arr.Release()
		for i := 0; i < arr.Len(); i++ {
			if arr.IsValid(i) {
				recs = append(recs, arr.Value(i))
			} else {
				recs = append(recs, nil)
			}
		}

	case arrow.INT8:
		arr := array.NewInt8Data(data)
		defer arr.Release()
		for i := 0; i < arr.Len(); i++ {
			if arr.IsValid(i) {
				recs = append(recs, arr.Value(i))
			} else {
				recs = append(recs, nil)
			}
		}

	case arrow.INT16:
		arr := array.NewInt16Data(data)
		defer arr.Release()
		for i := 0; i < arr.Len(); i++ {
			if arr.IsValid(i) {
				recs = append(recs, arr.Value(i))
			} else {
				recs = append(recs, nil)
			}
		}

	case arrow.INT32:
		arr := array.NewInt32Data(data)
		defer arr.Release()
		for i := 0; i < arr.Len(); i++ {
			if arr.IsValid(i) {
				recs = append(recs, arr.Value(i))
			} else {
				recs = append(recs, nil)
			}
		}

	case arrow.INT64:
		arr := array.NewInt64Data(data)
		defer arr.Release()
		for i := 0; i < arr.Len(); i++ {
			if arr.IsValid(i) {
				recs = append(recs, arr.Value(i))
			} else {
				recs = append(recs, nil)
			}
		}

	case arrow.UINT8:
		arr := array.NewUint8Data(data)
		defer arr.Release()
		for i := 0; i < arr.Len(); i++ {
			if arr.IsValid(i) {
				recs = append(recs, arr.Value(i))
			} else {
				recs = append(recs, nil)
			}
		}

	case arrow.UINT16:
		arr := array.NewUint16Data(data)
		defer arr.Release()
		for i := 0; i < arr.Len(); i++ {
			if arr.IsValid(i) {
				recs = append(recs, arr.Value(i))
			} else {
				recs = append(recs, nil)
			}
		}

	case arrow.UINT32:
		arr := array.NewUint32Data(data)
		defer arr.Release()
		for i := 0; i < arr.Len(); i++ {
			if arr.IsValid(i) {
				recs = append(recs, arr.Value(i))
			} else {
				recs = append(recs, nil)
			}
		}

	case arrow.UINT64:
		arr := array.NewUint64Data(data)
		defer arr.Release()
		for i := 0; i < arr.Len(); i++ {
			if arr.IsValid(i) {
				recs = append(recs, arr.Value(i))
			} else {
				recs = append(recs, nil)
			}
		}

	case arrow.FLOAT32:
		arr := array.NewFloat32Data(data)
		defer arr.Release()
		for i := 0; i < arr.Len(); i++ {
			if arr.IsValid(i) {
				recs = append(recs, arr.Value(i))
			} else {
				recs = append(recs, nil)
			}
		}

	case arrow.FLOAT64:
		arr := array.NewFloat64Data(data)
		defer arr.Release()
		for i := 0; i < arr.Len(); i++ {
			if arr.IsValid(i) {
				recs = append(recs, arr.Value(i))
			} else {
				recs = append(recs, nil)
			}
		}

	case arrow.STRING:
		arr := array.NewStringData(data)
		defer arr.Release()
		for i := 0; i < arr.Len(); i++ {
			if arr.IsValid(i) {
				recs = append(recs, arr.Value(i))
			} else {
				recs = append(recs, nil)
			}
		}

	case arrow.BINARY:
		arr := array.NewBinaryData(data)
		defer arr.Release()
		for i := 0; i < arr.Len(); i++ {
			if arr.IsValid(i) {
				recs = append(recs, arr.Value(i))
			} else {
				recs = append(recs, nil)
			}
		}

	case arrow.DATE32:
		arr := array.NewDate32Data(data)
		defer arr.Release()
		for i := 0; i < arr.Len(); i++ {
			if arr.IsValid(i) {
				recs = append(recs, arr.Value(i))
			} else {
				recs = append(recs, nil)
			}
		}

	case arrow.DATE64:
		arr := array.NewDate64Data(data)
		defer arr.Release()
		for i := 0; i < arr.Len(); i++ {
			if arr.IsValid(i) {
				recs = append(recs, arr.Value(i))
			} else {
				recs = append(recs, nil)
			}
		}

	case arrow.TIME32:
		arr := array.NewTime32Data(data)
		defer arr.Release()
		for i := 0; i < arr.Len(); i++ {
			if arr.IsValid(i) {
				recs = append(recs, arr.Value(i))
			} else {
				recs = append(recs, nil)
			}
		}

	case arrow.TIME64:
		arr := array.NewTime64Data(data)
		defer arr.Release()
		for i := 0; i < arr.Len(); i++ {
			if arr.IsValid(i) {
				recs = append(recs, arr.Value(i))
			} else {
				recs = append(recs, nil)
			}
		}

	case arrow.TIMESTAMP:
		arr := array.NewTimestampData(data)
		defer arr.Release()
		for i := 0; i < arr.Len(); i++ {
			if arr.IsValid(i) {
				recs = append(recs, arr.Value(i))
			} else {
				recs = append(recs, nil)
			}
		}

	case arrow.STRUCT:
		arr := array.NewStructData(data)
		defer arr.Release()
		st, stOk := arr.DataType().(*arrow.StructType)
		if !stOk {
			return nil, fmt.Errorf("unsupported data type %v: %w", arr.DataType(), ErrUnsupportedType)
		}
		for i := 0; i < arr.Len(); i++ {
			if arr.IsValid(i) {
				recs = append(recs, make(map[string]interface{}, arr.NumField()))
			} else {
				recs = append(recs, nil)
			}
		}
		for i := 0; i < arr.NumField(); i++ {
			values, err := convertToGo(arr.Field(i).Data())
			if err != nil {
				return nil, err
			}
			for j, v := range values {
				if arr.IsValid(j) {
					if r, ok := recs[j].(map[string]interface{}); ok {
						r[st.Field(i).Name] = v
					}
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
				defer slice.Release()
				values, err := convertToGo(slice.Data())
				if err != nil {
					return nil, err
				}
				recs = append(recs, values)
			} else {
				recs = append(recs, nil)
			}
		}
	}

	return recs, nil
}
