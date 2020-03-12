package parquetgo

import (
	"fmt"
	"reflect"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/xitongsys/parquet-go/common"
	"github.com/xitongsys/parquet-go/layout"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/schema"
)

func MarshalArrow(maybeRecord []interface{}, bgn int, end int, schemaHandler *schema.SchemaHandler) (*map[string]*layout.Table, error) {
	if len(maybeRecord) != 1 {
		return nil, fmt.Errorf("size of records is invalid")
	}

	// FIXME it doesn't work
	record, recordOk := maybeRecord[0].(array.Record)
	if !recordOk {
		return nil, fmt.Errorf("unexpected input type: %v", reflect.TypeOf(maybeRecord[0]))
	}

	res := make(map[string]*layout.Table)

	// record
	pathStr := schemaHandler.GetRootInName() + "." + schemaHandler.Infos[1].InName
	res[pathStr] = layout.NewEmptyTable()
	res[pathStr].Path = common.StrToPath(pathStr)
	res[pathStr].MaxDefinitionLevel = 1
	res[pathStr].MaxRepetitionLevel = 0
	res[pathStr].RepetitionType = parquet.FieldRepetitionType_OPTIONAL
	res[pathStr].Type = schemaHandler.SchemaElements[schemaHandler.MapIndex[pathStr]].GetType()
	res[pathStr].Info = schemaHandler.Infos[1]

	// fields
	for i, c := range record.Columns() {
		pathStr := schemaHandler.GetRootInName() + "." + schemaHandler.Infos[i+2].InName
		res[pathStr] = layout.NewEmptyTable()
		res[pathStr].Path = common.StrToPath(pathStr)
		res[pathStr].MaxDefinitionLevel = 1
		res[pathStr].MaxRepetitionLevel = 0
		res[pathStr].RepetitionType = parquet.FieldRepetitionType_OPTIONAL
		res[pathStr].Type = schemaHandler.SchemaElements[schemaHandler.MapIndex[pathStr]].GetType()
		res[pathStr].Info = schemaHandler.Infos[i+2]

		data := c.Data()

		switch data.DataType().ID() {
		case arrow.BOOL:
			v := array.NewBooleanData(data)
			for i := 0; i < v.Len(); i++ {
				res[pathStr].Values = append(res[pathStr].Values, v.Value(i))
			}
		case arrow.UINT32:
			res[pathStr].Values = append(res[pathStr].Values, array.NewUint32Data(data).Uint32Values())
		case arrow.UINT64:
			res[pathStr].Values = append(res[pathStr].Values, array.NewUint64Data(data).Uint64Values())
		case arrow.FLOAT32:
			res[pathStr].Values = append(res[pathStr].Values, array.NewFloat32Data(data).Float32Values())
		case arrow.FLOAT64:
			res[pathStr].Values = append(res[pathStr].Values, array.NewFloat64Data(data).Float64Values())
		case arrow.STRING:
			v := array.NewStringData(data)
			for i := 0; i < v.Len(); i++ {
				res[pathStr].Values = append(res[pathStr].Values, v.Value(i))
			}
		case arrow.BINARY:
			v := array.NewBinaryData(data)
			for i := 0; i < v.Len(); i++ {
				res[pathStr].Values = append(res[pathStr].Values, v.Value(i))
			}
		default:
			return nil, fmt.Errorf("unsupported data type: %v", data.DataType())
		}

		res[pathStr].RepetitionLevels = append(res[pathStr].RepetitionLevels, 0)
		res[pathStr].DefinitionLevels = append(res[pathStr].DefinitionLevels, 1)
	}

	return &res, nil
}
