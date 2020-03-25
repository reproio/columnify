package parquetgo

import (
	"fmt"
	"reflect"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/reproio/columnify/record"
	"github.com/xitongsys/parquet-go/common"
	"github.com/xitongsys/parquet-go/layout"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/schema"
	"github.com/xitongsys/parquet-go/types"
)

// MarshalMap converts 1 arrow record to parquet tables.
func MarshalArrow(maybeRecord []interface{}, bgn int, end int, schemaHandler *schema.SchemaHandler) (*map[string]*layout.Table, error) {
	// NOTE This marshaler expects record values aggregation has done before call
	if len(maybeRecord) != 1 {
		return nil, fmt.Errorf("size of records is invalid")
	}

	wrapped, recordOk := maybeRecord[0].(*record.WrappedRecord)
	if !recordOk {
		return nil, fmt.Errorf("unexpected input type: %v", reflect.TypeOf(maybeRecord[0]))
	}

	return marshalArrowRecord(wrapped.Record, schemaHandler)
}

func marshalArrowRecord(record array.Record, sh *schema.SchemaHandler) (*map[string]*layout.Table, error) {
	tables, err := prepareTables(sh)
	if err != nil {
		return nil, err
	}

	keys := make([]string, 0, len(record.Schema().Fields()))
	for _, f := range record.Schema().Fields() {
		keys = append(keys, common.HeadToUpper(f.Name))
	}

	for i, c := range record.Columns() {
		childPathMap := sh.PathMap.Children[keys[i]]
		data := c.Data()
		tables, err = marshalArrowData(data, tables, sh, childPathMap, 0, 0, false)
		if err != nil {
			return nil, err
		}
	}

	return &tables, nil
}

func marshalArrowData(data *array.Data, tables map[string]*layout.Table, sh *schema.SchemaHandler, pathMap *schema.PathMapType, parentRl int32, parentDl int32, isParentList bool) (map[string]*layout.Table, error) {
	pathStr := pathMap.Path

	var info *common.Tag
	if i, ok := sh.MapIndex[pathStr]; ok {
		info = sh.Infos[i]
	} else {
		return nil, fmt.Errorf("schema not found to path: %v", pathStr)
	}

	switch data.DataType().ID() {
	case arrow.BOOL:
		values := array.NewBooleanData(data)
		for i := 0; i < values.Len(); i++ {
			v, deltaDl, err := arrowPrimitiveToDataPageSource(values.Value(i), values.IsValid(i), info)
			if err != nil {
				return nil, err
			}
			tables[pathStr].Values = append(tables[pathStr].Values, v)
			tables[pathStr].DefinitionLevels = append(tables[pathStr].DefinitionLevels, parentDl+deltaDl)
			tables[pathStr].RepetitionLevels = append(tables[pathStr].RepetitionLevels, getRepetitionLevel(i, parentRl, isParentList))
		}

	case arrow.UINT32:
		values := array.NewUint32Data(data)
		for i := 0; i < values.Len(); i++ {
			v, deltaDl, err := arrowPrimitiveToDataPageSource(values.Value(i), values.IsValid(i), info)
			if err != nil {
				return nil, err
			}
			tables[pathStr].Values = append(tables[pathStr].Values, v)
			tables[pathStr].DefinitionLevels = append(tables[pathStr].DefinitionLevels, parentDl+deltaDl)
			tables[pathStr].RepetitionLevels = append(tables[pathStr].RepetitionLevels, getRepetitionLevel(i, parentRl, isParentList))
		}

	case arrow.UINT64:
		values := array.NewUint64Data(data)
		for i := 0; i < values.Len(); i++ {
			v, deltaDl, err := arrowPrimitiveToDataPageSource(values.Value(i), values.IsValid(i), info)
			if err != nil {
				return nil, err
			}
			tables[pathStr].Values = append(tables[pathStr].Values, v)
			tables[pathStr].DefinitionLevels = append(tables[pathStr].DefinitionLevels, parentDl+deltaDl)
			tables[pathStr].RepetitionLevels = append(tables[pathStr].RepetitionLevels, getRepetitionLevel(i, parentRl, isParentList))
		}

	case arrow.FLOAT32:
		values := array.NewFloat32Data(data)
		for i := 0; i < values.Len(); i++ {
			v, deltaDl, err := arrowPrimitiveToDataPageSource(values.Value(i), values.IsValid(i), info)
			if err != nil {
				return nil, err
			}
			tables[pathStr].Values = append(tables[pathStr].Values, v)
			tables[pathStr].DefinitionLevels = append(tables[pathStr].DefinitionLevels, parentDl+deltaDl)
			tables[pathStr].RepetitionLevels = append(tables[pathStr].RepetitionLevels, getRepetitionLevel(i, parentRl, isParentList))
		}

	case arrow.FLOAT64:
		values := array.NewFloat64Data(data)
		for i := 0; i < values.Len(); i++ {
			v, deltaDl, err := arrowPrimitiveToDataPageSource(values.Value(i), values.IsValid(i), info)
			if err != nil {
				return nil, err
			}
			tables[pathStr].Values = append(tables[pathStr].Values, v)
			tables[pathStr].DefinitionLevels = append(tables[pathStr].DefinitionLevels, parentDl+deltaDl)
			tables[pathStr].RepetitionLevels = append(tables[pathStr].RepetitionLevels, getRepetitionLevel(i, parentRl, isParentList))
		}

	case arrow.STRING:
		values := array.NewStringData(data)
		for i := 0; i < values.Len(); i++ {
			v, deltaDl, err := arrowPrimitiveToDataPageSource(values.Value(i), values.IsValid(i), info)
			if err != nil {
				return nil, err
			}
			tables[pathStr].Values = append(tables[pathStr].Values, v)
			tables[pathStr].DefinitionLevels = append(tables[pathStr].DefinitionLevels, parentDl+deltaDl)
			tables[pathStr].RepetitionLevels = append(tables[pathStr].RepetitionLevels, getRepetitionLevel(i, parentRl, isParentList))
		}

	case arrow.BINARY:
		values := array.NewBinaryData(data)
		for i := 0; i < values.Len(); i++ {
			v, deltaDl, err := arrowPrimitiveToDataPageSource(values.Value(i), values.IsValid(i), info)
			if err != nil {
				return nil, err
			}
			tables[pathStr].Values = append(tables[pathStr].Values, v)
			tables[pathStr].DefinitionLevels = append(tables[pathStr].DefinitionLevels, parentDl+deltaDl)
			tables[pathStr].RepetitionLevels = append(tables[pathStr].RepetitionLevels, getRepetitionLevel(i, parentRl, isParentList))
		}

	// TODO rl and dl
	case arrow.STRUCT:
		values := array.NewStructData(data)
		st, stOk := values.DataType().(*arrow.StructType)
		if !stOk {
			return nil, fmt.Errorf("unsupported data type: %v", values.DataType())
		}
		keys := make([]string, 0, len(st.Fields()))
		for _, f := range st.Fields() {
			keys = append(keys, common.HeadToUpper(f.Name))
		}
		for i := 0; i < values.NumField(); i++ {
			childPathMap := pathMap.Children[keys[i]]
			data := values.Field(i).Data()
			var err error
			tables, err = marshalArrowData(data, tables, sh, childPathMap, parentRl, parentDl, false)
			if err != nil {
				return nil, err
			}
		}

	case arrow.LIST:
		values := array.NewListData(data)
		var err error
		tables, err = marshalArrowData(values.ListValues().Data(), tables, sh, pathMap, parentRl, parentDl+1, true)
		if err != nil {
			return nil, err
		}

	default:
		return nil, fmt.Errorf("unsupported data type: %v", data.DataType())
	}

	return tables, nil
}

func arrowPrimitiveToDataPageSource(value interface{}, isValid bool, info *common.Tag) (interface{}, int32, error) {
	switch info.RepetitionType {
	case parquet.FieldRepetitionType_REQUIRED:
		if isValid {
			return formatValue(value, info), 0, nil
		} else {
			return nil, -1, fmt.Errorf("null value detected for required field: %v", info)
		}
	case parquet.FieldRepetitionType_OPTIONAL:
		if isValid {
			return formatValue(value, info), 1, nil
		} else {
			return nil, 0, nil
		}
	default:
		return nil, -1, fmt.Errorf("invalid field repetition type for: %v", info)
	}
}

func formatValue(value interface{}, info *common.Tag) interface{} {
	pT, cT := types.TypeNameToParquetType(info.Type, info.BaseType)
	return types.StrToParquetType(fmt.Sprintf("%v", value), pT, cT, int(info.Length), int(info.Scale))
}

func getRepetitionLevel(index int, parentRl int32, isParentList bool) int32 {
	if isParentList && index != 0 {
		return parentRl + 1
	} else {
		return parentRl
	}
}
