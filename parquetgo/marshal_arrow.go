package parquetgo

import (
	"fmt"
	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/xitongsys/parquet-go/common"
	"github.com/xitongsys/parquet-go/layout"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/schema"
	"github.com/xitongsys/parquet-go/types"
	"reflect"
)

type wrappedRecord struct {
	record array.Record
}

func NewWrappedRecord(b *array.RecordBuilder) *wrappedRecord {
	return &wrappedRecord{
		record: b.NewRecord(),
	}
}

func MarshalArrow(maybeRecord []interface{}, bgn int, end int, schemaHandler *schema.SchemaHandler) (*map[string]*layout.Table, error) {
	// NOTE This marshaler expects record values aggregation has done before call
	if len(maybeRecord) != 1 {
		return nil, fmt.Errorf("size of records is invalid")
	}

	wrapped, recordOk := maybeRecord[0].(*wrappedRecord)
	if !recordOk {
		return nil, fmt.Errorf("unexpected input type: %v", reflect.TypeOf(maybeRecord[0]))
	}

	return marshalArrowRecord(wrapped.record, schemaHandler)
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
		tables, err = marshalArrowData(data, tables, sh, childPathMap, 0, 0)
		if err != nil {
			return nil, err
		}
	}

	return &tables, nil
}

func marshalArrowData(data *array.Data, tables map[string]*layout.Table, sh *schema.SchemaHandler, pathMap *schema.PathMapType, parentRl int32, parentDl int32) (map[string]*layout.Table, error) {
	pathStr := pathMap.Path

	var info *common.Tag
	if i, ok := sh.MapIndex[pathStr]; ok {
		info = sh.Infos[i]
		// NOTE its maybe done here on before schema resolution
		se := sh.SchemaElements[i]
		if se.ConvertedType != nil {
			info.Type = se.ConvertedType.String()
		} else {
			info.Type = se.Type.String()
		}
	} else {
		return nil, fmt.Errorf("schema not found to path: %v", pathStr)
	}

	// TODO consider repeated case
	rl := parentRl

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
			tables[pathStr].RepetitionLevels = append(tables[pathStr].RepetitionLevels, rl)
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
			tables[pathStr].RepetitionLevels = append(tables[pathStr].RepetitionLevels, rl)
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
			tables[pathStr].RepetitionLevels = append(tables[pathStr].RepetitionLevels, rl)
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
			tables[pathStr].RepetitionLevels = append(tables[pathStr].RepetitionLevels, rl)
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
			tables[pathStr].RepetitionLevels = append(tables[pathStr].RepetitionLevels, rl)
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
			tables[pathStr].RepetitionLevels = append(tables[pathStr].RepetitionLevels, rl)
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
			tables[pathStr].RepetitionLevels = append(tables[pathStr].RepetitionLevels, rl)
		}

	// TODO struct type

	// TODO list type

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
