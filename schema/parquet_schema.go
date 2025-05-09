package schema

import (
	"fmt"

	"github.com/xitongsys/parquet-go/parquet"
)

func typeNameToParquetType(name string, baseName string) (*parquet.Type, *parquet.ConvertedType) {
	if name == "BOOLEAN" {
		return parquet.TypePtr(parquet.Type_BOOLEAN), nil
	} else if name == "INT32" {
		return parquet.TypePtr(parquet.Type_INT32), nil
	} else if name == "INT64" {
		return parquet.TypePtr(parquet.Type_INT64), nil
	} else if name == "INT96" {
		return parquet.TypePtr(parquet.Type_INT96), nil
	} else if name == "FLOAT" {
		return parquet.TypePtr(parquet.Type_FLOAT), nil
	} else if name == "DOUBLE" {
		return parquet.TypePtr(parquet.Type_DOUBLE), nil
	} else if name == "BYTE_ARRAY" {
		return parquet.TypePtr(parquet.Type_BYTE_ARRAY), nil
	} else if name == "FIXED_LEN_BYTE_ARRAY" {
		return parquet.TypePtr(parquet.Type_FIXED_LEN_BYTE_ARRAY), nil
	} else if name == "UTF8" {
		return parquet.TypePtr(parquet.Type_BYTE_ARRAY), parquet.ConvertedTypePtr(parquet.ConvertedType_UTF8)
	} else if name == "INT_8" {
		return parquet.TypePtr(parquet.Type_INT32), parquet.ConvertedTypePtr(parquet.ConvertedType_INT_8)
	} else if name == "INT_16" {
		return parquet.TypePtr(parquet.Type_INT32), parquet.ConvertedTypePtr(parquet.ConvertedType_INT_16)
	} else if name == "INT_32" {
		return parquet.TypePtr(parquet.Type_INT32), parquet.ConvertedTypePtr(parquet.ConvertedType_INT_32)
	} else if name == "INT_64" {
		return parquet.TypePtr(parquet.Type_INT64), parquet.ConvertedTypePtr(parquet.ConvertedType_INT_64)
	} else if name == "UINT_8" {
		return parquet.TypePtr(parquet.Type_INT32), parquet.ConvertedTypePtr(parquet.ConvertedType_UINT_8)
	} else if name == "UINT_16" {
		return parquet.TypePtr(parquet.Type_INT32), parquet.ConvertedTypePtr(parquet.ConvertedType_UINT_16)
	} else if name == "UINT_32" {
		return parquet.TypePtr(parquet.Type_INT32), parquet.ConvertedTypePtr(parquet.ConvertedType_UINT_32)
	} else if name == "UINT_64" {
		return parquet.TypePtr(parquet.Type_INT64), parquet.ConvertedTypePtr(parquet.ConvertedType_UINT_64)
	} else if name == "DATE" {
		return parquet.TypePtr(parquet.Type_INT32), parquet.ConvertedTypePtr(parquet.ConvertedType_DATE)
	} else if name == "TIME_MILLIS" {
		return parquet.TypePtr(parquet.Type_INT32), parquet.ConvertedTypePtr(parquet.ConvertedType_TIME_MILLIS)
	} else if name == "TIME_MICROS" {
		return parquet.TypePtr(parquet.Type_INT64), parquet.ConvertedTypePtr(parquet.ConvertedType_TIME_MICROS)
	} else if name == "TIMESTAMP_MILLIS" {
		return parquet.TypePtr(parquet.Type_INT64), parquet.ConvertedTypePtr(parquet.ConvertedType_TIMESTAMP_MILLIS)
	} else if name == "TIMESTAMP_MICROS" {
		return parquet.TypePtr(parquet.Type_INT64), parquet.ConvertedTypePtr(parquet.ConvertedType_TIMESTAMP_MICROS)
	} else if name == "INTERVAL" {
		return parquet.TypePtr(parquet.Type_FIXED_LEN_BYTE_ARRAY), parquet.ConvertedTypePtr(parquet.ConvertedType_INTERVAL)
	} else if name == "DECIMAL" {
		if baseName == "INT32" {
			return parquet.TypePtr(parquet.Type_INT32), parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL)
		} else if baseName == "INT64" {
			return parquet.TypePtr(parquet.Type_INT64), parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL)
		} else if baseName == "FIXED_LEN_BYTE_ARRAY" {
			return parquet.TypePtr(parquet.Type_FIXED_LEN_BYTE_ARRAY), parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL)
		} else {
			return parquet.TypePtr(parquet.Type_BYTE_ARRAY), parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL)
		}
	}
	panic(fmt.Errorf("Unknown data type: '%s'", name))
}
