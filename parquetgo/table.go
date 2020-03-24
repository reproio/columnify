package parquetgo

import (
	"fmt"

	"github.com/xitongsys/parquet-go/common"
	"github.com/xitongsys/parquet-go/layout"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/schema"
)

// prepareTables returns tables from fields(non record) in schema elements.
func prepareTables(schemaHandler *schema.SchemaHandler) (map[string]*layout.Table, error) {
	numSchemaElements := len(schemaHandler.SchemaElements)
	if len(schemaHandler.Infos) != numSchemaElements {
		return nil, fmt.Errorf("sizes of SchemaElement and Infos don't match")
	}
	if len(schemaHandler.MapIndex) != numSchemaElements {
		return nil, fmt.Errorf("sizes of SchemaElement and MapIndex don't match")
	}

	tables := make(map[string]*layout.Table)
	for i, e := range schemaHandler.SchemaElements {
		if e.GetNumChildren() == 0 { // fields(non record)
			pathStr := schemaHandler.IndexMap[int32(i)]
			path := common.StrToPath(pathStr)

			maxDefinitionLevel, err := schemaHandler.MaxDefinitionLevel(path)
			if err != nil {
				return nil, err
			}

			maxRepetitionLevel, err := schemaHandler.MaxRepetitionLevel(path)
			if err != nil {
				return nil, err
			}

			var tpe parquet.Type
			if index, ok := schemaHandler.MapIndex[pathStr]; ok {
				if int(index) < len(schemaHandler.SchemaElements) {
					tpe = schemaHandler.SchemaElements[index].GetType()
				} else {
					return nil, fmt.Errorf("invalid index %v to schema elements %v ", index, schemaHandler.SchemaElements)
				}
			} else {
				return nil, fmt.Errorf("invalid schema key: %v", pathStr)
			}

			tables[pathStr] = &layout.Table{
				Path:               path,
				MaxDefinitionLevel: maxDefinitionLevel,
				MaxRepetitionLevel: maxRepetitionLevel,
				RepetitionType:     e.GetRepetitionType(),
				Type:               tpe,
				Info:               schemaHandler.Infos[i],
			}
		}
	}

	return tables, nil
}
