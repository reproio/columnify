package parquet

import (
	"errors"
	"fmt"

	"github.com/xitongsys/parquet-go/common"
	"github.com/xitongsys/parquet-go/layout"
	"github.com/xitongsys/parquet-go/schema"
)

var (
	ErrInvalidParquetSchema = errors.New("invalid parquet schema")
	ErrInvalidParquetRecord = errors.New("invalid parquet record")
	ErrUnsupportedMethod    = errors.New("unsupported method")
)

// prepareTables returns tables from fields(non record) in schema elements.
func prepareTables(schemaHandler *schema.SchemaHandler) (map[string]*layout.Table, error) {
	numSchemaElements := len(schemaHandler.SchemaElements)
	if len(schemaHandler.Infos) != numSchemaElements {
		return nil, fmt.Errorf("sizes of SchemaElement and Infos don't match: %w", ErrInvalidParquetSchema)
	}
	if len(schemaHandler.MapIndex) != numSchemaElements {
		return nil, fmt.Errorf("sizes of SchemaElement and MapIndex don't match: %w", ErrInvalidParquetSchema)
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

			tables[pathStr] = &layout.Table{
				Path:               path,
				MaxDefinitionLevel: maxDefinitionLevel,
				MaxRepetitionLevel: maxRepetitionLevel,
				RepetitionType:     e.GetRepetitionType(),
				Schema:             schemaHandler.SchemaElements[schemaHandler.MapIndex[pathStr]],
				Info:               schemaHandler.Infos[i],
			}
		}
	}

	return tables, nil
}
