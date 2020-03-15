package schema

import (
	"fmt"

	"github.com/apache/arrow/go/arrow"
	"github.com/xitongsys/parquet-go/common"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/schema"
	"github.com/xitongsys/parquet-go/types"
)

var (
	arrowToParquet = map[arrow.DataType]string{
		arrow.FixedWidthTypes.Boolean: "BOOLEAN",
		arrow.PrimitiveTypes.Uint32:   "INT32",
		arrow.PrimitiveTypes.Uint64:   "INT64",
		arrow.PrimitiveTypes.Float32:  "FLOAT",
		arrow.PrimitiveTypes.Float64:  "DOUBLE",
		arrow.BinaryTypes.Binary:      "BYTE_ARRAY",
		arrow.BinaryTypes.String:      "UTF8",
	}
)

func NewSchemaHandlerFromArrow(s IntermediateSchema) (*schema.SchemaHandler, error) {
	elems := make([]*parquet.SchemaElement, 0)
	tags := make([]*common.Tag, 0)

	numChildren := int32(len(s.ArrowSchema.Fields()))
	rootElem := &parquet.SchemaElement{
		Name:           s.Name,
		NumChildren:    &numChildren,
		RepetitionType: parquet.FieldRepetitionTypePtr(parquet.FieldRepetitionType_REQUIRED),
	}
	elems = append(elems, rootElem)
	rootTag := common.Tag{
		ExName: rootElem.GetName(),
		InName: common.HeadToUpper(rootElem.GetName()),
		Type:   "", // empty string indicates record type
	}
	tags = append(tags, &rootTag)

	for _, f := range s.ArrowSchema.Fields() {
		if tn, ok := arrowToParquet[f.Type]; ok {
			t, ct := types.TypeNameToParquetType(tn, "")

			e := &parquet.SchemaElement{
				Type: t,
				Name: f.Name,
			}

			if ct != nil {
				e.ConvertedType = ct
			}

			if f.Nullable {
				e.RepetitionType = parquet.FieldRepetitionTypePtr(parquet.FieldRepetitionType_OPTIONAL)
			} else {
				e.RepetitionType = parquet.FieldRepetitionTypePtr(parquet.FieldRepetitionType_REQUIRED)
			}

			elems = append(elems, e)

			tag := common.Tag{
				ExName: e.GetName(),
				InName: common.HeadToUpper(e.GetName()),
				Type:   tn,
			}
			tags = append(tags, &tag)

		} else {
			return nil, fmt.Errorf("invalid schema conversion")
		}
	}

	sh := schema.NewSchemaHandlerFromSchemaList(elems)

	// NOTE parquet-go erases tag info used to write files by NewSchemaHandlerFromSchemaList()
	// So rewrite it here, like json implementation in parquet-go
	sh.Infos = tags

	return sh, nil
}
